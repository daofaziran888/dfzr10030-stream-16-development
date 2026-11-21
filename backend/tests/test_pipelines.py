"""跨模块管道测试：覆盖 7 条数据流的真实行为断言。"""
import pytest
from app.services.pipeline_orchestrator import (
    run_source_cdc_pipeline, run_window_metric_pipeline,
    run_rule_router_pipeline, run_metric_sink_pipeline,
    run_quality_scan, run_partition_backpressure_pipeline,
    run_checkpoint_state_pipeline, run_all_pipelines,
    run_backlog_resume_pipeline, list_pipeline_results,
)
from app.store import store


class TestSourceCdcPipeline:
    def test_source_count(self):
        r = run_source_cdc_pipeline()
        assert r["source_count"] == 2
        assert r["total_events"] == 8

    def test_mysql_events(self):
        r = run_source_cdc_pipeline()
        mysql = r["sources"]["SOUR-MYSQL-01"]
        assert mysql["INSERT"] == 3
        assert mysql["UPDATE"] == 1
        assert mysql["DELETE"] == 1

    def test_pipeline_result_written(self):
        run_source_cdc_pipeline()
        results = list_pipeline_results()
        assert any(pr["pipeline_id"] == "source_cdc_transform" for pr in results)


class TestWindowMetricPipeline:
    def test_window_aggregation(self):
        r = run_window_metric_pipeline()
        assert r["total_records"] == 10
        assert "WIN-TUMBLE-60s" in r
        assert r["WIN-TUMBLE-60s"]["记录数"] == 10

    def test_gmv_calculation(self):
        r = run_window_metric_pipeline()
        gmv = r["WIN-TUMBLE-60s"]["gmv"]
        assert gmv > 0
        assert 1800 < gmv < 1900  # 128.5+56+320+89.9+250+175+42.5+500+198+67.8

    def test_channel_distribution(self):
        r = run_window_metric_pipeline()
        dist = r["WIN-TUMBLE-60s"]["渠道分布"]
        assert dist["app"] == 4
        assert dist["小程序"] == 3
        assert dist["h5"] == 3

    def test_metrics_computed(self):
        r = run_window_metric_pipeline()
        assert "metrics" in r
        assert "METR-ORDER-COUNT" in r["metrics"]
        assert r["metrics"]["METR-ORDER-COUNT"] == 10

    def test_pipeline_result_written(self):
        run_window_metric_pipeline()
        results = list_pipeline_results()
        assert any(pr["pipeline_id"] == "window_metric_calc" for pr in results)


class TestRuleRouterPipeline:
    def test_rules_fired(self):
        r = run_rule_router_pipeline()
        assert r["rules_fired"] == 4
        assert r["total_hits"] >= 0

    def test_routing_generated(self):
        r = run_rule_router_pipeline()
        assert len(r["routing"]) > 0

    def test_pipeline_result_written(self):
        run_rule_router_pipeline()
        results = list_pipeline_results()
        assert any(pr["pipeline_id"] == "rule_router_orch" for pr in results)


class TestMetricSinkPipeline:
    def test_sinks_written(self):
        r = run_metric_sink_pipeline()
        assert r["sink_count"] == 4
        assert all(s["写入状态"] == "成功" for s in r["sinks"])

    def test_pipeline_result_written(self):
        run_metric_sink_pipeline()
        results = list_pipeline_results()
        assert any(pr["pipeline_id"] == "metric_pipeline_sink" for pr in results)


class TestQualityScan:
    def test_score_calculation(self):
        r = run_quality_scan()
        assert 0 <= r["quality_score"] <= 100

    def test_anomalies_initial(self):
        r = run_quality_scan()
        assert isinstance(r["anomalies"], list)
        assert isinstance(r["module_issues"], list)

    def test_pipeline_result_written(self):
        run_quality_scan()
        results = list_pipeline_results()
        assert any(pr["pipeline_id"] == "quality_scan" for pr in results)


class TestPartitionBackpressure:
    def test_skew_detection(self):
        r = run_partition_backpressure_pipeline()
        assert r["partition_count"] == 2
        assert r["alert_count"] >= 0

    def test_pipeline_result_written(self):
        run_partition_backpressure_pipeline()
        results = list_pipeline_results()
        assert any(pr["pipeline_id"] == "partition_backpressure" for pr in results)


class TestCheckpointState:
    def test_health_check(self):
        r = run_checkpoint_state_pipeline()
        assert "health" in r
        assert r["checkpoint_total"] == 3

    def test_pipeline_result_written(self):
        run_checkpoint_state_pipeline()
        results = list_pipeline_results()
        assert any(pr["pipeline_id"] == "checkpoint_state" for pr in results)


class TestBacklogResume:
    def test_plan_points_at_orders(self):
        r = run_backlog_resume_pipeline()
        assert r["queue_limit"] == 200
        assert r["next_source"] == "orders"
        assert r["dispatch"] == ["SEG-0001"]
        assert "SEG-0003" in r["pause"]
        assert r["resume_points"]["orders"] == 1000

    def test_pipeline_result_written(self):
        run_backlog_resume_pipeline()
        results = list_pipeline_results()
        assert any(pr["pipeline_id"] == "backlog_resume" for pr in results)


class TestRunAllPipelines:
    def test_all_pipelines_run(self):
        r = run_all_pipelines()
        keys = ["source_cdc", "window_metric", "rule_router", "metric_sink",
                "quality_scan", "partition_bp", "checkpoint_state", "backlog_resume"]
        for k in keys:
            assert k in r, f"Missing {k}"
        results = list_pipeline_results()
        assert len(results) == 8