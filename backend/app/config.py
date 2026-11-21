"""运行配置：端口、跨域、运行环境。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    app_name: str = "管道工坊 · 实时数仓与流式管道平台"
    env: str = "local"
    port: int = 8000
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    # 背压积压续送：默认队列上限（行）。页面上可绑定到具体背压策略的「队列上限」。
    backlog_queue_limit: int = 200


settings = Settings()
