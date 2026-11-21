<template>
  <section class="page" data-module="backlog_segment">
    <header class="page-head">
      <div>
        <h2>背压积压续送</h2>
        <p class="page-desc">
          把队列上限与各源表尚未确认的位点范围绑在一起：旧积压段先续送，新流段按剩余容量接纳；
          入库回执核验起点与末点后，容量才从已确认区间释放。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="run-round" type="button" :disabled="working" @click="runRound">
          执行续送轮次
        </button>
        <button class="btn" data-testid="bind-capacity" type="button" @click="openCapacity">绑定队列容量</button>
        <button class="btn" data-testid="open-create" type="button" @click="openCreate">登记积压段</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出积压清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">队列上限（行）</span>
        <strong class="stat-value" data-testid="stat-limit">{{ summary['队列上限'] ?? '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已占容量（未回执）</span>
        <strong class="stat-value" data-testid="stat-occupied">{{ summary['已占容量'] ?? '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">剩余容量</span>
        <strong class="stat-value" data-testid="stat-remaining">{{ summary['剩余容量'] ?? '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待续送 / 续送中 / 已入库</span>
        <strong class="stat-value" data-testid="stat-segments">
          {{ summary['待续送段'] ?? 0 }} / {{ summary['续送中段'] ?? 0 }} / {{ summary['已入库段'] ?? 0 }}
        </strong>
      </article>
    </div>

    <div class="resume-console" data-testid="resume-console">
      <div class="console-head">
        <strong>本轮调度预览</strong>
        <span class="console-next" data-testid="next-source">
          下一源表：{{ plan['下一源表'] || '—' }}
          <template v-if="plan['下一段']">（{{ plan['下一段']['段编号'] }} · {{ plan['下一段']['起始位点'] }} ~ {{ plan['下一段']['结束位点'] }}）</template>
        </span>
      </div>
      <div class="console-grid">
        <div>
          <span class="console-label">可调度段</span>
          <p v-if="!plan['预计调度']?.length" class="console-empty">本轮没有可用容量的段</p>
          <ul class="console-list">
            <li v-for="item in plan['预计调度']" :key="item['段编号']" :data-testid="`plan-${item['段编号']}`">
              {{ item['源表'] }} · {{ item['段编号'] }} · {{ item['起始位点'] }} ~ {{ item['结束位点'] }}
              <em>{{ item['数据行数'] }} 行 · {{ item['容量方式'] }}</em>
            </li>
          </ul>
        </div>
        <div>
          <span class="console-label">预计暂停（说明原因，保留位点）</span>
          <p v-if="!plan['预计暂停']?.length" class="console-empty">本轮没有需要顺延的段</p>
          <ul class="console-list">
            <li v-for="item in plan['预计暂停']" :key="`pause-${item['段编号']}`" :data-testid="`pause-${item['段编号']}`">
              {{ item['段编号'] }} · {{ item['原因类型'] }}
              <em>{{ item['原因'] }}</em>
            </li>
          </ul>
        </div>
      </div>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="onAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" :data-testid="`trace-${row.id}`" @click="openTrace(row)">逐段回查</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无积压段，可先登记积压段</td>
        </tr>
      </tbody>
    </table>

    <div class="detail-grid">
      <section class="detail-card">
        <header><strong>源表待补范围</strong><span class="muted">未获取入库回执的区间逐段列出</span></header>
        <table class="data-table compact">
          <thead>
            <tr><th>源表</th><th>连接</th><th>续送起点</th><th>已确认位点</th><th>待补段数</th><th>未确认行数</th></tr>
          </thead>
          <tbody>
            <tr v-for="cursor in summary['源表'] || []" :key="cursor['源表']" :data-testid="`cursor-${cursor['源表']}`">
              <td>{{ cursor['源表'] }}</td>
              <td>{{ cursor['连接状态'] }}</td>
              <td>{{ cursor['续送起点'] ?? '—' }}</td>
              <td>{{ cursor['已确认位点'] ?? '—' }}</td>
              <td>{{ cursor['待补段数'] }}</td>
              <td>{{ cursor['未确认行数'] }}</td>
            </tr>
            <tr v-if="!(summary['源表'] || []).length">
              <td colspan="6" class="empty-state">源表位点尚未建立</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="detail-card">
        <header><strong>暂停记录</strong><span class="muted">未进入某轮的段必须说明原因</span></header>
        <table class="data-table compact">
          <thead><tr><th>轮次</th><th>段编号</th><th>原因类型</th><th>数据位点</th><th>待补范围</th></tr></thead>
          <tbody>
            <tr v-for="pause in pauses" :key="pause.id" :data-testid="`pause-row-${pause.id}`">
              <td>{{ pause['轮次编号'] }}</td>
              <td>{{ pause['段编号'] }}</td>
              <td>{{ pause['原因类型'] }}</td>
              <td>{{ pause['数据位点'] }}</td>
              <td>{{ (pause['待补范围'] || []).join(' ~ ') }}</td>
            </tr>
            <tr v-if="!pauses.length">
              <td colspan="5" class="empty-state">还没有暂停记录</td>
            </tr>
          </tbody>
        </table>
      </section>
    </div>

    <section class="detail-card">
      <header><strong>变更捕获续送区间</strong><span class="muted">由变更捕获沿原位置重读</span></header>
      <table class="data-table compact">
        <thead><tr><th>任务编号</th><th>源表</th><th>起始位点</th><th>结束位点</th><th>读取方式</th><th>状态</th></tr></thead>
        <tbody>
          <tr v-for="interval in intervals" :key="interval.id" :data-testid="`interval-${interval.id}`">
            <td>{{ interval['任务编号'] }}</td>
            <td>{{ interval['监控表'] }}</td>
            <td>{{ interval['起始位点'] }}</td>
            <td>{{ interval['结束位点'] }}</td>
            <td>{{ interval['读取方式'] }}</td>
            <td>{{ interval['状态'] }}</td>
          </tr>
          <tr v-if="!intervals.length">
            <td colspan="6" class="empty-state">还没有下发续送区间</td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="receiptRow" class="modal-backdrop" data-testid="receipt-modal">
      <form class="modal" @submit.prevent="submitReceipt">
        <h3>入库回执核验</h3>
        <p class="muted">
          段 {{ receiptRow['段编号'] }}（{{ receiptRow['源表'] }}）的计划区间为
          {{ receiptRow['起始位点'] }} ~ {{ receiptRow['结束位点'] }}。起点与末点必须同时核验通过才会结算；
          重复回执不会二次释放容量。
        </p>
        <label class="create-field">
          <span>回执起点</span>
          <input v-model="receiptForm.start_pos" data-testid="receipt-start" required />
        </label>
        <label class="create-field">
          <span>回执末点</span>
          <input v-model="receiptForm.end_pos" data-testid="receipt-end" required />
        </label>
        <label class="create-field">
          <span>回执编号（可选）</span>
          <input v-model="receiptForm.receipt_id" data-testid="receipt-id" placeholder="例如 RCPT-ORDER-0001" />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="receiptRow = null">取消</button>
          <button class="btn primary" data-testid="submit-receipt" type="submit" :disabled="working">核验回执</button>
        </div>
      </form>
    </div>

    <div v-if="traceDetail" class="modal-backdrop" data-testid="trace-modal">
      <div class="modal">
        <h3>逐段回查 · {{ traceDetail['段']['段编号'] }}</h3>
        <dl class="trace-list">
          <dt>数据位点</dt>
          <dd>
            起始 {{ traceDetail['数据位点']['起始位点'] }} / 结束 {{ traceDetail['数据位点']['结束位点'] }} /
            续送起点 {{ traceDetail['数据位点']['续送起点'] }} / 已送达 {{ traceDetail['数据位点']['已送达位点'] ?? '—' }}
          </dd>
          <dt>待补范围</dt>
          <dd>{{ (traceDetail['待补范围'] || []).length ? traceDetail['待补范围'].join(' ~ ') : '已全部入库' }}</dd>
          <dt>源表位点</dt>
          <dd>
            {{ traceDetail['源表位点']['源表'] }} · {{ traceDetail['源表位点']['连接状态'] }} ·
            续送起点 {{ traceDetail['源表位点']['续送起点'] ?? '—' }}
          </dd>
          <dt>回执</dt>
          <dd v-if="!traceDetail['回执'].length">暂无回执</dd>
          <dd v-for="item in traceDetail['回执']" :key="item.id">
            {{ item['回执编号'] }} · {{ item['结果'] }} · {{ item['回执起点'] }} ~ {{ item['回执末点'] }} ·
            {{ item['说明'] }}
          </dd>
          <dt>暂停记录</dt>
          <dd v-if="!traceDetail['暂停记录'].length">暂无暂停</dd>
          <dd v-for="item in traceDetail['暂停记录']" :key="`p-${item.id}`">
            {{ item['轮次编号'] }} · {{ item['原因类型'] }} · {{ item['暂停原因'] }}
          </dd>
          <dt>调度轮次</dt>
          <dd v-if="!traceDetail['调度轮次'].length">尚未进入任何轮次</dd>
          <dd v-for="item in traceDetail['调度轮次']" :key="item['轮次编号']">
            {{ item['轮次编号'] }} · {{ item['状态'] }}
          </dd>
        </dl>
        <div class="page-actions">
          <button class="btn" type="button" @click="traceDetail = null">关闭</button>
        </div>
      </div>
    </div>

    <div v-if="showCapacity" class="modal-backdrop" data-testid="capacity-modal">
      <form class="modal" @submit.prevent="submitCapacity">
        <h3>绑定队列容量</h3>
        <p class="muted">可以绑定到背压策略的「队列上限」，也可以直接给定数值上限。容量只从入库已确认的范围释放。</p>
        <label class="create-field">
          <span>队列上限（行）</span>
          <input v-model="capacityForm.queue_limit" data-testid="capacity-limit" required />
        </label>
        <label class="create-field">
          <span>背压策略编号（可选，填写后以策略的队列上限为准）</span>
          <input v-model="capacityForm.policy_id" data-testid="capacity-policy" />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCapacity = false">取消</button>
          <button class="btn primary" data-testid="submit-capacity" type="submit" :disabled="working">保存</button>
        </div>
      </form>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记积压段</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input
            v-model="createValues[field]"
            :data-testid="`create-${field}`"
            :required="requiredCreateFields.includes(field)"
          />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条积压段记录</span>
      <span v-if="notice" class="notice-text" data-testid="notice">{{ notice }}</span>
      <span v-if="errorMessage" class="error-text" data-testid="error">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, any>
type Detail = Record<string, any>

const ENDPOINT = '/api/backlog_segment'
const columns = ['段编号', '源表', '源端编号', '起始位点', '结束位点', '数据行数', '续送起点', '来源类型', '入库回执']
const actions = ['调度续送', '回执核验', '断线回退']
const requiredCreateFields = ['段编号', '源表', '起始位点', '结束位点']

const rows = ref<Row[]>([])
const total = ref(0)
const summary = ref<Detail>({})
const plan = ref<Detail>({})
const pauses = ref<Row[]>([])
const intervals = ref<Row[]>([])
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const errorMessage = ref('')
const notice = ref('')
const working = ref(false)
const creating = ref(false)

const showCreate = ref(false)
const createValues = ref<Record<string, string>>({})
const showCapacity = ref(false)
const capacityForm = ref<Record<string, string>>({ queue_limit: '', policy_id: '' })
const receiptRow = ref<Row | null>(null)
const receiptForm = ref<Record<string, string>>({ start_pos: '', end_pos: '', receipt_id: '' })
const traceDetail = ref<Detail | null>(null)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function call(path: string, init?: RequestInit): Promise<Detail> {
  const response = await request(`${ENDPOINT}${path}`, init)
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) {
    throw new Error(payload?.detail || payload?.message || `接口返回 ${response.status}`)
  }
  return payload
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const [list, summaryPayload, planPayload, pausePayload, intervalPayload] = await Promise.all([
      call(`?${query}`),
      call('/summary'),
      call('/plan'),
      call('/pauses'),
      call('/cdc-intervals'),
    ])
    rows.value = list.items ?? []
    total.value = list.total ?? rows.value.length
    summary.value = summaryPayload
    plan.value = planPayload
    pauses.value = (pausePayload as Row[]) ?? []
    intervals.value = (intervalPayload as Row[]) ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '背压续送数据读取失败'
  }
}

async function runRound() {
  working.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await call('/rounds', { method: 'POST', body: JSON.stringify({}) })
    notice.value = result['说明'] ?? '续送轮次已执行'
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '续送轮次执行失败'
  } finally {
    working.value = false
  }
}

function openReceipt(row: Row) {
  receiptRow.value = row
  receiptForm.value = {
    start_pos: String(row['续送起点'] ?? row['起始位点'] ?? ''),
    end_pos: String(row['结束位点'] ?? ''),
    receipt_id: '',
  }
}

async function submitReceipt() {
  if (!receiptRow.value) return
  working.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await call(`/${receiptRow.value.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({
        action: '回执核验',
        start_pos: receiptForm.value.start_pos,
        end_pos: receiptForm.value.end_pos,
        receipt_id: receiptForm.value.receipt_id || null,
      }),
    })
    notice.value = String(result.message ?? '')
    if (!result.ok) errorMessage.value = String(result.message ?? '回执核验未通过')
    receiptRow.value = null
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执核验失败'
  } finally {
    working.value = false
  }
}

async function onAction(action: string, row: Row) {
  if (action === '回执核验') {
    openReceipt(row)
    return
  }
  working.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await call(`/${row.id}/actions`, {
      method: 'POST', body: JSON.stringify({ action }),
    })
    notice.value = String(result.message ?? '')
    if (!result.ok) errorMessage.value = String(result.message ?? '动作未生效')
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '背压续送操作失败'
  } finally {
    working.value = false
  }
}

async function openTrace(row: Row) {
  errorMessage.value = ''
  try {
    traceDetail.value = await call(`/${row.id}/trace`)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '逐段回查失败'
  }
}

function openCapacity() {
  capacityForm.value = {
    queue_limit: String(summary.value['队列上限'] ?? ''),
    policy_id: '',
  }
  showCapacity.value = true
}

async function submitCapacity() {
  working.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    const body: Record<string, unknown> = {}
    if (capacityForm.value.queue_limit) body.queue_limit = capacityForm.value.queue_limit
    if (capacityForm.value.policy_id) body.policy_id = Number(capacityForm.value.policy_id)
    const result = await call('/capacity', { method: 'PUT', body: JSON.stringify(body) })
    notice.value = String(result.message ?? '')
    if (!result.ok) errorMessage.value = String(result.message ?? '容量绑定失败')
    showCapacity.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '容量绑定失败'
  } finally {
    working.value = false
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  createValues.value['来源类型'] = '积压续送'
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await call('', { method: 'POST', body: JSON.stringify({ values: createValues.value }) })
    if (!result.ok) throw new Error(result.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'backlog_segment.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.resume-console {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px 14px;
  margin-bottom: 12px;
}
.console-head {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 12px;
  margin-bottom: 8px;
}
.console-next { font-size: 13px; color: var(--brand); }
.console-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.console-label { font-size: 12px; color: var(--muted); }
.console-list { margin: 6px 0 0; padding-left: 18px; font-size: 13px; }
.console-list li { margin-bottom: 6px; }
.console-list em { display: block; color: var(--muted); font-style: normal; font-size: 12px; }
.console-empty { font-size: 13px; color: var(--muted); margin: 6px 0 0; }
.detail-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 12px; }
.detail-card { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; margin-top: 12px; }
.detail-card header { display: flex; justify-content: space-between; align-items: baseline; gap: 12px; }
.muted { color: var(--muted); font-size: 12px; }
.data-table.compact th, .data-table.compact td { padding: 6px 8px; font-size: 12px; }
.trace-list { display: grid; grid-template-columns: 96px 1fr; gap: 6px 12px; font-size: 13px; }
.trace-list dt { color: var(--muted); font-size: 12px; }
.trace-list dd { margin: 0; }
.notice-text { color: #15803d; }
</style>
