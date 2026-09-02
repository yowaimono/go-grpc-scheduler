<script setup>
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { api, discover, instances } from './api'
import { Bell, Connection, DataAnalysis, DocumentAdd, Monitor, MoreFilled, Refresh, Setting, SwitchButton, TrendCharts } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'

const activeView = ref('tasks')
const node = ref({ node_id: '—', role: 'probing', epoch: 0, ready: false })
const workers = ref([])
const tasks = ref([])
const events = ref([])
const overview = ref({ queue_depth: 0, workers_online: 0, events: 0 })
const dashboard = ref({ current: {}, history: [] })
const loading = ref(false)
const dialogVisible = ref(false)
const submitting = ref(false)
const historyVisible = ref(false)
const historyTask = ref(null)
const historyItems = ref([])
const taskFilter = ref('')
const taskPage = ref(1)
const pageSize = 10
const error = ref('')
const formRef = ref()
const form = reactive({ task_name: 'demo.echo', role: 'default', schedule_type: 'ONCE', run_at: '', delay_ms: 0, interval_ms: 60000, cron_expr: '*/5 * * * *', timezone: 'Asia/Shanghai', priority: 5, params: '{"message":"hello scheduler"}' })
let timer

const stats = computed(() => ({ pending: tasks.value.filter((item) => ['PENDING', 'RETRY_WAIT'].includes(item.status)).length, workers: workers.value.length, running: tasks.value.filter((item) => item.status === 'RUNNING').length, total: tasks.value.length }))
const filteredTasks = computed(() => { const value = taskFilter.value.trim().toLowerCase(); return value ? tasks.value.filter((item) => [item.task_id, item.task_name, item.required_role, item.status].some((field) => String(field || '').toLowerCase().includes(value))) : tasks.value })
const pagedTasks = computed(() => filteredTasks.value.slice((taskPage.value - 1) * pageSize, taskPage.value * pageSize))
const statusText = computed(() => node.value.ready ? 'Master 在线' : '正在探测')
const rules = { task_name: [{ required: true, message: '请输入任务名称', trigger: 'blur' }], role: [{ required: true, message: '请输入 Role', trigger: 'blur' }], params: [{ validator: (_rule, value, callback) => { try { JSON.parse(value); callback() } catch { callback(new Error('参数必须是合法 JSON')) } }, trigger: 'blur' }] }

async function refresh() {
  loading.value = true
  try {
    node.value = await discover()
    const [workerData, taskData, overviewData, eventData, dashboardData] = await Promise.all([api('/api/v1/workers'), api('/api/v1/tasks?limit=100'), api('/api/v1/observability/overview'), api('/api/v1/events'), api('/api/v1/observability/dashboard')])
    workers.value = workerData.items || []
    tasks.value = taskData.items || []
    overview.value = overviewData
    events.value = eventData.items || []
    dashboard.value = dashboardData || { current: {}, history: [] }
    error.value = ''
  } catch (err) {
    error.value = err.message
    node.value = { ...node.value, ready: false, role: 'offline' }
  } finally { loading.value = false }
}

function openCreate() { Object.assign(form, { task_name: 'demo.echo', role: 'default', schedule_type: 'ONCE', run_at: '', delay_ms: 0, interval_ms: 60000, cron_expr: '*/5 * * * *', timezone: 'Asia/Shanghai', priority: 5, params: '{"message":"hello scheduler"}' }); dialogVisible.value = true }
async function submit() {
  try {
    if (!await formRef.value?.validate()) return
  } catch { return }
  submitting.value = true
  try {
    const body = { ...form, params: JSON.parse(form.params), run_at_unix_ms: form.run_at ? new Date(form.run_at).getTime() : 0 }
    await api('/api/v1/tasks', { method: 'POST', body: JSON.stringify(body) })
    dialogVisible.value = false
    ElMessage.success('任务已提交到 Master')
    await refresh()
  } catch (err) { ElMessage.error(`提交失败：${err.message}`) }
  finally { submitting.value = false }
}

function formatTime(value) { return value ? new Date(value).toLocaleString('zh-CN', { hour12: false }) : '—' }
function statusType(status) { return ({ SUCCEEDED: 'success', RUNNING: 'primary', FAILED: 'danger', CANCELED: 'info', RETRY_WAIT: 'warning' })[status] || 'info' }
function roleText(roles) { return (roles || []).join(' / ') || '—' }
function openMetrics() { window.open(`${node.value.api_base || 'http://localhost:18080'}/metrics`, '_blank') }
async function actionTask(task, action) { try { await api(`/api/v1/tasks/${task.task_id}/${action}`, { method: 'POST' }); ElMessage.success('状态已更新'); await refresh() } catch (err) { ElMessage.error(`操作失败：${err.message}`) } }
function chartPoints(field, width = 620, height = 180) { const values = (dashboard.value.history || []).map((item) => Number(item[field] || 0)); if (!values.length) return ''; const max = Math.max(...values, 1); return values.map((value, index) => `${(index / Math.max(values.length - 1, 1)) * width},${height - (value / max) * (height - 14) - 7}`).join(' ') }
function utilization(snapshot) { return snapshot?.worker_slots ? Math.min(100, Math.round((snapshot.worker_in_flight || 0) / snapshot.worker_slots * 100)) : 0 }
async function openHistory(task) { try { historyTask.value = task; historyItems.value = (await api(`/api/v1/tasks/${task.task_id}/history`)).items || []; historyVisible.value = true } catch (err) { ElMessage.error(`历史加载失败：${err.message}`) } }

onMounted(() => { refresh(); timer = setInterval(refresh, 5000) })
onUnmounted(() => clearInterval(timer))
</script>

<template>
  <el-container class="app-shell">
    <el-aside width="236px" class="sidebar">
      <div class="side-brand"><div class="brand-glyph">S</div><div><strong>Scheduler</strong><span>CONTROL PLANE</span></div></div>
      <div class="side-section">调度管理</div>
      <el-menu :default-active="activeView" class="side-menu" @select="(key) => activeView = key">
        <el-menu-item index="tasks"><el-icon><DataAnalysis /></el-icon><span>任务调度</span><el-badge :value="stats.pending" :hidden="!stats.pending" class="menu-badge" /></el-menu-item>
        <el-menu-item index="workers"><el-icon><Monitor /></el-icon><span>Worker 实例</span></el-menu-item>
        <el-menu-item index="dashboard"><el-icon><TrendCharts /></el-icon><span>监控面板</span></el-menu-item>
        <el-menu-item index="observability"><el-icon><TrendCharts /></el-icon><span>可观测性</span></el-menu-item>
      </el-menu>
      <div class="side-section">系统</div>
      <el-menu class="side-menu" :default-active="activeView"><el-menu-item index="settings" @click="activeView = 'settings'"><el-icon><Setting /></el-icon><span>运行配置</span></el-menu-item></el-menu>
      <div class="side-bottom"><div class="node-line"><span class="node-dot" :class="{ offline: !node.ready }"></span><span>{{ statusText }}</span></div><small>{{ node.node_id }} · epoch {{ node.epoch }}</small></div>
    </el-aside>

    <el-container>
      <el-header class="top-header"><div><span class="crumb">SCHEDULER /</span><strong>{{ activeView === 'tasks' ? '任务调度' : activeView === 'workers' ? 'Worker 实例' : activeView === 'dashboard' ? '监控面板' : activeView === 'observability' ? '可观测性' : '运行配置' }}</strong></div><div class="header-actions"><el-tag :type="node.ready ? 'success' : 'warning'" effect="plain"><span class="tag-dot"></span>{{ statusText }}</el-tag><el-tooltip content="刷新数据"><el-button :icon="Refresh" circle text @click="refresh" /></el-tooltip><el-button :icon="Bell" circle text /></div></el-header>
      <el-main class="main-content">
        <template v-if="activeView === 'tasks'">
          <div class="page-title"><div><h1>任务调度</h1><p>配置任务规则、参数与执行周期，Master 会将任务分配给对应 Role 的 Worker。</p></div><el-button type="primary" :icon="DocumentAdd" @click="openCreate">创建任务</el-button></div>
          <div class="stat-row"><div class="stat-card"><span>待调度</span><strong>{{ stats.pending }}</strong><small>等待时间轮触发</small></div><div class="stat-card"><span>执行中</span><strong>{{ stats.running }}</strong><small>当前运行任务</small></div><div class="stat-card"><span>Worker</span><strong>{{ stats.workers }}</strong><small>已注册实例</small></div><div class="stat-card accent"><span>Master</span><strong>{{ node.ready ? 'ON' : '—' }}</strong><small>{{ node.node_id }}</small></div></div>
          <el-card shadow="never" class="table-card"><template #header><div class="card-header"><div><strong>任务列表</strong><span>最近同步 {{ new Date().toLocaleTimeString('zh-CN', { hour12: false }) }}</span></div><div class="table-tools"><el-input v-model="taskFilter" clearable placeholder="筛选 ID / 名称 / Role" size="small" @input="taskPage = 1" /><el-button text :icon="Refresh" @click="refresh">刷新</el-button></div></div></template><el-table v-loading="loading" :data="pagedTasks" stripe table-layout="fixed"><el-table-column prop="task_id" label="任务 ID" min-width="190"><template #default="scope"><span class="mono">{{ scope.row.task_id }}</span></template></el-table-column><el-table-column prop="task_name" label="任务名称" min-width="170" /><el-table-column prop="required_role" label="Role" width="140"><template #default="scope"><el-tag size="small" effect="plain">{{ scope.row.required_role }}</el-tag></template></el-table-column><el-table-column prop="status" label="状态" width="120"><template #default="scope"><el-tag :type="statusType(scope.row.status)" size="small">{{ scope.row.status }}</el-tag></template></el-table-column><el-table-column prop="priority" label="优先级" width="90" /><el-table-column prop="next_run_at" label="下次调度" min-width="190"><template #default="scope">{{ formatTime(scope.row.next_run_at) }}</template></el-table-column><el-table-column label="操作" width="150" fixed="right"><template #default="scope"><el-dropdown trigger="click"><el-button text :icon="MoreFilled" /><template #dropdown><el-dropdown-menu><el-dropdown-item v-if="scope.row.status === 'PENDING' || scope.row.status === 'RETRY_WAIT'" @click="actionTask(scope.row, 'pause')">暂停</el-dropdown-item><el-dropdown-item v-if="scope.row.status === 'PAUSED'" @click="actionTask(scope.row, 'resume')">恢复</el-dropdown-item><el-dropdown-item v-if="scope.row.status === 'FAILED'" @click="actionTask(scope.row, 'retry')">立即重试</el-dropdown-item><el-dropdown-item v-if="scope.row.status !== 'CANCELED' && scope.row.status !== 'SUCCEEDED'" divided @click="actionTask(scope.row, 'cancel')">取消任务</el-dropdown-item><el-dropdown-item @click="openHistory(scope.row)">查看历史</el-dropdown-item></el-dropdown-menu></template></el-dropdown></template></el-table-column><template #empty><el-empty description="暂无任务，先创建一个任务吧" /></template></el-table><div class="pagination-row"><span>共 {{ filteredTasks.length }} 条任务</span><el-pagination v-model:current-page="taskPage" :page-size="pageSize" :total="filteredTasks.length" layout="prev, pager, next" background /></div></el-card>
        </template>

        <template v-else-if="activeView === 'dashboard'"><div class="page-title"><div><h1>监控面板</h1><p>直接观察调度运行状态：吞吐、延迟、队列、Worker 容量和失败信号。</p></div><div class="dashboard-actions"><el-tag type="success" effect="plain"><span class="tag-dot"></span>实时快照</el-tag><el-button plain :icon="Connection" @click="openMetrics">导出指标</el-button></div></div><div class="monitor-grid"><el-card shadow="never" class="monitor-card monitor-card-wide"><div class="monitor-card-head"><div><span class="monitor-kicker">DISPATCH THROUGHPUT</span><strong>任务吞吐</strong></div><span class="monitor-value">{{ dashboard.current.assigned_total || 0 }}</span></div><div class="chart-wrap"><svg viewBox="0 0 620 180" preserveAspectRatio="none"><polyline :points="chartPoints('assigned_total')" fill="none" stroke="#8db83a" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" /><polyline :points="chartPoints('succeeded_total')" fill="none" stroke="#3a7161" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" opacity=".75" /></svg><div class="chart-legend"><span><i class="legend-line lime"></i>派发累计</span><span><i class="legend-line green"></i>成功累计</span></div></div></el-card><el-card shadow="never" class="monitor-card"><div class="monitor-card-head"><div><span class="monitor-kicker">SCHEDULE LAG</span><strong>调度延迟</strong></div><span class="monitor-value">{{ Math.round(dashboard.current.average_schedule_lag_ms || 0) }}<small>ms</small></span></div><div class="ring-wrap"><el-progress type="dashboard" :percentage="Math.min(100, Math.round((dashboard.current.average_schedule_lag_ms || 0) / 10))" :color="'#8db83a'" :width="118" :stroke-width="9"><template #default><span class="progress-label">{{ Math.round(dashboard.current.average_schedule_lag_ms || 0) }}<small>ms avg</small></span></template></el-progress></div></el-card><el-card shadow="never" class="monitor-card"><div class="monitor-card-head"><div><span class="monitor-kicker">WORKER CAPACITY</span><strong>Worker 利用率</strong></div><span class="monitor-value">{{ utilization(dashboard.current) }}<small>%</small></span></div><el-progress :percentage="utilization(dashboard.current)" :stroke-width="12" :show-text="false" color="#3a7161" /><div class="capacity-meta"><span>{{ dashboard.current.worker_in_flight || 0 }} 执行中</span><span>{{ dashboard.current.worker_slots || 0 }} 总槽位</span></div><div class="capacity-meta muted"><span>{{ dashboard.current.workers_online || 0 }} 在线实例</span><span>role pool</span></div></el-card></div><div class="monitor-lower"><el-card shadow="never" class="table-card status-card"><template #header><div class="card-header"><div><strong>任务状态分布</strong><span>当前内存调度快照</span></div></div></template><div class="status-list"><div><span><i class="status-key pending"></i>待调度</span><strong>{{ dashboard.current.pending || 0 }}</strong></div><div><span><i class="status-key running"></i>执行中</span><strong>{{ dashboard.current.running || 0 }}</strong></div><div><span><i class="status-key retry"></i>等待重试</span><strong>{{ dashboard.current.retry_wait || 0 }}</strong></div><div><span><i class="status-key failed"></i>失败</span><strong>{{ dashboard.current.failed || 0 }}</strong></div><div><span><i class="status-key paused"></i>已暂停</span><strong>{{ dashboard.current.paused || 0 }}</strong></div></div></el-card><el-card shadow="never" class="table-card event-card"><template #header><div class="card-header"><div><strong>实时事件</strong><span>最近 {{ events.length }} 条</span></div><el-tag type="info" effect="plain">EVENT STREAM</el-tag></div></template><el-scrollbar height="270px"><div class="event-list"><div v-for="item in events.slice().reverse().slice(0, 12)" :key="`${item.at}-${item.type}`" class="event-row"><span class="event-time">{{ formatTime(item.at) }}</span><el-tag size="small" :type="item.type.includes('failed') || item.type.includes('expired') ? 'danger' : 'success'">{{ item.type }}</el-tag><span class="event-task mono">{{ item.task_id || 'system' }}</span><span class="event-worker">{{ item.worker_id || 'scheduler' }}</span></div><el-empty v-if="!events.length" description="暂无调度事件" /></div></el-scrollbar></el-card></div></template>

        <template v-else-if="activeView === 'workers'">
          <div class="page-title"><div><h1>Worker 实例</h1><p>Worker 只负责执行任务；Role、调度规则和参数全部由任务配置决定。</p></div><el-tag type="success" effect="plain"><span class="tag-dot"></span>自动刷新 5s</el-tag></div>
          <el-card shadow="never" class="table-card"><template #header><div class="card-header"><div><strong>在线实例</strong><span>{{ workers.length }} 个 Worker 已注册</span></div></div></template><el-table v-loading="loading" :data="workers" stripe><el-table-column prop="worker_id" label="Worker ID" min-width="220"><template #default="scope"><span class="worker-name"><span class="online-dot"></span>{{ scope.row.worker_id }}</span></template></el-table-column><el-table-column label="支持 Role" min-width="260"><template #default="scope"><el-tag v-for="role in scope.row.roles" :key="role" size="small" effect="plain" class="role-tag">{{ role }}</el-tag><span v-if="!scope.row.roles?.length">—</span></template></el-table-column><el-table-column label="负载" width="180"><template #default="scope"><div class="load-cell"><el-progress :percentage="scope.row.slots ? Math.round((scope.row.in_flight || 0) / scope.row.slots * 100) : 0" :stroke-width="8" :show-text="false" /><span>{{ scope.row.in_flight || 0 }} / {{ scope.row.slots }}</span></div></template></el-table-column><el-table-column prop="last_heartbeat" label="最近心跳" min-width="190"><template #default="scope">{{ formatTime(scope.row.last_heartbeat) }}</template></el-table-column><template #empty><el-empty description="暂无 Worker 实例" /></template></el-table></el-card>
        </template>

        <template v-else-if="activeView === 'observability'"><div class="page-title"><div><h1>可观测性</h1><p>用指标、事件和健康信号观察调度系统，而不是等故障发生后再猜。</p></div><el-button plain :icon="Connection" @click="openMetrics">Prometheus 指标</el-button></div><div class="stat-row"><div class="stat-card"><span>READY QUEUE</span><strong>{{ overview.queue_depth }}</strong><small>当前待调度任务</small></div><div class="stat-card"><span>WORKERS ONLINE</span><strong>{{ overview.workers_online }}</strong><small>最近注册实例</small></div><div class="stat-card"><span>EVENT BUFFER</span><strong>{{ overview.events }}</strong><small>保留最近 200 条</small></div><div class="stat-card accent"><span>LEADER</span><strong>{{ node.role === 'master' ? 'ACTIVE' : 'STANDBY' }}</strong><small>epoch {{ node.epoch }}</small></div></div><el-card shadow="never" class="table-card"><template #header><div class="card-header"><div><strong>调度事件</strong><span>任务提交、派发、完成、租约过期</span></div><el-tag type="info" effect="plain">STRUCTURED EVENTS</el-tag></div></template><el-table :data="events" stripe><el-table-column prop="at" label="时间" width="210"><template #default="scope">{{ formatTime(scope.row.at) }}</template></el-table-column><el-table-column prop="type" label="事件" width="190"><template #default="scope"><el-tag size="small" :type="scope.row.type.includes('failed') || scope.row.type.includes('expired') ? 'danger' : 'success'">{{ scope.row.type }}</el-tag></template></el-table-column><el-table-column prop="task_id" label="任务 ID" min-width="220"><template #default="scope"><span class="mono">{{ scope.row.task_id || '—' }}</span></template></el-table-column><el-table-column prop="worker_id" label="Worker" min-width="180" /><el-table-column prop="role" label="Role" width="130" /><template #empty><el-empty description="暂无调度事件" /></template></el-table></el-card></template>
        <template v-else><div class="page-title"><div><h1>运行配置</h1><p>当前实例列表来自前端构建时的 VITE_SCHEDULER_INSTANCES 环境变量。</p></div></div><el-card shadow="never" class="settings-card"><el-descriptions :column="1" border><el-descriptions-item label="Scheduler 实例列表">{{ instances().join('、') }}</el-descriptions-item><el-descriptions-item label="当前 Master">{{ node.node_id }}</el-descriptions-item><el-descriptions-item label="Leader Epoch">{{ node.epoch }}</el-descriptions-item><el-descriptions-item label="数据库">PostgreSQL（由 DATABASE_URL 配置）</el-descriptions-item></el-descriptions></el-card></template>
        <p v-if="error" class="error-banner"><el-icon><SwitchButton /></el-icon>{{ error }}</p>
      </el-main>
    </el-container>
  </el-container>

  <el-dialog v-model="dialogVisible" title="创建任务" width="620px" destroy-on-close class="task-dialog"><el-form ref="formRef" :model="form" :rules="rules" label-position="top"><el-row :gutter="18"><el-col :span="14"><el-form-item label="任务名称" prop="task_name"><el-input v-model="form.task_name" placeholder="例如 image.resize" /></el-form-item></el-col><el-col :span="10"><el-form-item label="Role" prop="role"><el-input v-model="form.role" placeholder="例如 image" /></el-form-item></el-col></el-row><el-form-item label="调度模式"><el-radio-group v-model="form.schedule_type"><el-radio-button value="ONCE">立即 / 定时</el-radio-button><el-radio-button value="INTERVAL">固定周期</el-radio-button><el-radio-button value="CRON">Cron</el-radio-button></el-radio-group></el-form-item><el-row :gutter="18"><el-col :span="12"><el-form-item v-if="form.schedule_type === 'ONCE'" label="执行时间"><el-date-picker v-model="form.run_at" type="datetime" value-format="YYYY-MM-DDTHH:mm:ss" placeholder="留空则立即执行" style="width: 100%" /></el-form-item><el-form-item v-else-if="form.schedule_type === 'INTERVAL'" label="周期（毫秒）"><el-input-number v-model="form.interval_ms" :min="1000" :step="1000" style="width: 100%" /></el-form-item><el-form-item v-else label="Cron 表达式"><el-input v-model="form.cron_expr" placeholder="*/5 * * * *" /></el-form-item></el-col><el-col :span="12"><el-form-item v-if="form.schedule_type === 'CRON'" label="时区"><el-input v-model="form.timezone" placeholder="Asia/Shanghai" /></el-form-item><el-form-item v-else label="延时（毫秒）"><el-input-number v-model="form.delay_ms" :min="0" :step="1000" style="width: 100%" /></el-form-item></el-col></el-row><el-form-item label="优先级"><el-input-number v-model="form.priority" :min="0" :max="100" /></el-form-item><el-form-item label="任务参数（JSON）" prop="params"><el-input v-model="form.params" type="textarea" :rows="6" placeholder="{ &quot;message&quot;: &quot;hello&quot; }" /></el-form-item></el-form><template #footer><el-button @click="dialogVisible = false">取消</el-button><el-button type="primary" :loading="submitting" @click="submit">创建并调度</el-button></template></el-dialog>
  <el-drawer v-model="historyVisible" :title="historyTask ? `执行历史 · ${historyTask.task_id}` : '执行历史'" size="520px"><el-timeline><el-timeline-item v-for="item in historyItems" :key="`${item.at}-${item.type}`" :timestamp="formatTime(item.at)" placement="top"><strong>{{ item.type }}</strong><p class="drawer-meta">{{ item.worker_id || 'scheduler' }} <span v-if="item.role">· {{ item.role }}</span></p></el-timeline-item></el-timeline><el-empty v-if="!historyItems.length" description="暂无历史记录" /></el-drawer>
</template>
