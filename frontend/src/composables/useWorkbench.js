import { computed, reactive, ref, shallowRef } from 'vue'
import { api } from '../api'

/* One shared workbench store. The run/conversation lifecycle carries a few
   ordering rules that must hold no matter which component triggers them:

   - a response for a run the user already left must never restore it,
   - a deleted run must not be re-selected by an in-flight refresh,
   - starting a new conversation clears live events from the previous one.

   `selectionVersion` and `deletingRuns` enforce those, exactly as the
   single-file workbench did before the UI was split into components. */

const STATE_META = {
  QUEUED: { label: '排队中', tone: 'muted', hint: '等待调度' },
  PLANNING: { label: '规划中', tone: 'info', hint: 'Planner 正在拆解需求' },
  BINDING: { label: '算子绑定', tone: 'info', hint: 'Specialist 正在并行检索算子' },
  INTEGRATING: { label: '拼装中', tone: 'info', hint: 'Integrator 正在对齐字段' },
  VALIDATING: { label: '验证中', tone: 'info', hint: '正在执行契约校验' },
  READY: { label: '已生成', tone: 'brand', hint: '静态检查通过，尚未执行' },
  RUNNING: { label: '执行中', tone: 'warn', hint: 'DataFlow 正在运行 pipeline' },
  EXECUTED: { label: '已执行', tone: 'ok', hint: '运行成功并产出结果' },
  VERIFIED: { label: '已验证', tone: 'ok', hint: '独立 Verifier 认可结果' },
  APPROVAL_REQUIRED: { label: '待授权', tone: 'warn', hint: '等待手动批准执行' },
  RESOURCE_REQUIRED: { label: '待配置服务', tone: 'warn', hint: '缺少 API resource' },
  REFUSED: { label: '已拒绝', tone: 'muted', hint: 'Planner 判断不支持该需求' },
  BLOCKED: { label: '已阻断', tone: 'danger', hint: '生成或执行失败' },
  IDLE: { label: '空闲', tone: 'muted', hint: '' },
}

const EVENT_META = {
  'workflow.state': { label: '流程状态', tone: 'brand' },
  'workflow.repair': { label: '修复尝试', tone: 'warn' },
  'workflow.validation_failed': { label: '校验失败', tone: 'danger' },
  'agent.started': { label: 'Agent 开始', tone: 'info' },
  'agent.completed': { label: 'Agent 完成', tone: 'ok' },
  'agent.failed': { label: 'Agent 失败', tone: 'danger' },
  'tool.called': { label: '工具调用', tone: 'muted' },
  'skill.invoked': { label: 'Skill 调用', tone: 'brand' },
  'approval.granted': { label: '审批通过', tone: 'ok' },
}

const AGENT_META = {
  planner: { name: 'Planner', duty: '拆解需求与目标字段' },
  operator_specialist: { name: 'Operator Specialist', duty: '检索算子源码并绑定参数' },
  pipeline_integrator: { name: 'Pipeline Integrator', duty: '对齐 schema 与参数' },
  verifier: { name: 'Evidence Verifier', duty: '检查真实运行证据' },
  leader: { name: 'Orchestrator', duty: '调度与修复' },
}

const STAGE_LABELS = {
  PLANNING: '正在规划任务',
  BINDING: '正在并行匹配算子',
  INTEGRATING: '正在对齐字段并生成 Pipeline',
  VALIDATING: '正在执行契约校验',
  READY: 'Pipeline 已生成并通过静态检查',
  RUNNING: 'Pipeline 正在执行',
  EXECUTED: 'Pipeline 执行完成',
  BLOCKED: '任务已阻断，请查看失败证据',
}

const AGENT_DONE_LABELS = {
  planner: 'Planner 已完成任务拆解',
  operator_specialist: '算子专家已完成算子绑定',
  pipeline_integrator: 'Pipeline Integrator 已完成字段对齐与拼装',
  verifier: 'Verifier 已完成验证与证据检查',
}

export const stateMeta = (state) => STATE_META[state] || { label: state || '未知', tone: 'muted', hint: '' }
export const eventMeta = (event) => EVENT_META[event] || { label: event, tone: 'muted' }
export const agentMeta = (agent) => AGENT_META[agent] || { name: agent, duty: '' }

/* The built-in sample. A request is only ever run against the data the
   composer holds, so the UI has to say when that data is still this. */
export const DEFAULT_INPUT = '[{"raw_content":"  Hello   world  "},{"raw_content":"Hello world"}]'

export const RUNNING_STATES = ['QUEUED', 'PLANNING', 'BINDING', 'INTEGRATING', 'VALIDATING', 'RUNNING']
export const STATE_ORDER = ['PLANNING', 'BINDING', 'INTEGRATING', 'READY']
export const EXECUTABLE_STATES = ['READY', 'VERIFIED', 'EXECUTED', 'APPROVAL_REQUIRED', 'RESOURCE_REQUIRED', 'BLOCKED']

function createStore() {
  const backendMode = ref('unknown')
  const toasts = ref([])
  const runs = ref([])
  const runQuery = ref('')
  const runFilter = ref('all')
  const selectedId = ref('')
  const selected = shallowRef(null)
  const events = ref([])
  const agentOutputs = ref([])
  const stages = ref([])
  const selectedStage = shallowRef(null)
  const collaboration = shallowRef(null)
  const runSkills = ref([])
  const runEvidence = ref([])
  const deletingRuns = reactive(new Set())

  const datasets = ref([])
  const selectedDatasetId = ref('')
  const inputText = ref(DEFAULT_INPUT)
  const allowCustom = ref(true)

  const resources = ref([])
  const models = ref([])

  const conversation = shallowRef(null)
  const conversationMessages = ref([])
  const liveMessages = ref([])

  const loadingRuns = ref(false)
  const loadingRun = ref(false)
  const sending = ref(false)
  const executing = ref(false)
  const startingConversation = ref(false)

  let stream = null
  let poller = null
  let refreshHandle = null
  let selectionVersion = 0
  const announced = new Set()

  function notify(message, tone = 'danger') {
    const id = `toast-${Date.now()}-${Math.random().toString(16).slice(2)}`
    toasts.value = [...toasts.value.slice(-3), { id, message: String(message), tone }]
    setTimeout(() => dismiss(id), tone === 'danger' ? 9000 : 4200)
  }
  const dismiss = (id) => (toasts.value = toasts.value.filter((item) => item.id !== id))
  const guard = async (work, fallback) => {
    try {
      return await work()
    } catch (error) {
      notify(error.message)
      return fallback
    }
  }

  const visibleRuns = computed(() => {
    const query = runQuery.value.trim().toLowerCase()
    return runs.value.filter((run) => {
      const matchesQuery = !query ||
        (run.request || '').toLowerCase().includes(query) ||
        run.run_id.toLowerCase().includes(query)
      const running = RUNNING_STATES.includes(run.state)
      const matchesFilter =
        runFilter.value === 'all' ||
        (runFilter.value === 'active' && running) ||
        (runFilter.value === 'done' && ['READY', 'EXECUTED', 'VERIFIED'].includes(run.state)) ||
        (runFilter.value === 'attention' && ['BLOCKED', 'REFUSED', 'RESOURCE_REQUIRED', 'APPROVAL_REQUIRED'].includes(run.state))
      return matchesQuery && matchesFilter
    })
  })

  /* What the next message will actually be run against. */
  const inputSummary = computed(() => {
    if (selectedDatasetId.value) {
      const item = datasets.value.find((entry) => entry.id === selectedDatasetId.value)
      return { kind: 'dataset', tone: 'brand', rows: item?.rows ?? null,
               text: `数据集 ${item?.name || selectedDatasetId.value}${item?.rows ? ` · ${item.rows} 行` : ''}` }
    }
    const text = inputText.value.trim()
    if (!text) return { kind: 'empty', tone: 'warn', rows: 0, text: '未提供输入，将使用仓库示例数据' }
    try {
      const rows = JSON.parse(text)
      if (!Array.isArray(rows)) throw new Error('not an array')
      const fields = rows.length ? Object.keys(rows[0]) : []
      const shape = `${rows.length} 行 · ${fields.join(', ') || '无字段'}`
      return inputText.value === DEFAULT_INPUT
        ? { kind: 'sample', tone: 'warn', rows: rows.length, text: `示例数据 · ${shape}` }
        : { kind: 'inline', tone: 'muted', rows: rows.length, text: shape }
    } catch {
      return { kind: 'invalid', tone: 'danger', rows: 0, text: 'JSON 无法解析' }
    }
  })

  const currentState = computed(() => selected.value?.state || 'IDLE')
  const steps = computed(() => selected.value?.pipeline?.steps || [])
  const isRunning = computed(() => RUNNING_STATES.includes(currentState.value))
  const canExecute = computed(() => !!selected.value?.pipeline && EXECUTABLE_STATES.includes(currentState.value))
  const timeline = computed(() => [...events.value].sort((a, b) => (b.seq || 0) - (a.seq || 0)))
  /* Stored messages and live event announcements interleave in time: the
     backend appends a failure explanation while SSE progress lines are still
     arriving, so the merged list is ordered by timestamp, not by source. */
  const chatMessages = computed(() =>
    [...conversationMessages.value, ...liveMessages.value]
      .map((item, index) => ({ item, index }))
      .sort((a, b) => (a.item.created_at || 0) - (b.item.created_at || 0) || a.index - b.index)
      .map((entry) => entry.item))

  function resetRunState() {
    selected.value = null
    events.value = []
    agentOutputs.value = []
    stages.value = []
    selectedStage.value = null
    collaboration.value = null
    runSkills.value = []
    runEvidence.value = []
    liveMessages.value = []
    announced.clear()
  }

  function closeStream() {
    stream?.close()
    stream = null
  }

  async function refreshRuns() {
    loadingRuns.value = true
    try {
      runs.value = await api.runs()
      if (selectedId.value) await selectRun(selectedId.value, false)
    } finally {
      loadingRuns.value = false
    }
  }

  async function selectRun(id, connect = true) {
    if (deletingRuns.has(id)) return
    const version = ++selectionVersion
    const changed = selectedId.value !== id
    selectedId.value = id
    if (changed) {
      closeStream()
      selectedStage.value = null
      liveMessages.value = []
      announced.clear()
      loadingRun.value = true
    }
    try {
      const [run, runEvents, outputs, stageData, collab, skills, evidence] = await Promise.all([
        api.run(id), api.events(id), api.agentOutputs(id), api.stages(id),
        api.collaboration(id), api.skills(id), api.evidence(id),
      ])
      // A response for a run the user already left must never restore it.
      if (version !== selectionVersion || id !== selectedId.value) return
      const keptStage = selectedStage.value?.stage_id
      selected.value = run
      events.value = runEvents
      agentOutputs.value = outputs
      stages.value = stageData.stages || []
      selectedStage.value = stages.value.find((stage) => stage.stage_id === keptStage) || stages.value.at(-1) || null
      collaboration.value = collab
      runSkills.value = skills.skills || []
      runEvidence.value = evidence.artifacts || []
      if (connect) connectStream(id)
    } finally {
      if (version === selectionVersion) loadingRun.value = false
    }
  }

  /* SSE bursts arrive in clusters; one refresh per cluster is enough. */
  function scheduleRefresh(id) {
    if (refreshHandle) return
    refreshHandle = setTimeout(() => {
      refreshHandle = null
      if (id === selectedId.value) selectRun(id, false).catch((error) => notify(error.message))
    }, 350)
  }

  function announce(item) {
    const key = `${item.trace_id}:${item.seq}`
    if (!item?.seq || announced.has(key)) return
    // The failure analyst speaks for itself; a generic "已完成" would sit
    // between its two messages.
    if (item.agent === 'failure_analyst') return
    announced.add(key)
    let content = ''
    if (item.event === 'agent.completed') {
      content = `${AGENT_DONE_LABELS[item.agent] || `${item.agent} 已完成`}。${item.job ? `作业 ${item.job} 已产出结构化结果。` : ''}`
    } else if (item.event === 'agent.failed') {
      content = `${agentMeta(item.agent).name} 本次尝试失败，系统将依据事件记录进行重试或阻断。`
    } else if (item.event === 'workflow.state') {
      content = STAGE_LABELS[item.state] || `工作流状态更新为 ${item.state}`
    }
    if (!content) return
    liveMessages.value = [...liveMessages.value, {
      message_id: `live-${item.trace_id}-${item.seq}`,
      role: 'controller',
      content,
      intent: 'progress_update',
      live: true,
      created_at: item.timestamp || Date.now() / 1000,
      revision: conversation.value?.active_revision || 0,
    }]
  }

  function connectStream(runId) {
    closeStream()
    const source = new EventSource(`/api/v1/runs/${runId}/stream`)
    stream = source
    source.onmessage = (message) => {
      if (source !== stream || runId !== selectedId.value) return
      try {
        const item = JSON.parse(message.data)
        announce(item)
        if (!events.value.some((event) => event.seq === item.seq)) events.value = [...events.value, item]
        if (['workflow.state', 'agent.completed', 'agent.failed'].includes(item.event)) scheduleRefresh(runId)
      } catch (error) {
        notify(error.message)
      }
    }
    source.addEventListener('done', () => {
      source.close()
      if (stream === source) stream = null
      scheduleRefresh(runId)
      guard(refreshConversation)
      setTimeout(() => guard(refreshConversation), 4000)
    })
    source.onerror = () => {
      source.close()
      if (stream === source) stream = null
    }
  }

  async function deleteRun(id) {
    if (deletingRuns.has(id)) return
    deletingRuns.add(id)
    const wasSelected = selectedId.value === id
    if (wasSelected) {
      ++selectionVersion
      closeStream()
    }
    try {
      await api.deleteRun(id)
      if (selectedId.value === id) {
        ++selectionVersion
        selectedId.value = ''
        resetRunState()
      }
      runs.value = runs.value.filter((run) => run.run_id !== id)
      await refreshRuns()
    } catch (error) {
      notify(error.message)
      if (wasSelected && selectedId.value === id) connectStream(id)
    } finally {
      deletingRuns.delete(id)
    }
  }

  function parseInputRows() {
    if (selectedDatasetId.value) return undefined
    const text = inputText.value.trim()
    if (!text) return undefined
    try {
      const rows = JSON.parse(text)
      if (!Array.isArray(rows)) throw new Error('输入数据需要是 JSON 数组')
      return rows
    } catch (error) {
      throw new Error(`输入数据解析失败：${error.message}`)
    }
  }

  /* Failure analysis is appended to the conversation by the backend, so the
     client has to re-read it rather than only updating on send. */
  async function refreshConversation() {
    const current = conversation.value?.conversation_id
    if (!current) return
    const fresh = await api.get(`/api/v1/conversations/${current}`)
    if (conversation.value?.conversation_id !== current) return
    conversation.value = fresh
    const messages = fresh.messages || []
    if (messages.length !== conversationMessages.value.length) conversationMessages.value = messages
  }

  /* Reopen the last conversation instead of starting a new one on every load:
     failure analysis is appended to the conversation that owns the run, and a
     reload used to leave the user watching an empty one. */
  async function ensureConversation() {
    if (conversation.value) return conversation.value
    const existing = (await api.get('/api/v1/conversations').catch(() => ({}))).conversations || []
    const recent = existing.find((item) => (item.messages || []).length)
    conversation.value = recent || await api.createConversation({ title: 'DataFlow workbench' })
    conversationMessages.value = conversation.value.messages || []
    return conversation.value
  }

  async function sendMessage(text) {
    const content = text.trim()
    if (!content || sending.value || startingConversation.value) return
    sending.value = true
    try {
      const rows = parseInputRows()
      await ensureConversation()
      const payload = {
        content,
        allow_custom: allowCustom.value,
        ...(rows ? { input_rows: rows } : {}),
        ...(selectedDatasetId.value ? { dataset_id: selectedDatasetId.value } : {}),
      }
      const data = await api.sendMessage(conversation.value.conversation_id, payload)
      conversation.value = data.conversation
      conversationMessages.value = data.conversation.messages || []
      if (data.run_id) {
        await selectRun(data.run_id)
        await refreshRuns()
      }
      return true
    } catch (error) {
      notify(error.message)
      return false
    } finally {
      sending.value = false
    }
  }

  async function startConversation() {
    if (startingConversation.value || sending.value) return
    startingConversation.value = true
    try {
      const fresh = await api.createConversation({ title: 'DataFlow workbench' })
      ++selectionVersion
      closeStream()
      conversation.value = fresh
      conversationMessages.value = []
      selectedId.value = ''
      resetRunState()
      notify('已创建新对话', 'ok')
    } catch (error) {
      notify(error.message)
    } finally {
      startingConversation.value = false
    }
  }

  /* Run pipeline replays the run's own snapshot; this sends the data the
     composer currently holds to a fresh run that reuses the same pipeline. */
  async function rerunWithCurrentInput() {
    if (!selectedId.value || executing.value) return
    executing.value = true
    try {
      const rows = parseInputRows()
      const payload = selectedDatasetId.value ? { dataset_id: selectedDatasetId.value } : { input_rows: rows }
      if (!selectedDatasetId.value && !rows) throw new Error('请先在输入区提供数据')
      const created = await api.post(`/api/v1/runs/${selectedId.value}/rerun`, payload)
      notify(`已复用该 pipeline 对 ${created.rows} 行新数据执行`, 'ok')
      await selectRun(created.run_id)
      await refreshRuns()
      await refreshConversation()
    } catch (error) {
      notify(error.message)
    } finally {
      executing.value = false
    }
  }

  async function executePipeline() {
    if (!selectedId.value || executing.value) return
    executing.value = true
    try {
      await api.execute(selectedId.value)
      notify('已提交执行请求', 'info')
      await refreshRuns()
      if (selectedId.value) connectStream(selectedId.value)
    } catch (error) {
      notify(error.message)
    } finally {
      executing.value = false
    }
  }

  const loadResources = () => guard(async () => (resources.value = (await api.resources()).resources))
  const loadDatasets = () => guard(async () => {
    datasets.value = (await api.datasets()).datasets
  })

  async function selectDataset(id) {
    selectedDatasetId.value = id
    if (!id) return
    await guard(async () => {
      const data = await api.datasetPreview(id)
      if (data.rows?.length) inputText.value = JSON.stringify(data.rows, null, 2)
    })
  }

  async function bootstrap() {
    await guard(async () => (backendMode.value = (await api.health()).backend))
    await Promise.all([loadResources(), loadDatasets(), guard(refreshRuns)])
    await guard(ensureConversation)
    // Open the most recent run so the workspace is never empty on load.
    const initial = conversation.value?.active_run_id || runs.value[0]?.run_id
    if (!selectedId.value && initial) await guard(() => selectRun(initial))
    else if (selectedId.value) connectStream(selectedId.value)
    poller = setInterval(() => {
      if (startingConversation.value) return
      guard(refreshRuns)
      guard(refreshConversation)
    }, 5000)
  }

  function teardown() {
    clearInterval(poller)
    clearTimeout(refreshHandle)
    refreshHandle = null
    closeStream()
  }

  return {
    backendMode, toasts, notify, dismiss,
    runs, visibleRuns, runQuery, runFilter, selectedId, selected, events, timeline, agentOutputs,
    stages, selectedStage, collaboration, runSkills, runEvidence, deletingRuns,
    datasets, selectedDatasetId, inputText, allowCustom, inputSummary,
    resources, models, conversation, conversationMessages, liveMessages, chatMessages,
    loadingRuns, loadingRun, sending, executing, startingConversation,
    currentState, steps, isRunning, canExecute,
    bootstrap, teardown, refreshRuns, selectRun, deleteRun, sendMessage, startConversation,
    executePipeline, rerunWithCurrentInput, loadResources, loadDatasets, selectDataset,
    refreshConversation, guard,
  }
}

let store = null
export function useWorkbench() {
  if (!store) store = createStore()
  return store
}
