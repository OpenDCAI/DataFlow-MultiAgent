/* Thin fetch wrapper. Every call surfaces the backend `detail` message,
   because those messages are the workbench's main failure explanation.

   Every request is bounded. Without a deadline a hung or half-open
   connection leaves the caller awaiting a promise that never settles, which
   is what "注册数据集一直卡着不动" looked like: the button stayed disabled
   and no error was ever shown. */
const DEFAULT_TIMEOUT_MS = 20000

async function request(path, options = {}) {
  const { timeoutMs = DEFAULT_TIMEOUT_MS, ...init } = options
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  let response
  try {
    response = await fetch(path, {
      headers: { 'Content-Type': 'application/json' },
      signal: controller.signal,
      ...init,
    })
  } catch (error) {
    if (controller.signal.aborted) {
      throw new Error(`请求超时（${Math.round(timeoutMs / 1000)} 秒未响应）：${path}`)
    }
    throw new Error(`无法连接后端：${error.message}`)
  } finally {
    clearTimeout(timer)
  }
  if (!response.ok) {
    const body = await response.json().catch(() => ({}))
    throw new Error(body.detail || `${response.status} ${response.statusText}`)
  }
  return response.status === 204 ? null : response.json()
}

const send = (method) => (path, body, options) =>
  request(path, { method, ...(body === undefined ? {} : { body: JSON.stringify(body) }), ...options })

export const api = {
  get: (path) => request(path),
  post: send('POST'),
  delete: send('DELETE'),

  health: () => request('/api/v1/health'),

  resources: () => request('/api/v1/resources'),
  registerResource: (payload) => send('POST')('/api/v1/resources', payload),
  deleteResource: (name) => send('DELETE')(`/api/v1/resources/${encodeURIComponent(name)}`),
  models: (payload) => send('POST')('/api/v1/models', payload),

  settings: () => request('/api/v1/settings'),
  saveSettings: (payload) => send('POST')('/api/v1/settings', payload),
  // Verifying a key makes a real outbound call, so it gets a longer deadline.
  testSettings: (payload) => send('POST')('/api/v1/settings/test', payload, { timeoutMs: 30000 }),

  datasets: () => request('/api/v1/datasets'),
  // Registering writes the rows to disk, so it gets a longer deadline than
  // the status calls the UI polls.
  registerDataset: (payload) => send('POST')('/api/v1/datasets', payload, { timeoutMs: 60000 }),
  datasetPreview: (id) => request(`/api/v1/datasets/${encodeURIComponent(id)}/preview`),
  deleteDataset: (id) => send('DELETE')(`/api/v1/datasets/${encodeURIComponent(id)}`),

  runs: () => request('/api/v1/runs'),
  run: (id) => request(`/api/v1/runs/${id}`),
  events: (id) => request(`/api/v1/runs/${id}/events`),
  agentOutputs: (id) => request(`/api/v1/runs/${id}/agent-outputs`),
  stages: (id) => request(`/api/v1/runs/${id}/stages`),
  collaboration: (id) => request(`/api/v1/runs/${id}/collaboration`),
  skills: (id) => request(`/api/v1/runs/${id}/skills`),
  evidence: (id) => request(`/api/v1/runs/${id}/evidence`),
  pipelineCode: (id) => request(`/api/v1/runs/${id}/pipeline-code`),
  createRun: (payload) => send('POST')('/api/v1/runs', payload),
  deleteRun: (id) => send('DELETE')(`/api/v1/runs/${id}`),
  execute: (id) => send('POST')(`/api/v1/runs/${id}/execute`),

  createConversation: (payload) => send('POST')('/api/v1/conversations', payload),
  sendMessage: (id, payload) => send('POST')(`/api/v1/conversations/${id}/messages`, payload),
}
