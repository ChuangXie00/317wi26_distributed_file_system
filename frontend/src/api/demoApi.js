// Demo Backend 基础地址：可通过 VITE_DEMO_API_BASE_URL 覆盖，默认指向本地 18080。
const envBaseUrl =
  (typeof import.meta !== 'undefined' && import.meta.env && import.meta.env.VITE_DEMO_API_BASE_URL) ||
  (typeof process !== 'undefined' && process.env && process.env.VITE_DEMO_API_BASE_URL) ||
  'http://127.0.0.1:18080/api/demo'

const DEMO_API_BASE_URL = String(envBaseUrl).replace(/\/$/, '')

export class DemoApiError extends Error {
  constructor({ message, code = '', status = 0, details = null }) {
    super(message)
    this.name = 'DemoApiError'
    this.code = code
    this.status = status
    this.details = details
  }
}

async function requestDemo(path, { method = 'GET', signal, body } = {}) {
  // 统一请求入口：读取标准包络，并把异常映射为 DemoApiError。
  const headers = {
    Accept: 'application/json'
  }
  if (body !== undefined) {
    headers['Content-Type'] = 'application/json'
  }

  const response = await fetch(`${DEMO_API_BASE_URL}${path}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
    signal
  })

  const responseBody = await readJsonBody(response)
  if (!response.ok || !responseBody?.ok) {
    const error = responseBody?.error || {}
    throw new DemoApiError({
      message: error.message || response.statusText || 'demo api request failed',
      code: error.code || '',
      status: response.status,
      details: error.details ?? null
    })
  }

  return responseBody.data || {}
}

async function requestDemoForm(path, { signal, formData } = {}) {
  // 文件上传走 multipart/form-data：不手工设置 Content-Type，交给浏览器自动带 boundary。
  const response = await fetch(`${DEMO_API_BASE_URL}${path}`, {
    method: 'POST',
    body: formData,
    signal
  })

  const responseBody = await readJsonBody(response)
  if (!response.ok || !responseBody?.ok) {
    const error = responseBody?.error || {}
    throw new DemoApiError({
      message: error.message || response.statusText || 'demo api request failed',
      code: error.code || '',
      status: response.status,
      details: error.details ?? null
    })
  }

  return responseBody.data || {}
}

async function readJsonBody(response) {
  // 统一 JSON 解析：即使后端返回非 JSON，也保证调用方拿到对象。
  try {
    return await response.json()
  } catch {
    return {}
  }
}

export function fetchDemoState({ signal } = {}) {
  return requestDemo('/state', { signal })
}

export function fetchDemoMetrics({ signal } = {}) {
  return requestDemo('/metrics', { signal })
}

export function postDemoAction(payload, { signal } = {}) {
  // 动作调用：由 actionStore 触发，执行 stop/start/reload 链路。
  return requestDemo('/action', {
    method: 'POST',
    signal,
    body: payload
  })
}

export function fetchDemoEvents({ sinceSeq = 0, limit = 50, types = [], signal } = {}) {
  // 事件查询：支持 since_seq 增量拉取与 type 多值筛选。
  const params = new URLSearchParams()
  params.set('since_seq', String(Math.max(0, Number(sinceSeq) || 0)))
  params.set('limit', String(Math.max(1, Math.min(200, Number(limit) || 50))))

  for (const type of types) {
    const value = String(type || '').trim()
    if (!value) {
      continue
    }
    params.append('type', value)
  }

  return requestDemo(`/events?${params.toString()}`, { signal })
}

export function postDemoFileUpload(file, { fileName = '', signal } = {}) {
  // 提交文件到 demo-backend，由后端代理完成 chunk/register/upload/commit。
  const formData = new FormData()
  formData.append('file', file)
  if (String(fileName || '').trim()) {
    formData.append('file_name', String(fileName).trim())
  }
  return requestDemoForm('/file/upload', { signal, formData })
}

export function fetchDemoFileDownload({ fileName, signal } = {}) {
  const params = new URLSearchParams()
  params.set('file_name', String(fileName || '').trim())
  return requestDemo(`/file/download?${params.toString()}`, { signal })
}
