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

async function requestDemo(path, { method = 'GET', signal } = {}) {
  // 统一请求入口：读取标准包络并把异常映射为 DemoApiError。
  const response = await fetch(`${DEMO_API_BASE_URL}${path}`, {
    method,
    headers: {
      Accept: 'application/json'
    },
    signal
  })

  const body = await readJsonBody(response)

  if (!response.ok || !body?.ok) {
    const error = body?.error || {}
    throw new DemoApiError({
      message: error.message || response.statusText || 'demo api request failed',
      code: error.code || '',
      status: response.status,
      details: error.details ?? null
    })
  }

  return body.data || {}
}

async function readJsonBody(response) {
  // 统一 JSON 解析：即使后端异常返回文本，也保证调用方拿到对象。
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
