import { reactive } from 'vue'

import { fetchDemoState } from '../api/demoApi.js'

// 单例状态：Commit 7 先集中管理 /state 轮询与展示字段。
const state = reactive({
  snapshot: null,
  sourceHealth: {},
  warnings: [],
  pollHealth: 'unknown',
  loading: false,
  lastUpdatedAt: '',
  errorMessage: '',
  errorCode: '',
  // 前台可见时轮询周期（ms），对齐 8890 Commit 7 要求。
  visiblePollMs: 2000,
  // 页面隐藏时降低轮询频率，减少无效请求。
  hiddenPollMs: 5000
})

let pollingRefCount = 0
let pollingTimer = null
let inFlightPromise = null
let visibilityListenerBound = false

function getCurrentPollMs() {
  if (typeof document !== 'undefined' && document.hidden) {
    return state.hiddenPollMs
  }
  return state.visiblePollMs
}

function scheduleNextTick(delayMs) {
  if (pollingTimer) {
    clearTimeout(pollingTimer)
  }
  pollingTimer = setTimeout(() => {
    refreshNow()
      .catch(() => {
        // 刷新异常已在 refreshNow 内部写入状态，这里无需重复处理。
      })
      .finally(() => {
        if (pollingRefCount > 0) {
          scheduleNextTick(getCurrentPollMs())
        }
      })
  }, delayMs)
}

function bindVisibilityListener() {
  if (visibilityListenerBound || typeof document === 'undefined') {
    return
  }
  document.addEventListener('visibilitychange', handleVisibilityChange)
  visibilityListenerBound = true
}

function unbindVisibilityListener() {
  if (!visibilityListenerBound || typeof document === 'undefined') {
    return
  }
  document.removeEventListener('visibilitychange', handleVisibilityChange)
  visibilityListenerBound = false
}

function handleVisibilityChange() {
  // 切换可见性后立即触发一轮刷新，避免显示过期数据。
  if (pollingRefCount <= 0) {
    return
  }
  scheduleNextTick(0)
}

export function useDemoStateStore() {
  return {
    state,
    startPolling,
    stopPolling,
    refreshNow
  }
}

export async function refreshNow() {
  if (inFlightPromise) {
    return inFlightPromise
  }

  state.loading = !state.snapshot
  inFlightPromise = fetchDemoState()
    .then((data) => {
      // 同步更新核心状态字段，供 KPI/Cluster/Entry 组件消费。
      state.snapshot = data
      state.sourceHealth = data.source_health || {}
      state.warnings = Array.isArray(data.warnings) ? data.warnings : []
      state.pollHealth = data?.derived?.poll_health || inferPollHealthFromSource(state.sourceHealth)
      state.lastUpdatedAt = new Date().toISOString()
      state.errorMessage = ''
      state.errorCode = ''
    })
    .catch((error) => {
      state.errorMessage = error?.message || 'failed to fetch /state'
      state.errorCode = error?.code || ''
      // 请求失败时进入降级态，但保留最近成功快照继续展示。
      state.pollHealth = 'degraded'
      throw error
    })
    .finally(() => {
      state.loading = false
      inFlightPromise = null
    })

  return inFlightPromise
}

function inferPollHealthFromSource(sourceHealth) {
  const values = Object.values(sourceHealth || {})
  if (!values.length) {
    return 'unknown'
  }
  return values.every((value) => value === 'ok') ? 'ok' : 'degraded'
}

function startPolling() {
  pollingRefCount += 1
  if (pollingRefCount > 1) {
    return
  }

  bindVisibilityListener()
  scheduleNextTick(0)
}

function stopPolling() {
  if (pollingRefCount <= 0) {
    return
  }
  pollingRefCount -= 1
  if (pollingRefCount > 0) {
    return
  }

  if (pollingTimer) {
    clearTimeout(pollingTimer)
    pollingTimer = null
  }
  unbindVisibilityListener()
}
