import { reactive } from 'vue'

import { DemoApiError, fetchDemoMetrics } from '../api/demoApi.js'

// 单例状态：Commit 7 负责 /metrics 轮询与恢复指标展示。
const state = reactive({
  metrics: {
    failover_recovery_seconds: null,
    last_failover_started_at: '',
    last_failover_recovered_at: '',
    leader_switch_count: 0,
    event_backlog: 0,
    sampling_window_seconds: 900
  },
  hasSample: false,
  loading: false,
  lastUpdatedAt: '',
  errorMessage: '',
  errorCode: '',
  visiblePollMs: 2000,
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
        // 错误状态已落到 store，这里不重复抛出。
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
  if (pollingRefCount <= 0) {
    return
  }
  scheduleNextTick(0)
}

export function useMetricStore() {
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

  state.loading = !state.lastUpdatedAt
  inFlightPromise = fetchDemoMetrics()
    .then((data) => {
      state.metrics = normalizeMetrics(data)
      state.hasSample = state.metrics.failover_recovery_seconds !== null
      state.lastUpdatedAt = new Date().toISOString()
      state.errorMessage = ''
      state.errorCode = ''
    })
    .catch((error) => {
      // 503 + DEMO-MET-001 代表“无样本”，不是接口不可用。
      if (error instanceof DemoApiError && error.code === 'DEMO-MET-001') {
        state.metrics = normalizeMetrics(error.details || {})
        state.hasSample = false
        state.lastUpdatedAt = new Date().toISOString()
        state.errorMessage = ''
        state.errorCode = ''
        return
      }

      state.errorMessage = error?.message || 'failed to fetch /metrics'
      state.errorCode = error?.code || ''
      throw error
    })
    .finally(() => {
      state.loading = false
      inFlightPromise = null
    })

  return inFlightPromise
}

function normalizeMetrics(data) {
  return {
    failover_recovery_seconds:
      data?.failover_recovery_seconds === null || data?.failover_recovery_seconds === undefined
        ? null
        : Number(data.failover_recovery_seconds),
    last_failover_started_at: data?.last_failover_started_at || '',
    last_failover_recovered_at: data?.last_failover_recovered_at || '',
    leader_switch_count: Number(data?.leader_switch_count || 0),
    event_backlog: Number(data?.event_backlog || 0),
    sampling_window_seconds: Number(data?.sampling_window_seconds || 900)
  }
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
