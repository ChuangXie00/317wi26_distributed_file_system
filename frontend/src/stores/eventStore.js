import { reactive } from 'vue'

import { fetchDemoEvents } from '../api/demoApi.js'

const DEFAULT_VISIBLE_POLL_MS = 1000
const DEFAULT_HIDDEN_POLL_MS = 3000
const EVENT_BUFFER_MAX = 2000

const FILTER_KEYS = ['meta', 'storage', 'file', 'error', 'action']

// 单例状态：负责 /events 增量拉取、去重合并、筛选展示。
const state = reactive({
  events: [],
  visibleEvents: [],
  nextSeq: 1,
  hasMore: false,
  loading: false,
  lastUpdatedAt: '',
  errorMessage: '',
  errorCode: '',
  visiblePollMs: DEFAULT_VISIBLE_POLL_MS,
  hiddenPollMs: DEFAULT_HIDDEN_POLL_MS,
  filters: {
    meta: true,
    storage: true,
    file: true,
    error: true,
    action: true
  }
})

// seq 去重索引：防止轮询窗口重叠导致重复事件进入列表。
const seenSeq = new Set()

let pollingRefCount = 0
let pollingTimer = null
let inFlightPromise = null
let visibilityListenerBound = false

export function useEventStore() {
  return {
    state,
    startPolling,
    stopPolling,
    refreshNow,
    toggleFilter,
    setFilter,
    setAllFilters
  }
}

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
        // 错误状态已写入 store，调度层无需重复处理。
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

export async function refreshNow() {
  if (inFlightPromise) {
    return inFlightPromise
  }

  state.loading = !state.lastUpdatedAt
  // 增量游标：请求时使用 next_seq - 1，与后端契约保持一致。
  const sinceSeq = Math.max(0, Number(state.nextSeq || 1) - 1)
  inFlightPromise = fetchDemoEvents({
    sinceSeq,
    limit: 200
  })
    .then((data) => {
      const incoming = Array.isArray(data?.events) ? data.events : []
      mergeIncomingEvents(incoming)

      const nextSeq = Number(data?.next_seq || 0)
      if (Number.isFinite(nextSeq) && nextSeq > 0) {
        state.nextSeq = Math.max(state.nextSeq, nextSeq)
      }

      state.hasMore = Boolean(data?.has_more)
      state.lastUpdatedAt = new Date().toISOString()
      state.errorMessage = ''
      state.errorCode = ''
      recomputeVisibleEvents()
    })
    .catch((error) => {
      state.errorMessage = error?.message || 'failed to fetch /events'
      state.errorCode = error?.code || ''
      throw error
    })
    .finally(() => {
      state.loading = false
      inFlightPromise = null
    })

  return inFlightPromise
}

function mergeIncomingEvents(incomingEvents) {
  // 合并规则：按 seq 去重，保持升序，超出容量后淘汰最旧事件。
  let appended = false
  for (const event of incomingEvents) {
    const seq = Number(event?.seq)
    if (!Number.isFinite(seq) || seq <= 0) {
      continue
    }
    if (seenSeq.has(seq)) {
      continue
    }

    seenSeq.add(seq)
    state.events.push(normalizeEvent(event))
    appended = true
  }

  if (appended) {
    state.events.sort((a, b) => a.seq - b.seq)
  }

  while (state.events.length > EVENT_BUFFER_MAX) {
    const removed = state.events.shift()
    if (!removed) {
      continue
    }
    seenSeq.delete(removed.seq)
  }
}

function normalizeEvent(event) {
  return {
    seq: Number(event?.seq || 0),
    ts: String(event?.ts || ''),
    type: String(event?.type || ''),
    severity: String(event?.severity || ''),
    entity_type: String(event?.entity_type || ''),
    entity_id: String(event?.entity_id || ''),
    title: String(event?.title || ''),
    message: String(event?.message || ''),
    correlation_id: String(event?.correlation_id || ''),
    payload: event?.payload && typeof event.payload === 'object' ? event.payload : {}
  }
}

function recomputeVisibleEvents() {
  const enabledFilters = FILTER_KEYS.filter((key) => state.filters[key])
  if (!enabledFilters.length) {
    state.visibleEvents = []
    return
  }

  state.visibleEvents = state.events.filter((event) => {
    return enabledFilters.some((filterKey) => matchesFilter(filterKey, event))
  })
}

function matchesFilter(filterKey, event) {
  const eventType = String(event?.type || '')
  const entityType = String(event?.entity_type || '')
  const severity = String(event?.severity || '')

  if (filterKey === 'action') {
    return eventType.startsWith('action.')
  }
  if (filterKey === 'error') {
    return severity === 'error' || eventType.startsWith('error.') || eventType === 'action.failed'
  }
  if (filterKey === 'file') {
    return entityType === 'file' || eventType.startsWith('file.')
  }
  if (filterKey === 'storage') {
    return entityType === 'storage'
  }
  if (filterKey === 'meta') {
    return (
      entityType === 'meta' ||
      eventType.startsWith('leader.') ||
      eventType.startsWith('entry.') ||
      (eventType.startsWith('node.') && String(event?.entity_id || '').startsWith('meta-'))
    )
  }
  return false
}

export function toggleFilter(filterKey) {
  if (!Object.hasOwn(state.filters, filterKey)) {
    return
  }
  state.filters[filterKey] = !state.filters[filterKey]
  recomputeVisibleEvents()
}

export function setFilter(filterKey, enabled) {
  if (!Object.hasOwn(state.filters, filterKey)) {
    return
  }
  state.filters[filterKey] = Boolean(enabled)
  recomputeVisibleEvents()
}

export function setAllFilters(enabled) {
  const value = Boolean(enabled)
  for (const key of FILTER_KEYS) {
    state.filters[key] = value
  }
  recomputeVisibleEvents()
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
