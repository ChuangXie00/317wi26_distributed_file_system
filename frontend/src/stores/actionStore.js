import { reactive } from 'vue'

import { DemoApiError, postDemoAction } from '../api/demoApi.js'
import { useDemoStateStore } from './demoStateStore.js'
import { useMetricStore } from './metricStore.js'

const demoStateStore = useDemoStateStore()
const metricStore = useMetricStore()

// 单例状态：Commit 8 负责确认弹窗、动作提交与回执展示。
const state = reactive({
  confirmModal: {
    visible: false,
    action: '',
    target: '',
    reason: 'demo_drill'
  },
  submitting: false,
  toast: {
    visible: false,
    kind: 'success',
    title: '',
    message: '',
    actionResult: null,
    errorCode: '',
    errorMessage: '',
    errorDetails: null
  },
  // 最近一次已确认提交的 payload，用于“同参重试”。
  lastSubmittedPayload: null
})

let toastTimer = null

export function useActionStore() {
  return {
    state,
    openConfirm,
    closeConfirm,
    confirmAndExecute,
    dismissToast,
    copyLastError,
    prepareRetry
  }
}

export function openConfirm({ action, target, reason = 'demo_drill' }) {
  // 所有危险动作必须先经过确认弹窗，不允许直接执行。
  state.confirmModal.visible = true
  state.confirmModal.action = String(action || '').trim().toLowerCase()
  state.confirmModal.target = String(target || '').trim().toLowerCase()
  state.confirmModal.reason = String(reason || 'demo_drill').trim().slice(0, 128) || 'demo_drill'
}

export function closeConfirm() {
  if (state.submitting) {
    return
  }
  state.confirmModal.visible = false
}

export async function confirmAndExecute() {
  if (!state.confirmModal.visible || state.submitting) {
    return { ok: false, skipped: true }
  }

  const payload = {
    action: state.confirmModal.action,
    target: state.confirmModal.target,
    reason: state.confirmModal.reason,
    client_request_id: createClientRequestId()
  }

  state.submitting = true
  state.lastSubmittedPayload = payload

  try {
    const result = await postDemoAction(payload)
    showSuccessToast(result)

    // 动作成功后主动刷新状态与指标，并开启 30s 的 /state 高频观测窗口。
    demoStateStore.boostPolling(30000, 1000)
    void demoStateStore.refreshNow().catch(() => {})
    void metricStore.refreshNow().catch(() => {})

    return { ok: true, data: result }
  } catch (error) {
    showErrorToast(error, payload)
    return { ok: false, error }
  } finally {
    state.submitting = false
    state.confirmModal.visible = false
  }
}

function showSuccessToast(result) {
  state.toast.visible = true
  state.toast.kind = 'success'
  state.toast.title = '动作执行成功'
  state.toast.message = `${result?.normalized_action || '--'} @ ${result?.target || '--'}`
  state.toast.actionResult = result || null
  state.toast.errorCode = ''
  state.toast.errorMessage = ''
  state.toast.errorDetails = null

  resetToastTimer(5500)
}

function showErrorToast(error, payload) {
  const apiError = normalizeApiError(error)

  state.toast.visible = true
  state.toast.kind = 'error'
  state.toast.title = '动作执行失败'
  state.toast.message = `${payload.action || '--'} @ ${payload.target || '--'}`
  state.toast.actionResult = null
  state.toast.errorCode = apiError.code
  state.toast.errorMessage = apiError.message
  state.toast.errorDetails = apiError.details

  // 失败回执保持展示，便于用户复制错误详情与重试。
  resetToastTimer(null)
}

function normalizeApiError(error) {
  if (error instanceof DemoApiError) {
    return {
      code: error.code || 'DEMO-CTL-003',
      message: error.message || 'action failed',
      details: error.details ?? null
    }
  }
  return {
    code: 'DEMO-CTL-003',
    message: error?.message || 'action failed',
    details: null
  }
}

function resetToastTimer(delayMs) {
  if (toastTimer) {
    clearTimeout(toastTimer)
    toastTimer = null
  }
  if (typeof delayMs !== 'number') {
    return
  }
  toastTimer = setTimeout(() => {
    dismissToast()
  }, delayMs)
}

export function dismissToast() {
  state.toast.visible = false
}

export function prepareRetry() {
  // “同参重试”仍走确认弹窗，保证高风险操作不绕过二次确认。
  if (!state.lastSubmittedPayload) {
    return
  }
  openConfirm({
    action: state.lastSubmittedPayload.action,
    target: state.lastSubmittedPayload.target,
    reason: state.lastSubmittedPayload.reason
  })
}

export async function copyLastError() {
  if (!state.toast.errorCode) {
    return false
  }
  const text = JSON.stringify(
    {
      code: state.toast.errorCode,
      message: state.toast.errorMessage,
      details: state.toast.errorDetails
    },
    null,
    2
  )
  if (typeof navigator === 'undefined' || !navigator.clipboard?.writeText) {
    return false
  }
  await navigator.clipboard.writeText(text)
  return true
}

function createClientRequestId() {
  // 客户端请求 ID：用于关联 action 与后端事件流。
  const ts = Date.now().toString(36)
  const rand = Math.random().toString(36).slice(2, 8)
  return `ui_${ts}_${rand}`
}
