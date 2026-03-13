<script setup>
import { computed, ref } from 'vue'

import { useActionStore } from '../../../stores/actionStore.js'

const actionStore = useActionStore()
const copied = ref(false)

const toast = computed(() => actionStore.state.toast)
const isVisible = computed(() => toast.value.visible)
const isError = computed(() => toast.value.kind === 'error')

async function copyError() {
  copied.value = false
  const ok = await actionStore.copyLastError()
  copied.value = ok
  if (!ok) {
    return
  }
  setTimeout(() => {
    copied.value = false
  }, 1500)
}
</script>

<template>
  <article v-if="isVisible" :class="['toast-card', isError ? 'toast-card--error' : 'toast-card--success']">
    <p class="toast-card__title">{{ toast.title }}</p>
    <p class="toast-card__body">{{ toast.message }}</p>

    <template v-if="isError">
      <p class="toast-card__meta">错误码：{{ toast.errorCode || '--' }}</p>
      <p class="toast-card__meta">{{ toast.errorMessage || '--' }}</p>

      <div class="btn-row toast-actions">
        <button type="button" class="btn" @click="copyError">
          {{ copied ? '已复制' : '复制错误详情' }}
        </button>
        <button type="button" class="btn" @click="actionStore.prepareRetry()">同参重试</button>
        <button type="button" class="btn" @click="actionStore.dismissToast()">关闭</button>
      </div>
    </template>

    <template v-else>
      <p class="toast-card__meta">
        duration={{ toast.actionResult?.duration_ms ?? '--' }}ms · normalized={{
          toast.actionResult?.normalized_action || '--'
        }}
      </p>
    </template>
  </article>
</template>

<style scoped>
.toast-card {
  border-radius: 12px;
  padding: 10px;
  animation: settle 0.35s ease-out both;
}

.toast-card--success {
  border: 1px solid rgba(31, 143, 103, 0.34);
  background: rgba(31, 143, 103, 0.12);
  color: #124d37;
}

.toast-card--error {
  border: 1px solid rgba(196, 63, 84, 0.35);
  background: rgba(196, 63, 84, 0.12);
  color: #6e1e30;
}

.toast-card__title {
  font-size: 0.82rem;
  font-weight: 700;
}

.toast-card__body {
  margin-top: 2px;
  font-size: 0.78rem;
}

.toast-card__meta {
  margin-top: 2px;
  font-size: 0.76rem;
}

.toast-actions {
  margin-top: 6px;
}

@keyframes settle {
  from {
    transform: translateY(8px);
    opacity: 0;
  }

  to {
    transform: translateY(0);
    opacity: 1;
  }
}
</style>
