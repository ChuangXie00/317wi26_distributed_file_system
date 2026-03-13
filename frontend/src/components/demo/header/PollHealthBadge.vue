<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

// 轮询健康态：优先使用后端 derived.poll_health，失败时回退到 degraded。
const pollHealth = computed(() => demoStateStore.state.pollHealth || 'unknown')
const warningCount = computed(() => (demoStateStore.state.warnings || []).length)

const badgeClass = computed(() => {
  if (pollHealth.value === 'ok') {
    return 'chip chip--ok'
  }
  if (pollHealth.value === 'degraded') {
    return 'chip chip--warn'
  }
  return 'chip'
})

const badgeText = computed(() => {
  if (pollHealth.value === 'ok') {
    return 'Poll Health · OK'
  }
  if (pollHealth.value === 'degraded') {
    return `Poll Health · DEGRADED (${warningCount.value})`
  }
  return 'Poll Health · UNKNOWN'
})
</script>

<template>
  <div :class="badgeClass">
    <span class="dot" />
    <span>{{ badgeText }}</span>
  </div>
</template>

<style scoped>
.dot {
  width: 8px;
  height: 8px;
  border-radius: 999px;
  background: currentColor;
}
</style>
