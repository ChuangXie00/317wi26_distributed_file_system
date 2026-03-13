<script setup>
import { computed } from 'vue'

import { useMetricStore } from '../../../stores/metricStore'

const metricStore = useMetricStore()

// 恢复计时来源：/api/demo/metrics.failover_recovery_seconds。
const metrics = computed(() => metricStore.state.metrics || {})
const recoverySeconds = computed(() => metrics.value?.failover_recovery_seconds)
const startedAt = computed(() => formatTs(metrics.value?.last_failover_started_at || ''))
const recoveredAt = computed(() => formatTs(metrics.value?.last_failover_recovered_at || ''))

const recoveryText = computed(() => {
  if (recoverySeconds.value === null || recoverySeconds.value === undefined || Number.isNaN(Number(recoverySeconds.value))) {
    return '-- s'
  }
  return `${Number(recoverySeconds.value).toFixed(3)} s`
})

function formatTs(ts) {
  if (!ts) {
    return '--'
  }
  const date = new Date(ts)
  if (Number.isNaN(date.getTime())) {
    return ts
  }
  return date.toLocaleTimeString()
}
</script>

<template>
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">Recovery Timer</h3>
    </header>
    <div class="panel__body subgrid">
      <p class="kpi-tile__value">{{ recoveryText }}</p>
      <p class="empty-state">started={{ startedAt }} / recovered={{ recoveredAt }}</p>
    </div>
  </article>
</template>
