<script setup>
import { computed } from 'vue'

import { useMetricStore } from '../../../stores/metricStore'

const metricStore = useMetricStore()

// 恢复耗时 KPI：有样本时显示秒数，无样本时显示 null。
const recoverySeconds = computed(() => metricStore.state.metrics.failover_recovery_seconds)
const leaderSwitchCount = computed(() => metricStore.state.metrics.leader_switch_count || 0)

const displayValue = computed(() => {
  if (recoverySeconds.value === null || Number.isNaN(recoverySeconds.value)) {
    return 'null'
  }
  return `${recoverySeconds.value.toFixed(3)} s`
})
</script>

<template>
  <article class="kpi-tile">
    <p class="kpi-tile__label">Failover Recovery</p>
    <p class="kpi-tile__value">{{ displayValue }}</p>
    <p class="empty-state">leader_switch_count={{ leaderSwitchCount }}</p>
  </article>
</template>
