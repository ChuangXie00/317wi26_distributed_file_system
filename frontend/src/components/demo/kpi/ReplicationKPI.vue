<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

// 复制状态数据来源：/api/demo/state 的 replication_view。
const replicationView = computed(() => demoStateStore.state.snapshot?.replication_view || {})
const heartbeat = computed(() => replicationView.value?.heartbeat || {})
const snapshot = computed(() => replicationView.value?.snapshot || {})
const sync = computed(() => replicationView.value?.sync || {})
const takeover = computed(() => replicationView.value?.takeover || {})

const heartbeatText = computed(() => {
  if (typeof heartbeat.value?.alive === 'boolean') {
    return heartbeat.value.alive ? 'alive' : 'dead'
  }
  const observedAt = String(heartbeat.value?.last_observed_at || '').trim()
  return observedAt ? `last_observed=${formatTs(observedAt)}` : '--'
})

const snapshotText = computed(() => {
  const successAt = String(snapshot.value?.last_snapshot_success_at || '').trim()
  if (successAt) {
    return `success_at=${formatTs(successAt)}`
  }
  const sentAt = String(snapshot.value?.last_snapshot_sent_at || '').trim()
  return sentAt ? `sent_at=${formatTs(sentAt)}` : '--'
})

const syncText = computed(() => {
  const appliedAt = String(sync.value?.last_sync_applied_at || '').trim()
  const source = String(sync.value?.last_sync_source || '').trim()
  if (appliedAt && source) {
    return `${source} @ ${formatTs(appliedAt)}`
  }
  if (appliedAt) {
    return `applied_at=${formatTs(appliedAt)}`
  }
  return '--'
})

const takeoverText = computed(() => {
  const result = String(takeover.value?.last_takeover_result || '').trim()
  if (result) {
    return result
  }
  const reason = String(takeover.value?.last_takeover_reason || '').trim()
  return reason || '--'
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
  <article class="kpi-tile kpi-detail">
    <p class="kpi-tile__label">Replication</p>
    <div class="kpi-detail__body">
      <span class="chip">heartbeat: {{ heartbeatText }}</span>
      <span class="chip">snapshot: {{ snapshotText }}</span>
      <span class="chip">sync: {{ syncText }}</span>
      <span class="chip">takeover: {{ takeoverText }}</span>
    </div>
  </article>
</template>

<style scoped>
.kpi-detail {
  display: grid;
  gap: 8px;
}

.kpi-detail__body {
  display: grid;
  gap: 8px;
}
</style>
