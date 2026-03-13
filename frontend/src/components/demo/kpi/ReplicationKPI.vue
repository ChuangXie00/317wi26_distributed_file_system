<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

// 复制状态数据来源：/api/demo/state 的 replication_view。
const replicationView = computed(() => demoStateStore.state.snapshot?.replication_view || {})
const snapshot = computed(() => replicationView.value?.snapshot || {})
const sync = computed(() => replicationView.value?.sync || {})

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
  <article class="kpi-tile">
    <p class="kpi-tile__label">Replication</p>
    <div class="chip-row">
      <span class="chip">snapshot: {{ snapshotText }}</span>
      <span class="chip">sync: {{ syncText }}</span>
    </div>
  </article>
</template>

<style scoped>
.chip-row {
  display: inline-flex;
  flex-wrap: wrap;
  gap: 8px;
}
</style>
