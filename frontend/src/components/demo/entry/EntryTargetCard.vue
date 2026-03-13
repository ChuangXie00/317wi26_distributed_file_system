<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

// Entry 卡片：展示 active/pending/decision/switch_count。
const entry = computed(
  () =>
    demoStateStore.state.snapshot?.entry || {
      active_leader_id: '--',
      pending_leader_id: '--',
      last_decision: '--',
      switch_count: 0
    }
)

const chipClass = computed(() => {
  if (!entry.value.active_leader_id || entry.value.active_leader_id === '--') {
    return 'chip chip--warn'
  }
  return 'chip chip--ok'
})
</script>

<template>
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">Entry Target</h3>
      <span :class="chipClass">active={{ entry.active_leader_id || '--' }}</span>
    </header>
    <div class="panel__body subgrid">
      <p class="kpi-tile__value">active_leader_id: {{ entry.active_leader_id || '--' }}</p>
      <p class="empty-state">pending_leader_id={{ entry.pending_leader_id || '--' }}</p>
      <p class="empty-state">last_decision={{ entry.last_decision || '--' }}</p>
      <p class="empty-state">entry_switch_count={{ entry.switch_count ?? 0 }}</p>
    </div>
  </article>
</template>
