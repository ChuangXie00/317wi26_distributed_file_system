<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

// Lamport KPI：展示当前逻辑时钟与 last_applied_lamport。
const lamport = computed(() => demoStateStore.state.snapshot?.leader_view?.lamport ?? '--')
const lastApplied = computed(() => demoStateStore.state.snapshot?.leader_view?.last_applied_lamport ?? '--')
</script>

<template>
  <article class="kpi-tile">
    <p class="kpi-tile__label">Lamport Clock</p>
    <p class="kpi-tile__value">{{ lamport }}</p>
    <p class="empty-state">last_applied={{ lastApplied }}</p>
  </article>
</template>
