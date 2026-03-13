<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

// leader KPI：展示当前观测 leader 与 epoch/term。
const leader = computed(() => demoStateStore.state.snapshot?.leader_view?.leader || '--')
const epoch = computed(() => demoStateStore.state.snapshot?.leader_view?.leader_epoch ?? '--')
const term = computed(() => demoStateStore.state.snapshot?.leader_view?.current_term ?? '--')
</script>

<template>
  <article class="kpi-tile">
    <p class="kpi-tile__label">Observed Leader</p>
    <p class="kpi-tile__value">{{ leader }}</p>
    <p class="empty-state">epoch={{ epoch }} · term={{ term }}</p>
  </article>
</template>
