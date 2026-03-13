<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

// 节点健康 KPI：直接读取 membership_view.summary。
const summary = computed(
  () =>
    demoStateStore.state.snapshot?.membership_view?.summary || {
      alive: 0,
      suspected: 0,
      dead: 0,
      total: 0
    }
)
</script>

<template>
  <article class="kpi-tile">
    <p class="kpi-tile__label">Node Health</p>
    <p class="kpi-tile__value">{{ summary.alive }} / {{ summary.total }}</p>
    <p class="empty-state">
      suspected={{ summary.suspected }}, dead={{ summary.dead }}
    </p>
  </article>
</template>
