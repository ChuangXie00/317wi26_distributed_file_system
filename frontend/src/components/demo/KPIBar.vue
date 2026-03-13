<script setup>
import LamportKPI from './kpi/LamportKPI.vue'
import LeaderKPI from './kpi/LeaderKPI.vue'
import NodeHealthKPI from './kpi/NodeHealthKPI.vue'
import RecoveryKPI from './kpi/RecoveryKPI.vue'
</script>

<template>
  <section class="panel reveal" style="--delay: 90ms">
    <header class="panel__head">
      <h2 class="panel__title">KPI 概览</h2>
      <span class="panel__meta">Commit 6：静态空态，Commit 7 接入真实数据</span>
    </header>

    <div class="panel__body subgrid subgrid--2 kpi-grid">
      <LeaderKPI />
      <NodeHealthKPI />
      <RecoveryKPI />
      <LamportKPI />
    </div>
  </section>
</template>

<style scoped>
.kpi-grid {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

@media (max-width: 1120px) {
  .kpi-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .kpi-grid {
    grid-template-columns: 1fr;
  }
}
</style>
