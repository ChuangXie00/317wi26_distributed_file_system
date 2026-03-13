<script setup>
import ConvergenceCard from './election/ConvergenceCard.vue'
import RecoveryTimer from './election/RecoveryTimer.vue'
import ReplicationCard from './election/ReplicationCard.vue'
</script>

<template>
  <section class="panel reveal" style="--delay: 190ms">
    <header class="panel__head">
      <h2 class="panel__title">Election &amp; Replication</h2>
      <span class="panel__meta">收敛判定 / 复制状态 / 恢复计时器</span>
    </header>
    <div class="panel__body subgrid subgrid--3">
      <ConvergenceCard />
      <ReplicationCard />
      <RecoveryTimer />
    </div>
  </section>
</template>
