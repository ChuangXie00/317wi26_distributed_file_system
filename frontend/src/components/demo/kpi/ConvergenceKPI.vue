<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

// 收敛判定数据来源：/api/demo/state 的 derived 视图。
const derived = computed(() => demoStateStore.state.snapshot?.derived || {})
const observedLeaderId = computed(() => demoStateStore.state.snapshot?.leader_view?.leader || '--')

const singleObservedLeader = computed(() => {
  const value = derived.value?.single_observed_leader
  return typeof value === 'boolean' ? value : null
})

const singleWritableLeader = computed(() => {
  const value = derived.value?.single_writable_leader
  return typeof value === 'boolean' ? value : null
})

function boolText(value) {
  if (value === true) {
    return 'true'
  }
  if (value === false) {
    return 'false'
  }
  return '--'
}

function boolChipClass(value) {
  if (value === true) {
    return 'chip chip--ok'
  }
  if (value === false) {
    return 'chip chip--warn'
  }
  return 'chip'
}
</script>

<template>
  <article class="kpi-tile kpi-detail">
    <p class="kpi-tile__label">Convergence</p>
    <div class="kpi-detail__body">
      <span :class="boolChipClass(singleObservedLeader)">
        single_observed_leader: {{ boolText(singleObservedLeader) }}
      </span>
      <span :class="boolChipClass(singleWritableLeader)">
        single_writable_leader: {{ boolText(singleWritableLeader) }}
      </span>
      <p class="empty-state">observed_leader={{ observedLeaderId }}</p>
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
