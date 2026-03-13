<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

// 收敛判定数据来源：/api/demo/state 的 derived 视图。
const derived = computed(() => demoStateStore.state.snapshot?.derived || {})

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
  <article class="kpi-tile">
    <p class="kpi-tile__label">Convergence</p>
    <p class="kpi-tile__value">{{ boolText(singleObservedLeader) }} / {{ boolText(singleWritableLeader) }}</p>
    <div class="chip-row">
      <span :class="boolChipClass(singleObservedLeader)">observed</span>
      <span :class="boolChipClass(singleWritableLeader)">writable</span>
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
