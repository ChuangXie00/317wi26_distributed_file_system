<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

// 收敛判定来源：直接读取 /api/demo/state 的 derived 字段。
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
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">Convergence</h3>
    </header>
    <div class="panel__body subgrid">
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
