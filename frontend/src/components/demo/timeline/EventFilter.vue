<script setup>
import { computed } from 'vue'

import { useEventStore } from '../../../stores/eventStore.js'

const eventStore = useEventStore()

// Commit 9 约定的五类筛选键，需与 8890 DoD 一致。
const FILTER_ITEMS = [
  { key: 'meta', label: 'meta' },
  { key: 'storage', label: 'storage' },
  { key: 'file', label: 'file' },
  { key: 'error', label: 'error' },
  { key: 'action', label: 'action' }
]

// 当前已启用筛选数量：用于头部快速反馈筛选状态。
const enabledCount = computed(() => {
  return FILTER_ITEMS.reduce((count, item) => {
    return count + (eventStore.state.filters[item.key] ? 1 : 0)
  }, 0)
})

function isActive(filterKey) {
  return Boolean(eventStore.state.filters[filterKey])
}

function toggleFilter(filterKey) {
  eventStore.toggleFilter(filterKey)
}

function enableAll() {
  eventStore.setAllFilters(true)
}

function clearAll() {
  eventStore.setAllFilters(false)
}
</script>

<template>
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">Event Filter</h3>
      <span class="panel__meta">已启用 {{ enabledCount }}/{{ FILTER_ITEMS.length }}</span>
    </header>
    <div class="panel__body subgrid">
      <div class="btn-row">
        <button
          v-for="item in FILTER_ITEMS"
          :key="item.key"
          type="button"
          class="btn btn-filter"
          :class="{ 'btn-filter--active': isActive(item.key) }"
          @click="toggleFilter(item.key)"
        >
          {{ item.label }}
        </button>
      </div>
      <div class="btn-row">
        <button type="button" class="btn" @click="enableAll">全选</button>
        <button type="button" class="btn" @click="clearAll">全不选</button>
      </div>
    </div>
  </article>
</template>

<style scoped>
.btn-filter--active {
  border-color: rgba(30, 106, 214, 0.45);
  color: var(--accent);
  background: var(--accent-soft);
}
</style>
