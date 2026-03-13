<script setup>
import { computed } from 'vue'

import { useEventStore } from '../../../stores/eventStore.js'

const eventStore = useEventStore()

// 事件列表展示层倒序：保持 store 内部升序，UI 展示改为最新在最上。
const events = computed(() => {
  const source = Array.isArray(eventStore.state.visibleEvents) ? eventStore.state.visibleEvents : []
  return [...source].reverse()
})
const loading = computed(() => eventStore.state.loading)
const sinceSeq = computed(() => Math.max(0, Number(eventStore.state.nextSeq || 1) - 1))
const lastUpdatedAt = computed(() => eventStore.state.lastUpdatedAt)

function rowClass(event) {
  const severity = String(event?.severity || '').toLowerCase()
  if (severity === 'error') {
    return 'event-row event-row--error'
  }
  if (severity === 'warn' || severity === 'warning') {
    return 'event-row event-row--warn'
  }
  return 'event-row'
}

function formatTs(ts) {
  if (!ts) {
    return '--'
  }
  const date = new Date(ts)
  if (Number.isNaN(date.getTime())) {
    return ts
  }
  return date.toLocaleTimeString()
}
</script>

<template>
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">Event Stream</h3>
      <span class="chip">since_seq: {{ sinceSeq }}</span>
    </header>
    <div class="panel__body subgrid">
      <p v-if="lastUpdatedAt" class="panel__meta">last_updated_at: {{ lastUpdatedAt }}</p>

      <article v-if="loading && !events.length" class="empty-state">
        <p><strong>事件加载中</strong></p>
        <p>正在增量拉取 /api/demo/events。</p>
      </article>

      <article v-else-if="!events.length" class="empty-state">
        <p><strong>暂无可见事件</strong></p>
        <p>请检查筛选器，或触发 stop/start/reload 观察事件流更新。</p>
      </article>

      <article v-for="event in events" v-else :key="event.seq" :class="rowClass(event)">
        <header class="event-row__head">
          <span class="chip">#{{ event.seq }}</span>
          <span class="chip">{{ event.type || '--' }}</span>
          <span class="chip">{{ event.entity_type || '--' }}:{{ event.entity_id || '--' }}</span>
          <span class="panel__meta">{{ formatTs(event.ts) }}</span>
        </header>
        <p class="event-row__title">{{ event.title || '--' }}</p>
        <p class="event-row__msg">{{ event.message || '--' }}</p>
        <p v-if="event.correlation_id" class="panel__meta">
          correlation_id: {{ event.correlation_id }}
        </p>
      </article>
    </div>
  </article>
</template>

<style scoped>
.event-row {
  border: 1px solid var(--line);
  border-radius: var(--radius-md);
  background: rgba(255, 255, 255, 0.82);
  padding: 10px 12px;
  display: grid;
  gap: 6px;
}

.event-row--warn {
  border-color: rgba(190, 95, 34, 0.28);
  background: rgba(190, 95, 34, 0.08);
}

.event-row--error {
  border-color: rgba(196, 63, 84, 0.32);
  background: rgba(196, 63, 84, 0.1);
}

.event-row__head {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  align-items: center;
}

.event-row__title {
  font-weight: 700;
  font-size: 0.92rem;
}

.event-row__msg {
  font-size: 0.84rem;
  color: var(--ink-1);
  word-break: break-word;
}
</style>
