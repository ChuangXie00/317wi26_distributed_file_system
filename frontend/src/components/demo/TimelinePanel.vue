<script setup>
import { computed } from 'vue'

import { useEventStore } from '../../stores/eventStore.js'
import EventFilter from './timeline/EventFilter.vue'
import EventStream from './timeline/EventStream.vue'

const eventStore = useEventStore()

// 时间线顶部统计：用于快速确认筛选结果、增量游标与异常状态。
const totalEvents = computed(() => eventStore.state.events.length)
const visibleEvents = computed(() => eventStore.state.visibleEvents.length)
const nextSeq = computed(() => eventStore.state.nextSeq || 1)
const hasMore = computed(() => eventStore.state.hasMore)
const errorMessage = computed(() => eventStore.state.errorMessage)
</script>

<template>
  <section class="panel reveal" style="--delay: 260ms">
    <header class="panel__head">
      <h2 class="panel__title">Timeline Panel</h2>
      <span class="panel__meta">
        {{ visibleEvents }}/{{ totalEvents }} events · next_seq={{ nextSeq }} ·
        {{ hasMore ? 'has_more=true' : 'has_more=false' }}
      </span>
    </header>
    <div class="panel__body subgrid">
      <p v-if="errorMessage" class="empty-state">
        events 拉取失败：{{ errorMessage }}
      </p>
      <EventFilter />
      <EventStream />
    </div>
  </section>
</template>
