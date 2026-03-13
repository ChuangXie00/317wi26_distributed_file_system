<script setup>
import EventFilter from './timeline/EventFilter.vue'
import EventStream from './timeline/EventStream.vue'
</script>

<template>
  <section class="panel reveal" style="--delay: 260ms">
    <header class="panel__head">
      <h2 class="panel__title">Timeline Panel</h2>
      <span class="panel__meta">events 增量拉取与筛选占位</span>
    </header>
    <div class="panel__body subgrid">
      <EventFilter />
      <EventStream />
    </div>
  </section>
</template>
