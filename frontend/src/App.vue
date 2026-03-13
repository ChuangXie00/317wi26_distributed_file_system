<script setup>
import { onBeforeUnmount, onMounted } from 'vue'

import ClusterPanel from './components/demo/ClusterPanel.vue'
import ElectionReplicationPanel from './components/demo/ElectionReplicationPanel.vue'
import EntryPanel from './components/demo/EntryPanel.vue'
import FeedbackLayer from './components/demo/FeedbackLayer.vue'
import FilePanel from './components/demo/FilePanel.vue'
import GlobalHeader from './components/demo/GlobalHeader.vue'
import KPIBar from './components/demo/KPIBar.vue'
import TimelinePanel from './components/demo/TimelinePanel.vue'
import { useDemoStateStore } from './stores/demoStateStore'
import { useEventStore } from './stores/eventStore'
import { useMetricStore } from './stores/metricStore'

// 顶部阶段标记：Commit 9 已接入时间线与事件筛选链路。
const phaseLabel = 'v0.2p00 · Commit 9 Timeline Events'
// 三类 store 统一由根组件启动轮询，避免子组件重复拉取。
const demoStateStore = useDemoStateStore()
const metricStore = useMetricStore()
const eventStore = useEventStore()

onMounted(() => {
  demoStateStore.startPolling()
  metricStore.startPolling()
  eventStore.startPolling()
})

onBeforeUnmount(() => {
  demoStateStore.stopPolling()
  metricStore.stopPolling()
  eventStore.stopPolling()
})
</script>

<template>
  <div class="demo-stage">
    <main class="demo-shell">
      <GlobalHeader :phase-label="phaseLabel" />
      <KPIBar />

      <section class="demo-grid">
        <ClusterPanel class="demo-span-8" />
        <EntryPanel class="demo-span-4" />
        <ElectionReplicationPanel class="demo-span-6" />
        <FilePanel class="demo-span-6" />
        <TimelinePanel class="demo-span-12" />
      </section>
    </main>

    <FeedbackLayer />
  </div>
</template>
