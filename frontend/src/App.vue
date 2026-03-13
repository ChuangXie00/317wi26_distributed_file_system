<script setup>
import ClusterPanel from './components/demo/ClusterPanel.vue'
import ElectionReplicationPanel from './components/demo/ElectionReplicationPanel.vue'
import EntryPanel from './components/demo/EntryPanel.vue'
import FeedbackLayer from './components/demo/FeedbackLayer.vue'
import FilePanel from './components/demo/FilePanel.vue'
import GlobalHeader from './components/demo/GlobalHeader.vue'
import KPIBar from './components/demo/KPIBar.vue'
import TimelinePanel from './components/demo/TimelinePanel.vue'

// 顶部阶段标记：用于明确当前页面处于 v0.2p00 的骨架阶段。
const phaseLabel = 'v0.2p00 · Commit 6 Skeleton'
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
