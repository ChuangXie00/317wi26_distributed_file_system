<script setup>
import FileOps from './file/FileOps.vue'
import ReplicaMatrix from './file/ReplicaMatrix.vue'
</script>

<template>
  <section class="panel reveal" style="--delay: 220ms">
    <header class="panel__head">
      <h2 class="panel__title">File Panel</h2>
      <span class="panel__meta"></span>
    </header>
    <div class="panel__body subgrid subgrid--2">
      <FileOps />
      <ReplicaMatrix />
    </div>
  </section>
</template>
