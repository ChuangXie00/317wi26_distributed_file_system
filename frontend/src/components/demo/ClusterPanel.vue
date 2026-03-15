<script setup>
import ActionBar from './cluster/ActionBar.vue'
import MetaTable from './cluster/MetaTable.vue'
import StorageTable from './cluster/StorageTable.vue'
</script>

<template>
  <section class="panel reveal" style="--delay: 130ms">
    <header class="panel__head">
      <h2 class="panel__title">Cluster Panel</h2>
    </header>

    <div class="panel__body subgrid subgrid--2">
      <StorageTable />
      <MetaTable />
      <ActionBar class="cluster-action-bar" />
    </div>
  </section>
</template>

<style scoped>
.cluster-action-bar {
  grid-column: 1 / -1;
}
</style>
