<script setup>
import EntryTargetCard from './entry/EntryTargetCard.vue'
</script>

<template>
  <section class="panel reveal" style="--delay: 160ms">
    <header class="panel__head">
      <h2 class="panel__title">Entry Panel</h2>
      <span class="panel__meta"></span>
    </header>
    <div class="panel__body">
      <EntryTargetCard />
    </div>
  </section>
</template>
