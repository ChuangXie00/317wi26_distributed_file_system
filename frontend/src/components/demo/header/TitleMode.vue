<script setup>
defineProps({
  phaseLabel: {
    type: String,
    default: ''
  }
})
</script>

<template>
  <div class="title-wrap">
    <p class="eyebrow">DFS Demo Console</p>
    <h1 class="title">Single Page Drill Console</h1>
    <p class="phase">{{ phaseLabel }}</p>
  </div>
</template>

<style scoped>
.title-wrap {
  display: grid;
  gap: 2px;
}

.eyebrow {
  font-size: 0.76rem;
  color: var(--ink-soft);
  letter-spacing: 0.18em;
  text-transform: uppercase;
}

.title {
  font-size: clamp(1.3rem, 2.4vw, 2rem);
  line-height: 1.2;
}

.phase {
  font-size: 0.84rem;
  color: var(--accent);
  font-family: var(--font-mono);
}
</style>
