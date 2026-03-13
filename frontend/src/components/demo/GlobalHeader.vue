<script setup>
import ClockAndDrillState from './header/ClockAndDrillState.vue'
import PollHealthBadge from './header/PollHealthBadge.vue'
import TitleMode from './header/TitleMode.vue'

defineProps({
  phaseLabel: {
    type: String,
    default: ''
  }
})
</script>

<template>
  <header class="panel reveal" style="--delay: 40ms">
    <div class="panel__body header-layout">
      <TitleMode :phase-label="phaseLabel" />
      <div class="header-side">
        <PollHealthBadge />
        <ClockAndDrillState />
      </div>
    </div>
  </header>
</template>

<style scoped>
.header-layout {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}

.header-side {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  justify-content: flex-end;
}

@media (max-width: 900px) {
  .header-layout {
    flex-direction: column;
    align-items: flex-start;
  }

  .header-side {
    justify-content: flex-start;
  }
}
</style>
