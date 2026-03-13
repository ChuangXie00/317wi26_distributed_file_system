<script setup>
import ConfirmModal from './feedback/ConfirmModal.vue'
import ToastResult from './feedback/ToastResult.vue'
</script>

<template>
  <aside class="feedback-layer">
    <ConfirmModal />
    <ToastResult />
  </aside>
</template>

<style scoped>
.feedback-layer {
  position: fixed;
  right: 14px;
  bottom: 14px;
  z-index: 20;
  width: min(360px, calc(100vw - 28px));
  display: grid;
  gap: 10px;
  pointer-events: none;
}

.feedback-layer > * {
  pointer-events: auto;
}

@media (max-width: 720px) {
  .feedback-layer {
    right: 10px;
    bottom: 10px;
    width: calc(100vw - 20px);
  }
}
</style>
