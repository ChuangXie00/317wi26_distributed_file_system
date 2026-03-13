<script setup>
import { computed } from 'vue'

import { useActionStore } from '../../../stores/actionStore.js'

const actionStore = useActionStore()

const modalState = computed(() => actionStore.state.confirmModal)
const isVisible = computed(() => modalState.value.visible)
const isSubmitting = computed(() => actionStore.state.submitting)

async function submitConfirmed() {
  await actionStore.confirmAndExecute()
}
</script>

<template>
  <article v-if="isVisible" class="feedback-card">
    <header class="feedback-card__head">High Risk Action</header>
    <div class="feedback-card__body">
      <p>Target：{{ modalState.target || '--' }}</p>
      <p>Action：{{ modalState.action || '--' }}</p>
      <p>Reason：{{ modalState.reason || '--' }}</p>
      <p class="feedback-hint">This operation may trigger the switch and unavailability. Please confirm.</p>

      <div class="btn-row">
        <button type="button" class="btn" :disabled="isSubmitting" @click="actionStore.closeConfirm()">
          Cancel
        </button>
        <button type="button" class="btn btn--danger" :disabled="isSubmitting" @click="submitConfirmed">
          {{ isSubmitting ? 'processing...' : 'Do it' }}
        </button>
      </div>
    </div>
  </article>
</template>

<style scoped>
.feedback-card {
  border: 1px solid rgba(145, 67, 44, 0.28);
  border-radius: 12px;
  background: rgba(255, 251, 249, 0.96);
  box-shadow: 0 8px 22px rgba(17, 33, 59, 0.16);
  overflow: clip;
}

.feedback-card__head {
  font-size: 0.78rem;
  font-weight: 700;
  color: #6a2d1f;
  border-bottom: 1px solid rgba(202, 171, 159, 0.65);
  padding: 8px 10px;
}

.feedback-card__body {
  display: grid;
  gap: 4px;
  padding: 10px;
  font-size: 0.8rem;
}

.feedback-hint {
  color: #735247;
}

.btn--danger {
  border-color: rgba(196, 63, 84, 0.4);
  color: #9f2038;
  background: rgba(196, 63, 84, 0.1);
}
</style>
