<script setup>
import { computed, ref, watch } from 'vue'

import { useActionStore } from '../../../stores/actionStore.js'
import { useDemoStateStore } from '../../../stores/demoStateStore.js'

const actionStore = useActionStore()
const demoStateStore = useDemoStateStore()

const DEFAULT_TARGETS = [
  'meta-01',
  'meta-02',
  'meta-03',
  'storage-01',
  'storage-02',
  'storage-03'
]

// 动作原因输入：默认 demo_drill，提交时会传给后端做审计记录。
const reason = ref('demo_drill')
const selectedTarget = ref(DEFAULT_TARGETS[0])

// 目标列表优先来自实时 membership，缺失时回退到默认白名单。
const targetOptions = computed(() => {
  const membership = demoStateStore.state.snapshot?.membership_view?.membership || {}
  const ids = Object.keys(membership).sort()
  if (!ids.length) {
    return DEFAULT_TARGETS
  }
  return ids
})

watch(
  targetOptions,
  (nextTargets) => {
    if (!nextTargets.includes(selectedTarget.value)) {
      selectedTarget.value = nextTargets[0]
    }
  },
  { immediate: true }
)

const actionDisabled = computed(() => actionStore.state.submitting || !selectedTarget.value)

function requestAction(action) {
  // 点击动作按钮时只打开确认弹窗，不直接执行。
  actionStore.openConfirm({
    action,
    target: selectedTarget.value,
    reason: reason.value
  })
}
</script>

<template>
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">Action Bar</h3>
      <span class="panel__meta"></span>
    </header>

    <div class="panel__body subgrid">
      <label class="field">
        <span class="field__label">Target</span>
        <select v-model="selectedTarget" class="field__control">
          <option v-for="target in targetOptions" :key="target" :value="target">
            {{ target }}
          </option>
        </select>
      </label>

      <label class="field">
        <span class="field__label">Reason</span>
        <input
          v-model.trim="reason"
          class="field__control"
          type="text"
          maxlength="128"
          placeholder="demo_drill"
        />
      </label>

      <div class="btn-row">
        <button type="button" class="btn" :disabled="actionDisabled" @click="requestAction('stop')">
          stop
        </button>
        <button type="button" class="btn" :disabled="actionDisabled" @click="requestAction('start')">
          start
        </button>
        <button type="button" class="btn" :disabled="actionDisabled" @click="requestAction('reload')">
          reload
        </button>
      </div>

      <p class="empty-state">
        {{ actionStore.state.submitting ? 'please waiting…' : '' }}
      </p>
    </div>
  </article>
</template>

<style scoped>
.field {
  display: grid;
  gap: 6px;
}

.field__label {
  font-size: 0.76rem;
  color: var(--ink-soft);
}

.field__control {
  width: 100%;
  border: 1px solid var(--line);
  border-radius: 10px;
  padding: 8px 10px;
  font-size: 0.84rem;
  color: var(--ink-0);
  background: rgba(255, 255, 255, 0.9);
}
</style>
