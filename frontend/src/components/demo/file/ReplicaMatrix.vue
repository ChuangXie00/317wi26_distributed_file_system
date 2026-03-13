<script setup>
import { computed, ref } from 'vue'

import { fetchDemoFileReplicas } from '../../../api/demoApi.js'

const fileNameInput = ref('')
const isLoading = ref(false)
const rows = ref([])
const currentFileName = ref('')
const statusKind = ref('idle')
const statusMessage = ref('输入文件名后点击 load matrix。')

const canLoad = computed(() => Boolean(fileNameInput.value.trim()) && !isLoading.value)

async function onLoadMatrix() {
  const targetName = fileNameInput.value.trim()
  if (!targetName) {
    return
  }

  isLoading.value = true
  statusKind.value = 'loading'
  statusMessage.value = `loading replica matrix for ${targetName} ...`

  try {
    const result = await fetchDemoFileReplicas({ fileName: targetName })
    currentFileName.value = result.file_name || targetName
    rows.value = Array.isArray(result.rows) ? result.rows : []
    statusKind.value = 'ok'
    statusMessage.value = `matrix loaded: chunks=${result.chunk_count ?? rows.value.length}`
  } catch (error) {
    rows.value = []
    currentFileName.value = ''
    statusKind.value = 'error'
    statusMessage.value = `load failed: ${error?.code || ''} ${error?.message || 'unknown error'}`
  } finally {
    isLoading.value = false
  }
}

function shortFingerprint(value) {
  const raw = String(value || '')
  if (raw.length <= 14) {
    return raw || '--'
  }
  return `${raw.slice(0, 6)}...${raw.slice(-6)}`
}
</script>

<template>
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">Replica Matrix</h3>
      <span class="panel__meta">Commit 12: live chunk replicas</span>
    </header>
    <div class="panel__body subgrid">
      <label class="replica__label">
        File Name
        <input v-model="fileNameInput" class="replica__input" type="text" placeholder="commit11_smoke.txt" />
      </label>

      <div class="btn-row">
        <button type="button" class="btn" :disabled="!canLoad" @click="onLoadMatrix">
          {{ isLoading ? 'loading...' : 'load matrix' }}
        </button>
      </div>

      <p class="empty-state" :class="{ 'replica__status--error': statusKind === 'error' }">{{ statusMessage }}</p>

      <div v-if="rows.length" class="replica__table-wrap">
        <p class="panel__meta">file={{ currentFileName }}</p>
        <table class="table">
          <thead>
            <tr>
              <th>#</th>
              <th>Fingerprint</th>
              <th>Replicas</th>
              <th>Count</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in rows" :key="`${row.chunk_index}-${row.fingerprint}`">
              <td>{{ row.chunk_index }}</td>
              <td :title="row.fingerprint">{{ shortFingerprint(row.fingerprint) }}</td>
              <td>{{ Array.isArray(row.locations) && row.locations.length ? row.locations.join(', ') : '--' }}</td>
              <td>{{ row.replica_count ?? 0 }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </article>
</template>

<style scoped>
.replica__label {
  display: grid;
  gap: 6px;
  font-size: 0.78rem;
  color: var(--ink-soft);
}

.replica__input {
  width: 100%;
  border: 1px solid var(--line);
  border-radius: 10px;
  padding: 7px 10px;
  background: rgba(255, 255, 255, 0.9);
  color: var(--ink-0);
  font-size: 0.82rem;
}

.replica__status--error {
  border-color: rgba(196, 63, 84, 0.3);
  background: rgba(196, 63, 84, 0.08);
}

.replica__table-wrap {
  overflow-x: auto;
}
</style>
