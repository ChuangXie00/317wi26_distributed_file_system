<script setup>
import { computed, ref, watch } from 'vue'

import { fetchDemoFileReplicas } from '../../../api/demoApi.js'
import { useFilePanelStore } from '../../../stores/filePanelStore'

const filePanelStore = useFilePanelStore()

const isLoading = ref(false)
const rows = ref([])
const currentFileName = ref('')
const statusKind = ref('idle')
const statusMessage = ref('select a file on the left to load replica matrix.')

const selectedFileName = computed(() => filePanelStore.state.selectedFileName)

watch(
  () => filePanelStore.state.selectionVersion,
  async () => {
    await loadMatrixForSelection()
  },
  { immediate: true }
)

async function loadMatrixForSelection() {
  const targetName = String(selectedFileName.value || '').trim()
  if (!targetName) {
    rows.value = []
    currentFileName.value = ''
    statusKind.value = 'idle'
    statusMessage.value = 'select a file on the left to load replica matrix.'
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
    statusMessage.value = `chunk = ${result.chunk_count ?? rows.value.length}`
  } catch (error) {
    rows.value = []
    currentFileName.value = targetName
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
      <span class="panel__meta">{{ statusMessage }}</span>
    </header>
    <div class="panel__body subgrid">
      <div v-if="rows.length" class="replica__table-wrap">
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
.replica__table-wrap {
  height: 520px;
  overflow: auto;
}
</style>
