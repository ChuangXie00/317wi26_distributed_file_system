<script setup>
import { computed, onMounted, ref } from 'vue'

import { deleteDemoFile, fetchDemoFileDownload, fetchDemoFileList, postDemoFileUpload } from '../../../api/demoApi.js'
import { useFilePanelStore } from '../../../stores/filePanelStore'

const filePanelStore = useFilePanelStore()

const hiddenFileInputRef = ref(null)
const selectedLocalFile = ref(null)
const files = ref([])
const sortBy = ref('time_desc')
const isRefreshing = ref(false)
const isUploading = ref(false)
const isDownloading = ref(false)
const isDeleting = ref(false)
const showDeleteConfirm = ref(false)
const statusKind = ref('idle')
const statusMessage = ref('请选择文件后可下载或删除。')

const selectedFileName = computed(() => filePanelStore.state.selectedFileName)
const selectedItem = computed(() => files.value.find((item) => item.file_name === selectedFileName.value) || null)

const canDownload = computed(() => Boolean(selectedFileName.value) && !isUploading.value && !isDownloading.value && !isDeleting.value)
const canDelete = computed(() => Boolean(selectedFileName.value) && !isUploading.value && !isDownloading.value && !isDeleting.value)

const sortedFiles = computed(() => {
  const copied = [...files.value]
  if (sortBy.value === 'name_asc') {
    return copied.sort((a, b) => String(a.file_name).localeCompare(String(b.file_name)))
  }
  if (sortBy.value === 'name_desc') {
    return copied.sort((a, b) => String(b.file_name).localeCompare(String(a.file_name)))
  }
  if (sortBy.value === 'time_asc') {
    return copied.sort((a, b) => toTs(a.updated_at) - toTs(b.updated_at))
  }
  return copied.sort((a, b) => toTs(b.updated_at) - toTs(a.updated_at))
})

const statusClass = computed(() => {
  if (statusKind.value === 'ok') {
    return 'fileops__status--ok'
  }
  if (statusKind.value === 'error') {
    return 'fileops__status--error'
  }
  return ''
})

onMounted(async () => {
  await refreshFiles()
})

async function refreshFiles() {
  isRefreshing.value = true
  try {
    const result = await fetchDemoFileList({ limit: 500 })
    files.value = Array.isArray(result.files) ? result.files : []
    if (selectedFileName.value && !files.value.some((item) => item.file_name === selectedFileName.value)) {
      filePanelStore.clearSelection()
    }
  } catch (error) {
    statusKind.value = 'error'
    statusMessage.value = `文件列表加载失败：${error?.code || ''} ${error?.message || 'unknown error'}`
  } finally {
    isRefreshing.value = false
  }
}

function onClickAdd() {
  hiddenFileInputRef.value?.click()
}

async function onPickFile(event) {
  const file = event?.target?.files?.[0] || null
  selectedLocalFile.value = file
  if (!selectedLocalFile.value) {
    return
  }
  await uploadSelectedFile()
  if (hiddenFileInputRef.value) {
    hiddenFileInputRef.value.value = ''
  }
}

async function uploadSelectedFile() {
  if (!selectedLocalFile.value) {
    return
  }

  isUploading.value = true
  statusKind.value = 'loading'
  statusMessage.value = `正在上传 ${selectedLocalFile.value.name} ...`

  try {
    const result = await postDemoFileUpload(selectedLocalFile.value, { fileName: selectedLocalFile.value.name })
    await refreshFiles()
    filePanelStore.setSelectedFile(result.file_name || selectedLocalFile.value.name)
    statusKind.value = 'ok'
    statusMessage.value = `上传成功：${result.file_name}，chunks=${result.chunk_count}，bytes=${result.total_bytes}`
  } catch (error) {
    statusKind.value = 'error'
    statusMessage.value = `上传失败：${error?.code || ''} ${error?.message || 'unknown error'}`
  } finally {
    isUploading.value = false
    selectedLocalFile.value = null
  }
}

async function onDownload() {
  if (!selectedFileName.value) {
    return
  }
  isDownloading.value = true
  statusKind.value = 'loading'
  statusMessage.value = `正在下载 ${selectedFileName.value} ...`
  try {
    const result = await fetchDemoFileDownload({ fileName: selectedFileName.value })
    const bytes = base64ToBytes(result.content_base64 || '')
    await saveBytesWithPicker(bytes, result.file_name || selectedFileName.value)
    statusKind.value = 'ok'
    statusMessage.value = `下载成功：${result.file_name || selectedFileName.value}`
  } catch (error) {
    statusKind.value = 'error'
    statusMessage.value = `下载失败：${error?.code || ''} ${error?.message || 'unknown error'}`
  } finally {
    isDownloading.value = false
  }
}

function openDeleteConfirm() {
  if (!canDelete.value) {
    return
  }
  showDeleteConfirm.value = true
}

function closeDeleteConfirm() {
  if (isDeleting.value) {
    return
  }
  showDeleteConfirm.value = false
}

async function confirmDelete() {
  if (!selectedFileName.value) {
    return
  }
  isDeleting.value = true
  statusKind.value = 'loading'
  statusMessage.value = `正在删除 ${selectedFileName.value} ...`
  try {
    const targetName = selectedFileName.value
    await deleteDemoFile({ fileName: targetName })
    showDeleteConfirm.value = false
    await refreshFiles()
    filePanelStore.clearSelection()
    statusKind.value = 'ok'
    statusMessage.value = `删除成功：${targetName}`
  } catch (error) {
    statusKind.value = 'error'
    statusMessage.value = `删除失败：${error?.code || ''} ${error?.message || 'unknown error'}`
  } finally {
    isDeleting.value = false
  }
}

function onSelectFile(fileName) {
  filePanelStore.setSelectedFile(fileName)
  statusKind.value = 'idle'
  statusMessage.value = `已选中：${fileName}`
}

function isSelected(fileName) {
  return selectedFileName.value === fileName
}

function formatTime(raw) {
  const value = String(raw || '').trim()
  if (!value) {
    return '--'
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  return date.toLocaleString()
}

function toTs(raw) {
  const date = new Date(String(raw || '').trim())
  return Number.isNaN(date.getTime()) ? 0 : date.getTime()
}

function base64ToBytes(encoded) {
  const binary = atob(String(encoded || ''))
  const out = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i += 1) {
    out[i] = binary.charCodeAt(i)
  }
  return out
}

async function saveBytesWithPicker(bytes, fileName) {
  // Chrome/Edge: 使用系统保存对话框；其他浏览器回退到常规下载流程。
  if (window.showSaveFilePicker) {
    const handle = await window.showSaveFilePicker({
      suggestedName: fileName,
      types: [
        {
          description: 'Text or binary file',
          accept: {
            'application/octet-stream': ['.txt', '.bin', '.log', '.json']
          }
        }
      ]
    })
    const writable = await handle.createWritable()
    await writable.write(bytes)
    await writable.close()
    return
  }

  const blob = new Blob([bytes], { type: 'application/octet-stream' })
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = fileName
  document.body.appendChild(anchor)
  anchor.click()
  anchor.remove()
  URL.revokeObjectURL(url)
}
</script>

<template>
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">File</h3>
      <span class="panel__meta">manage files and selection</span>
    </header>

    <div class="panel__body subgrid">
      <input ref="hiddenFileInputRef" class="fileops__hidden-input" type="file" @change="onPickFile" />

      <div class="fileops__toolbar">
        <div class="btn-row">
          <button type="button" class="btn" :disabled="isUploading || isDownloading || isDeleting" @click="onClickAdd">
            {{ isUploading ? 'adding...' : 'add' }}
          </button>
          <button type="button" class="btn" :disabled="!canDownload" @click="onDownload">
            {{ isDownloading ? 'downloading...' : 'download' }}
          </button>
        </div>
        <button type="button" class="btn" :disabled="!canDelete" @click="openDeleteConfirm">
          {{ isDeleting ? 'deleting...' : 'delete' }}
        </button>
      </div>

      <div class="fileops__list-head">
        <p class="panel__meta">Stored Files ({{ files.length }})</p>
        <label class="fileops__sort">
          sort
          <select v-model="sortBy" class="fileops__select">
            <option value="time_desc">time desc</option>
            <option value="time_asc">time asc</option>
            <option value="name_asc">name asc</option>
            <option value="name_desc">name desc</option>
          </select>
        </label>
      </div>

      <div class="fileops__list-wrap">
        <table class="table fileops__table">
          <thead>
            <tr>
              <th>File</th>
              <th>Chunks</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="!sortedFiles.length">
              <td colspan="3" class="fileops__empty">{{ isRefreshing ? 'loading files...' : 'no files yet' }}</td>
            </tr>
            <tr
              v-for="item in sortedFiles"
              :key="item.file_name"
              class="fileops__row"
              :class="{ 'fileops__row--active': isSelected(item.file_name) }"
              @click="onSelectFile(item.file_name)"
            >
              <td>{{ item.file_name }}</td>
              <td>{{ item.chunk_count ?? 0 }}</td>
              <td>{{ formatTime(item.updated_at) }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <p class="empty-state" :class="statusClass">{{ statusMessage }}</p>
    </div>

    <div v-if="showDeleteConfirm" class="fileops__modal-backdrop" @click="closeDeleteConfirm">
      <article class="panel fileops__modal" @click.stop>
        <header class="panel__head">
          <h4 class="panel__title">Confirm Delete</h4>
          <span class="chip chip--danger">high risk</span>
        </header>
        <div class="panel__body subgrid">
          <p class="empty-state">delete file `{{ selectedFileName || '--' }}` ? this action cannot be undone.</p>
          <div class="btn-row">
            <button type="button" class="btn" :disabled="isDeleting" @click="closeDeleteConfirm">cancel</button>
            <button type="button" class="btn" :disabled="isDeleting" @click="confirmDelete">
              {{ isDeleting ? 'deleting...' : 'confirm delete' }}
            </button>
          </div>
        </div>
      </article>
    </div>
  </article>
</template>

<style scoped>
.fileops__hidden-input {
  display: none;
}

.fileops__toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.fileops__list-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.fileops__sort {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 0.76rem;
  color: var(--ink-soft);
}

.fileops__select {
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 5px 8px;
  background: rgba(255, 255, 255, 0.92);
  color: var(--ink-1);
  font-size: 0.78rem;
}

.fileops__list-wrap {
  max-height: 280px;
  overflow: auto;
  border: 1px solid rgba(200, 213, 234, 0.78);
  border-radius: 12px;
}

.fileops__table tbody tr {
  cursor: pointer;
}

.fileops__row--active {
  background: rgba(30, 106, 214, 0.12);
}

.fileops__empty {
  text-align: center;
  color: var(--ink-soft);
}

.fileops__modal-backdrop {
  position: fixed;
  inset: 0;
  z-index: 20;
  display: grid;
  place-items: center;
  background: rgba(15, 26, 43, 0.28);
  backdrop-filter: blur(2px);
}

.fileops__modal {
  width: min(480px, calc(100vw - 24px));
}
</style>
