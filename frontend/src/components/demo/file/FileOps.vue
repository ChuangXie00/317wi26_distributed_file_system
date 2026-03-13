<script setup>
import { computed, ref } from 'vue'

import { fetchDemoFileDownload, postDemoFileUpload } from '../../../api/demoApi.js'

const selectedFile = ref(null)
const fileNameInput = ref('')
const downloadTarget = ref('')
const isUploading = ref(false)
const isDownloading = ref(false)
const statusKind = ref('idle')
const statusMessage = ref('请选择一个本地文件后点击 upload。')

const canUpload = computed(() => Boolean(selectedFile.value) && !isUploading.value && !isDownloading.value)
const canDownload = computed(() => {
  return Boolean(String(downloadTarget.value || fileNameInput.value || '').trim()) && !isUploading.value && !isDownloading.value
})
const statusKindClass = computed(() => {
  if (statusKind.value === 'ok') {
    return 'fileops__status--ok'
  }
  if (statusKind.value === 'error') {
    return 'fileops__status--error'
  }
  return ''
})

function onPickFile(event) {
  const files = event?.target?.files
  selectedFile.value = files && files.length ? files[0] : null
  if (selectedFile.value && !fileNameInput.value.trim()) {
    fileNameInput.value = selectedFile.value.name
  }
}

async function onUpload() {
  if (!selectedFile.value) {
    return
  }

  isUploading.value = true
  statusKind.value = 'loading'
  statusMessage.value = '正在上传并提交文件，请稍候...'

  try {
    const result = await postDemoFileUpload(selectedFile.value, {
      fileName: fileNameInput.value.trim() || selectedFile.value.name
    })
    statusKind.value = 'ok'
    statusMessage.value = `upload 成功：${result.file_name}，chunks=${result.chunk_count}，bytes=${result.total_bytes}`
    downloadTarget.value = result.file_name
  } catch (error) {
    statusKind.value = 'error'
    statusMessage.value = `upload 失败：${error?.code || ''} ${error?.message || 'unknown error'}`
  } finally {
    isUploading.value = false
  }
}

async function onDownload() {
  const targetName = String(downloadTarget.value || fileNameInput.value || '').trim()
  if (!targetName) {
    return
  }

  isDownloading.value = true
  statusKind.value = 'loading'
  statusMessage.value = `正在下载 ${targetName} ...`

  try {
    const result = await fetchDemoFileDownload({ fileName: targetName })
    const bytes = base64ToBytes(result.content_base64 || '')
    triggerBrowserDownload(bytes, result.file_name || targetName)
    statusKind.value = 'ok'
    statusMessage.value = `download 成功：${result.file_name || targetName}，bytes=${result.total_bytes}`
  } catch (error) {
    statusKind.value = 'error'
    statusMessage.value = `download 失败：${error?.code || ''} ${error?.message || 'unknown error'}`
  } finally {
    isDownloading.value = false
  }
}

function base64ToBytes(encoded) {
  // base64 -> Uint8Array：与后端 /file/download 的返回结构对应。
  const binary = atob(String(encoded || ''))
  const out = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i += 1) {
    out[i] = binary.charCodeAt(i)
  }
  return out
}

function triggerBrowserDownload(bytes, fileName) {
  // 浏览器侧落盘：生成临时 Blob URL 后触发 a 标签下载。
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
      <h3 class="panel__title">File Ops</h3>
      <span class="panel__meta">Commit 11: upload / download 最小可用</span>
    </header>
    <div class="panel__body subgrid">
      <label class="fileops__label">
        Local File
        <input class="fileops__input" type="file" @change="onPickFile" />
      </label>

      <label class="fileops__label">
        File Name
        <input v-model="fileNameInput" class="fileops__input" type="text" placeholder="demo.txt" />
      </label>

      <label class="fileops__label">
        Download Target
        <input v-model="downloadTarget" class="fileops__input" type="text" placeholder="name to download" />
      </label>

      <div class="btn-row">
        <button type="button" class="btn" :disabled="!canUpload" @click="onUpload">
          {{ isUploading ? 'uploading...' : 'upload' }}
        </button>
        <button type="button" class="btn" :disabled="!canDownload" @click="onDownload">
          {{ isDownloading ? 'downloading...' : 'download' }}
        </button>
        <button type="button" class="btn" disabled>delete</button>
      </div>

      <p class="empty-state" :class="statusKindClass">{{ statusMessage }}</p>
    </div>
  </article>
</template>

<style scoped>
.fileops__label {
  display: grid;
  gap: 6px;
  font-size: 0.78rem;
  color: var(--ink-soft);
}

.fileops__input {
  width: 100%;
  border: 1px solid var(--line);
  border-radius: 10px;
  padding: 7px 10px;
  background: rgba(255, 255, 255, 0.9);
  color: var(--ink-0);
  font-size: 0.82rem;
}

.fileops__status--ok {
  border-color: rgba(31, 143, 103, 0.28);
  background: rgba(31, 143, 103, 0.08);
}

.fileops__status--error {
  border-color: rgba(196, 63, 84, 0.3);
  background: rgba(196, 63, 84, 0.08);
}
</style>
