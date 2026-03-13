import { reactive } from 'vue'

const state = reactive({
  selectedFileName: '',
  selectionVersion: 0
})

export function useFilePanelStore() {
  return {
    state,
    setSelectedFile,
    clearSelection
  }
}

function setSelectedFile(fileName) {
  const next = String(fileName || '').trim()
  state.selectedFileName = next
  state.selectionVersion += 1
}

function clearSelection() {
  if (!state.selectedFileName) {
    return
  }
  state.selectedFileName = ''
  state.selectionVersion += 1
}
