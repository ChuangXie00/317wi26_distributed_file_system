<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

function resolveReplicaDisplay(nodeInfo) {
  const value = nodeInfo?.replica_chunks ?? nodeInfo?.replicas
  if (value === null || value === undefined || value === '') {
    return '--'
  }
  return value
}

// storage 节点表：读取 membership 中 storage-* 节点。
const rows = computed(() => {
  const membership = demoStateStore.state.snapshot?.membership_view?.membership || {}
  const nodeIds = Object.keys(membership)
    .filter((nodeId) => nodeId.startsWith('storage-'))
    .sort()

  if (!nodeIds.length) {
    return [
      { id: 'storage-01', replicas: '--', status: '--' },
      { id: 'storage-02', replicas: '--', status: '--' },
      { id: 'storage-03', replicas: '--', status: '--' }
    ]
  }

  return nodeIds.map((nodeId) => {
    const nodeInfo = membership[nodeId] || {}
    return {
      id: nodeId,
      replicas: resolveReplicaDisplay(nodeInfo),
      status: nodeInfo.status || '--'
    }
  })
})

function statusClass(status) {
  if (status === 'alive') {
    return 'chip chip--ok'
  }
  if (status === 'suspected') {
    return 'chip chip--warn'
  }
  if (status === 'dead') {
    return 'chip chip--danger'
  }
  return 'chip'
}
</script>

<template>
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">Storage Table</h3>
      <span class="chip">{{ rows.length }} nodes</span>
    </header>
    <div class="panel__body">
      <table class="table">
        <thead>
          <tr>
            <th>Node</th>
            <th>Replicas</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in rows" :key="row.id">
            <td>{{ row.id }}</td>
            <td>{{ row.replicas }}</td>
            <td>
              <span :class="statusClass(row.status)">{{ row.status }}</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </article>
</template>
