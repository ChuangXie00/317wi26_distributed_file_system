<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

// meta 节点表：融合 membership 与 leader_view，保证 leader 标识可见。
const rows = computed(() => {
  const snapshot = demoStateStore.state.snapshot || {}
  const membership = snapshot?.membership_view?.membership || {}
  const observedLeader = snapshot?.leader_view?.leader || ''

  const idsFromMembership = Object.keys(membership).filter((nodeId) => nodeId.startsWith('meta-'))
  const idsFromLeaderView = Array.isArray(snapshot?.leader_view?.meta_cluster)
    ? snapshot.leader_view.meta_cluster.filter((nodeId) => String(nodeId).startsWith('meta-'))
    : []

  const nodeIds = [...new Set([...idsFromMembership, ...idsFromLeaderView])].sort()
  if (!nodeIds.length) {
    return [
      { id: 'meta-01', role: '--', status: '--' },
      { id: 'meta-02', role: '--', status: '--' },
      { id: 'meta-03', role: '--', status: '--' }
    ]
  }

  return nodeIds.map((nodeId) => {
    const status = membership?.[nodeId]?.status || '--'
    return {
      id: nodeId,
      role: nodeId === observedLeader ? 'leader' : 'follower',
      status
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
      <h3 class="panel__title">Meta Table</h3>
      <span class="chip">{{ rows.length }} nodes</span>
    </header>
    <div class="panel__body">
      <table class="table">
        <thead>
          <tr>
            <th>Node</th>
            <th>Role</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in rows" :key="row.id">
            <td>{{ row.id }}</td>
            <td>{{ row.role }}</td>
            <td>
              <span :class="statusClass(row.status)">{{ row.status }}</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </article>
</template>
