<script setup>
import { computed } from 'vue'

import { useDemoStateStore } from '../../../stores/demoStateStore'

const demoStateStore = useDemoStateStore()

// meta 节点表：优先使用 membership 的角色视图，observed leader 仅作高优先级覆盖。
const rows = computed(() => {
  const snapshot = demoStateStore.state.snapshot || {}
  const membership = snapshot?.membership_view?.membership || {}
  const observedLeader = snapshot?.leader_view?.leader || ''

  const idsFromMembership = Object.keys(membership).filter((nodeId) => nodeId.startsWith('meta-'))
  const idsFromLeaderView = Array.isArray(snapshot?.leader_view?.meta_cluster)
    ? snapshot.leader_view.meta_cluster
        .map((item) => (typeof item === 'string' ? item : item?.node_id))
        .filter((nodeId) => String(nodeId || '').startsWith('meta-'))
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
    const nodeInfo = membership?.[nodeId] || {}
    const status = nodeInfo?.status || '--'
    const membershipRole = String(nodeInfo?.role || '')
      .trim()
      .toLowerCase()
    const roleFromMembership = ['leader', 'follower', 'candidate', 'unknown'].includes(membershipRole)
      ? membershipRole
      : 'follower'

    // dead 节点不展示旧角色，避免出现“dead 但 leader/follower”的误导。
    let role = status === 'dead' ? 'unknown' : roleFromMembership
    if (nodeId === observedLeader) {
      role = 'leader'
    }

    return {
      id: nodeId,
      role,
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
