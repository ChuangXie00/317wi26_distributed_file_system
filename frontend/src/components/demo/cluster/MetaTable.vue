<script setup>
// 占位数据：Commit 6 仅用于保证表格骨架完整可见。
const rows = [
  { id: 'meta-01', role: 'leader?', status: '--' },
  { id: 'meta-02', role: 'follower?', status: '--' },
  { id: 'meta-03', role: 'follower?', status: '--' }
]
</script>

<template>
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">Meta Table</h3>
      <span class="chip">3 nodes</span>
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
            <td>{{ row.status }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </article>
</template>
