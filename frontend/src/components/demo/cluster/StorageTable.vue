<script setup>
// 占位数据：Commit 6 仅用于保证表格骨架完整可见。
const rows = [
  { id: 'storage-01', replicas: '--', status: '--' },
  { id: 'storage-02', replicas: '--', status: '--' },
  { id: 'storage-03', replicas: '--', status: '--' }
]
</script>

<template>
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">Storage Table</h3>
      <span class="chip">3 nodes</span>
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
            <td>{{ row.status }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </article>
</template>
