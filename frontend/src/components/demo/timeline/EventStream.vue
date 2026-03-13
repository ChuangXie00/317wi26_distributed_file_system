<script setup>
// 空态时间线：Commit 6 保证列表区域稳定，Commit 9 再接 events 增量流。
const placeholders = [
  { id: 1, title: '等待事件流接入', detail: 'action.accepted / leader.switched 等将在这里按序展示。' },
  { id: 2, title: '筛选器待激活', detail: '支持 meta/storage/file/error/action 分类筛选。' }
]
</script>

<template>
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">Event Stream</h3>
      <span class="chip">since_seq: 0</span>
    </header>
    <div class="panel__body subgrid">
      <article v-for="item in placeholders" :key="item.id" class="empty-state">
        <p><strong>{{ item.title }}</strong></p>
        <p>{{ item.detail }}</p>
      </article>
    </div>
  </article>
</template>
