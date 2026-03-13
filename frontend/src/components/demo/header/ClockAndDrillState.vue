<script setup>
// 初始化时刻：Commit 6 阶段先展示静态时间戳占位。
const nowText = new Date().toLocaleTimeString('zh-CN', { hour12: false })
</script>

<template>
  <div class="chip">
    <span>演练状态：Idle</span>
    <span class="clock">{{ nowText }}</span>
  </div>
</template>

<style scoped>
.clock {
  font-family: var(--font-mono);
}
</style>
