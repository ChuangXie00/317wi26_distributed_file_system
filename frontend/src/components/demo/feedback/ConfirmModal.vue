<template>
  <article class="feedback-card">
    <header class="feedback-card__head">ConfirmModal 预览</header>
    <div class="feedback-card__body">
      <p>目标节点：meta-01</p>
      <p>动作：stop（高风险）</p>
      <p class="feedback-hint">Commit 8 接入真实确认流程与阻断入口。</p>
    </div>
  </article>
</template>

<style scoped>
.feedback-card {
  border: 1px solid rgba(59, 87, 129, 0.28);
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.9);
  box-shadow: 0 8px 22px rgba(17, 33, 59, 0.16);
  overflow: clip;
}

.feedback-card__head {
  font-size: 0.78rem;
  font-weight: 700;
  color: var(--ink-soft);
  border-bottom: 1px solid rgba(200, 213, 234, 0.8);
  padding: 8px 10px;
}

.feedback-card__body {
  display: grid;
  gap: 3px;
  padding: 9px 10px 10px;
  font-size: 0.8rem;
}

.feedback-hint {
  color: var(--ink-soft);
}
</style>
