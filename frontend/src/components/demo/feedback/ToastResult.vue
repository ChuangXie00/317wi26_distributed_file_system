<template>
  <article class="toast-card">
    <p class="toast-card__title">ToastResult 预览</p>
    <p class="toast-card__body">动作回执（成功 / 失败）将在这里短暂展示。</p>
  </article>
</template>

<style scoped>
.toast-card {
  border: 1px solid rgba(31, 143, 103, 0.34);
  border-radius: 12px;
  background: rgba(31, 143, 103, 0.12);
  color: #124d37;
  padding: 10px;
  animation: settle 0.35s ease-out both;
}

.toast-card__title {
  font-size: 0.82rem;
  font-weight: 700;
}

.toast-card__body {
  margin-top: 2px;
  font-size: 0.78rem;
}

@keyframes settle {
  from {
    transform: translateY(8px);
    opacity: 0;
  }

  to {
    transform: translateY(0);
    opacity: 1;
  }
}
</style>
