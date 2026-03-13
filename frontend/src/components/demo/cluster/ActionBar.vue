<template>
  <article class="panel">
    <header class="panel__head">
      <h3 class="panel__title">Action Bar</h3>
      <span class="panel__meta">危险动作需二次确认</span>
    </header>
    <div class="panel__body subgrid">
      <div class="btn-row">
        <button type="button" class="btn" disabled>stop</button>
        <button type="button" class="btn" disabled>start</button>
        <button type="button" class="btn" disabled>reload</button>
      </div>
      <p class="empty-state">Commit 8 接通确认弹窗、回执 toast 与 action API。</p>
    </div>
  </article>
</template>
