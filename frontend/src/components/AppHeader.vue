<script setup>
import { computed } from 'vue'
import { useTheme } from '../composables/useTheme'
import { useWorkbench } from '../composables/useWorkbench'

defineProps({ datasetCount: Number, servingCount: Number })
const emit = defineEmits(['open-datasets', 'open-serving', 'open-settings'])
const { backendMode, loadingRuns, refreshRuns, settings, guard } = useWorkbench()

/* The badge says which layer is judging intent, so a fallback to the
   keyword rules is visible rather than silent. */
const routing = computed(() => settings.value?.routing || null)
const { theme, toggle } = useTheme()
</script>

<template>
  <header class="topbar">
    <div class="brand">
      <span class="brand-mark">DF</span>
      <span class="brand-text">
        <strong>DataFlow</strong>
        <small>Multi-Agent Workbench</small>
      </span>
    </div>

    <div class="actions">
      <span class="tag" :class="backendMode === 'codex' ? 'brand' : 'info'" :title="`当前编排后端：${backendMode}`">
        <span class="dot" />{{ backendMode }} backend
      </span>
      <button class="btn small" @click="emit('open-datasets')">数据集 · {{ datasetCount }}</button>
      <button class="btn small" @click="emit('open-serving')">Serving / API · {{ servingCount }}</button>
      <button class="btn small" :class="{ warn: routing && routing.effective === 'rules' }"
              :title="routing ? `意图路由：${routing.effective === 'jev' ? 'Jev 决策模型' : '关键字规则（未启用或缺少 key）'}` : '意图路由设置'"
              @click="emit('open-settings')">
        路由 · {{ routing ? (routing.effective === 'jev' ? 'Jev' : '规则') : '…' }}
      </button>
      <button class="btn small icon" :disabled="loadingRuns" title="刷新运行列表" @click="guard(refreshRuns)">
        <svg viewBox="0 0 16 16" width="13" height="13" aria-hidden="true" :class="{ spin: loadingRuns }">
          <path fill="currentColor" d="M8 3V1L5 4l3 3V5a3 3 0 1 1-3 3H3.5A4.5 4.5 0 1 0 8 3Z" />
        </svg>
      </button>
      <button class="btn small icon" :title="theme === 'dark' ? '切换到浅色' : '切换到深色'" @click="toggle">
        <svg v-if="theme === 'dark'" viewBox="0 0 16 16" width="13" height="13" aria-hidden="true">
          <circle cx="8" cy="8" r="3.2" fill="currentColor" />
          <g stroke="currentColor" stroke-width="1.3" stroke-linecap="round">
            <path d="M8 1v1.6M8 13.4V15M1 8h1.6M13.4 8H15M3 3l1.1 1.1M11.9 11.9 13 13M13 3l-1.1 1.1M4.1 11.9 3 13" />
          </g>
        </svg>
        <svg v-else viewBox="0 0 16 16" width="13" height="13" aria-hidden="true">
          <path fill="currentColor" d="M13.2 10.2A5.6 5.6 0 0 1 5.8 2.8a5.6 5.6 0 1 0 7.4 7.4Z" />
        </svg>
      </button>
    </div>
  </header>
</template>

<style scoped>
.topbar {
  height: var(--header-height);
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 0 18px;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  position: relative;
  z-index: 5;
}
.brand { display: flex; align-items: center; gap: 10px; }
.brand-mark {
  width: 30px;
  height: 30px;
  border-radius: 9px;
  display: grid;
  place-items: center;
  font-size: 11px;
  font-weight: 800;
  color: #fff;
  background: var(--brand-gradient);
  box-shadow: 0 2px 10px -3px var(--brand);
}
.brand-text { display: flex; flex-direction: column; line-height: 1.2; }
.brand-text strong { font-size: 13.5px; }
.brand-text small { font-size: 10.5px; color: var(--text-3); }
.actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
.btn.warn { color: var(--warn); border-color: color-mix(in srgb, var(--warn) 40%, var(--border)); }
.spin { animation: spin 0.9s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }

@media (max-width: 720px) {
  .brand-text small { display: none; }
}
</style>
