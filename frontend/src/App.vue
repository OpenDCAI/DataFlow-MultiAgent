<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue'
import AppHeader from './components/AppHeader.vue'
import RunSidebar from './components/RunSidebar.vue'
import ChatPanel from './components/ChatPanel.vue'
import PipelineView from './components/PipelineView.vue'
import StageOutput from './components/StageOutput.vue'
import AgentRail from './components/AgentRail.vue'
import CodeDialog from './components/CodeDialog.vue'
import ServingDialog from './components/ServingDialog.vue'
import DatasetDialog from './components/DatasetDialog.vue'
import SettingsDialog from './components/SettingsDialog.vue'
import ToastHost from './components/ToastHost.vue'
import { useWorkbench } from './composables/useWorkbench'

const store = useWorkbench()
const { datasets, resources, selectedId, bootstrap, teardown } = store

const dialog = ref('')
const chat = ref(null)
const codeRunId = ref('')

const openCode = () => {
  codeRunId.value = selectedId.value
  dialog.value = 'code'
}

const shortcuts = (event) => {
  const typing = ['INPUT', 'TEXTAREA', 'SELECT'].includes(event.target?.tagName)
  if (event.key === '/' && !typing) {
    event.preventDefault()
    chat.value?.focus()
  }
  if (event.key.toLowerCase() === 'k' && (event.metaKey || event.ctrlKey)) {
    event.preventDefault()
    document.querySelector('input[type="search"]')?.focus()
  }
}

onMounted(() => {
  bootstrap()
  document.addEventListener('keydown', shortcuts)
})
onUnmounted(() => {
  teardown()
  document.removeEventListener('keydown', shortcuts)
})

const datasetCount = computed(() => datasets.value.length)
const servingCount = computed(() => resources.value.length)
</script>

<template>
  <div class="shell">
    <AppHeader :dataset-count="datasetCount" :serving-count="servingCount"
               @open-datasets="dialog = 'dataset'" @open-serving="dialog = 'serving'"
               @open-settings="dialog = 'settings'" />

    <main class="workspace">
      <RunSidebar />

      <section class="center">
        <ChatPanel ref="chat" @manage-datasets="dialog = 'dataset'" @configure-serving="dialog = 'serving'" />
        <PipelineView @view-code="openCode" @configure-serving="dialog = 'serving'" />
        <StageOutput />
      </section>

      <AgentRail />
    </main>

    <CodeDialog v-if="dialog === 'code' && codeRunId" :run-id="codeRunId" @close="dialog = ''" />
    <ServingDialog v-if="dialog === 'serving'" @close="dialog = ''" />
    <DatasetDialog v-if="dialog === 'dataset'" @close="dialog = ''" />
    <SettingsDialog v-if="dialog === 'settings'" @close="dialog = ''" />
    <ToastHost />
  </div>
</template>

<style scoped>
.shell {
  height: 100%;
  display: flex;
  flex-direction: column;
  background:
    radial-gradient(1100px 420px at 12% -8%, color-mix(in srgb, var(--brand) 8%, transparent), transparent 70%),
    var(--bg);
}
.workspace {
  flex: 1;
  min-height: 0;
  display: grid;
  grid-template-columns: var(--sidebar-width) minmax(0, 1fr) var(--rail-width);
  gap: var(--gap);
  padding: var(--gap);
}
.center { display: flex; flex-direction: column; gap: var(--gap); min-height: 0; overflow: auto; }
/* Nothing in this column may be shrunk below its content: when the three
   panels do not fit, the column scrolls instead of crushing the last one. */
/* One scroll region for the whole column. Pipeline nodes and stage rows grow
   to their natural height instead of clipping into their own short scroll
   boxes, so the wheel works anywhere and the scrollbar is a single, tall one. */
.center > :deep(.chat) { flex: 0 0 auto; }
.center > :deep(.pipeline) { flex: 0 0 auto; }
.center > :deep(.stage) { flex: 0 0 auto; }

@media (max-width: 1440px) {
  .workspace { grid-template-columns: 252px minmax(0, 1fr) 312px; }
}
/* Below this width the page scrolls as one document instead of three
   independently scrolling columns, which is easier to follow on a laptop. */
@media (max-width: 1180px) {
  .workspace {
    grid-template-columns: 240px minmax(0, 1fr);
    grid-template-rows: auto auto;
    overflow: auto;
  }
  .center { overflow: visible; }
  .workspace > :deep(.sidebar) { max-height: 560px; }
  .workspace > :deep(.rail) { grid-column: 1 / -1; max-height: 520px; }
}
@media (max-width: 860px) {
  .workspace { grid-template-columns: minmax(0, 1fr); }
  .workspace > :deep(.sidebar) { max-height: 340px; }
}
</style>
