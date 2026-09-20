<script setup>
import { computed } from 'vue'
import { STATE_ORDER, stateMeta, useWorkbench } from '../composables/useWorkbench'

const emit = defineEmits(['view-code', 'configure-serving'])
const { selected, steps, currentState, canExecute, executing, executePipeline, loadingRun,
        rerunWithCurrentInput, inputSummary } = useWorkbench()

const meta = computed(() => stateMeta(currentState.value))
const reached = computed(() => {
  const index = STATE_ORDER.indexOf(currentState.value)
  return index < 0 && selected.value?.pipeline ? STATE_ORDER.length - 1 : index
})

const fieldsOf = (args, prefix) =>
  Object.entries(args || {})
    .filter(([key, value]) => key.startsWith(prefix) && value !== null && value !== undefined)
    .flatMap(([, value]) => (Array.isArray(value) ? value : [value]))
    .filter((value) => typeof value === 'string')

function nodeOf(step) {
  return {
    id: step.step_id,
    operator: step.operator || step.operator_name || '未绑定',
    source: step.proposal ? '本次生成' : (step.import_path || step.module || ''),
    rationale: step.rationale || step.objective || '',
    inputs: fieldsOf(step.run_args, 'input_'),
    outputs: fieldsOf(step.run_args, 'output_'),
    copies: Object.entries(step.prepare_fields || {}).map(([target, source]) => `${source} → ${target}`),
    custom: !!step.proposal,
    risk: step.risk || 'low',
  }
}
const nodes = computed(() => steps.value.map(nodeOf))
const finalKeys = computed(() => selected.value?.pipeline?.final_keys || [])
const snapshot = computed(() => {
  const keys = selected.value?.input_keys || selected.value?.pipeline?.initial_keys || []
  return keys.length ? `字段 ${keys.join(', ')}` : '未知输入'
})
</script>

<template>
  <section class="pipeline card">
    <div class="card-head">
      <div class="title">
        <span class="eyebrow">PIPELINE</span>
        <h2 class="truncate">{{ selected?.request || '选择一个运行以查看 pipeline' }}</h2>
      </div>
      <span class="spacer" />
      <span v-if="selected" class="tag" :class="meta.tone" :title="meta.hint">
        <span class="dot" />{{ meta.label }}
      </span>
      <button v-if="selected?.pipeline" class="btn small" @click="emit('view-code')">查看代码</button>
      <button v-if="canExecute" class="btn small primary" :disabled="executing" @click="executePipeline"
              :title="`重放该 Run 的输入快照：${snapshot}`">
        {{ executing ? '提交中…' : '运行 pipeline' }}
      </button>
      <button v-if="canExecute" class="btn small" :disabled="executing" @click="rerunWithCurrentInput"
              :title="`复用这条 pipeline，对输入区当前的数据（${inputSummary.text}）新建一次运行`">
        换数据重跑
      </button>
    </div>

    <p v-if="selected?.pipeline" class="snapshot">
      <span class="eyebrow">本次运行的数据</span>
      <span class="mono">{{ snapshot }}</span>
      <span class="hint">· “运行 pipeline” 重放这份快照；要换数据请用“换数据重跑”</span>
    </p>

    <ol v-if="selected" class="stepper">
      <li v-for="(state, index) in STATE_ORDER" :key="state"
          :class="{ done: reached >= index, current: state === currentState }">
        <span class="bullet">{{ index + 1 }}</span>
        <span>{{ stateMeta(state).label }}</span>
      </li>
    </ol>

    <div v-if="selected?.summary" class="banner">
      <b>当前阶段</b><span>{{ selected.summary }}</span>
    </div>
    <div v-if="selected?.reason" class="banner" :class="currentState === 'BLOCKED' ? 'danger' : 'warn'">
      <b>{{ currentState === 'REFUSED' ? 'Planner 拒绝原因' : currentState === 'RESOURCE_REQUIRED' ? '需要配置 API resource' : '运行说明' }}</b>
      <span>{{ selected.reason }}</span>
      <button v-if="currentState === 'RESOURCE_REQUIRED'" class="btn small primary" @click="emit('configure-serving')">
        配置 Serving
      </button>
    </div>

    <div class="graph scroll">
      <template v-if="nodes.length">
        <div v-for="(node, index) in nodes" :key="node.id" class="node-wrap">
          <article class="node">
            <span class="index">{{ String(index + 1).padStart(2, '0') }}</span>
            <div class="node-body">
              <header>
                <b class="mono">{{ node.operator }}</b>
                <span v-if="node.custom" class="tag warn">生成算子</span>
                <span v-else-if="node.risk !== 'low'" class="tag">需复核</span>
                <span class="spacer" />
                <small class="mono truncate source">{{ node.source }}</small>
              </header>
              <p v-if="node.rationale" class="rationale">{{ node.rationale }}</p>
              <div class="chips">
                <span v-for="field in node.copies" :key="`c-${field}`" class="chip copy">{{ field }}</span>
                <span v-for="field in node.inputs" :key="`i-${field}`" class="chip in">in · {{ field }}</span>
                <span v-for="field in node.outputs" :key="`o-${field}`" class="chip out">out · {{ field }}</span>
              </div>
            </div>
          </article>
          <div v-if="index < nodes.length - 1" class="link"><span /></div>
        </div>
        <div v-if="finalKeys.length" class="final">
          <span class="eyebrow">输出字段</span>
          <span v-for="key in finalKeys" :key="key" class="chip out">{{ key }}</span>
        </div>
      </template>
      <div v-else-if="loadingRun" class="loading">
        <div v-for="n in 2" :key="n" class="skeleton node-skeleton" />
      </div>
      <div v-else class="empty">
        <strong>暂无算子绑定</strong>
        <span>Integrator 完成字段对齐后，这里会展示每一步算子。</span>
      </div>
    </div>
  </section>
</template>

<style scoped>
.pipeline { display: flex; flex-direction: column; min-height: 0; }
.title { display: flex; flex-direction: column; min-width: 0; gap: 1px; }
.title h2 { font-size: 13.5px; max-width: 46ch; }
.spacer { margin-left: auto; }

.snapshot {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 7px;
  margin: 11px 16px 0;
  font-size: 11px;
  color: var(--text-2);
}
.snapshot .hint { color: var(--text-3); }
.stepper { display: flex; gap: 6px; list-style: none; margin: 0; padding: 11px 16px 0; flex-wrap: wrap; }
.stepper li {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 10px 4px 5px;
  border-radius: 99px;
  background: var(--surface-3);
  color: var(--text-3);
  font-size: 11px;
  font-weight: 600;
}
.stepper .bullet {
  width: 17px;
  height: 17px;
  border-radius: 99px;
  display: grid;
  place-items: center;
  background: var(--surface);
  color: var(--text-3);
  font-size: 9.5px;
}
.stepper li.done { background: var(--brand-soft); color: var(--brand); }
.stepper li.done .bullet { background: var(--brand); color: #fff; }
.stepper li.current { box-shadow: 0 0 0 1px var(--brand) inset; }

.banner {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  margin: 11px 16px 0;
  padding: 9px 12px;
  border-radius: var(--radius);
  background: var(--surface-2);
  border: 1px solid var(--border);
  font-size: 12px;
  color: var(--text-2);
}
.banner b { font-size: 11px; color: var(--text-3); }
.banner.warn { background: var(--warn-soft); border-color: color-mix(in srgb, var(--warn) 34%, transparent); color: var(--warn); }
.banner.danger { background: var(--danger-soft); border-color: color-mix(in srgb, var(--danger) 34%, transparent); color: var(--danger); }
.banner span { flex: 1; min-width: 180px; word-break: break-word; }

.graph { flex: 1; min-height: 0; padding: 14px 16px 16px; }
.node-wrap { display: flex; flex-direction: column; }
.node {
  display: flex;
  gap: 11px;
  padding: 11px 13px;
  border: 1px solid var(--border);
  border-radius: var(--radius);
  background: var(--surface-2);
  transition: border-color 0.14s ease, box-shadow 0.14s ease;
}
.node:hover { border-color: var(--brand); box-shadow: var(--shadow-sm); }
.index {
  flex: none;
  width: 26px;
  height: 22px;
  border-radius: var(--radius-sm);
  display: grid;
  place-items: center;
  background: var(--brand-soft);
  color: var(--brand);
  font-family: var(--font-mono);
  font-size: 10.5px;
  font-weight: 700;
}
.node-body { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 6px; }
.node-body header { display: flex; align-items: center; gap: 7px; }
.node-body header b { font-size: 12.5px; }
.source { max-width: 30ch; font-size: 10px; color: var(--text-3); }
.rationale { font-size: 11.5px; color: var(--text-2); line-height: 1.5; }
.chips { display: flex; flex-wrap: wrap; gap: 5px; }
.chip {
  padding: 2px 7px;
  border-radius: 5px;
  font-family: var(--font-mono);
  font-size: 10px;
  background: var(--surface-3);
  color: var(--text-2);
}
.chip.in { background: var(--info-soft); color: var(--info); }
.chip.out { background: var(--ok-soft); color: var(--ok); }
.chip.copy { background: var(--muted-soft); color: var(--text-2); }
.link { height: 16px; display: grid; place-items: center; }
.link span { width: 1px; height: 100%; background: linear-gradient(var(--border-strong), var(--brand)); }
.final { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin-top: 14px; padding-top: 12px; border-top: 1px dashed var(--border); }
.loading { display: flex; flex-direction: column; gap: 10px; }
.node-skeleton { height: 76px; }
</style>
