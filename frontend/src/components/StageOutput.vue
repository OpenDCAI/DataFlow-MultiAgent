<script setup>
import { computed, ref } from 'vue'
import { cellText } from '../format'
import { useWorkbench } from '../composables/useWorkbench'

const { stages, selectedStage } = useWorkbench()
const expanded = ref(null)

const rows = computed(() => selectedStage.value?.rows || [])
const fields = computed(() =>
  selectedStage.value?.fields || (rows.value[0] ? Object.keys(rows.value[0]) : []))

const toggle = (row, field) => {
  const key = `${row}:${field}`
  expanded.value = expanded.value === key ? null : key
}
const isExpanded = (row, field) => expanded.value === `${row}:${field}`
</script>

<template>
  <section v-if="stages.length" class="stage card">
    <div class="card-head">
      <h2>阶段输出</h2>
      <span class="tag">{{ stages.length }} 个阶段</span>
      <span class="spacer" />
      <small v-if="selectedStage" class="mono truncate meta">{{ selectedStage.source }}</small>
    </div>

    <div class="tabs scroll">
      <button v-for="stage in stages" :key="stage.stage_id" class="tab"
              :class="{ active: selectedStage?.stage_id === stage.stage_id }"
              @click="selectedStage = stage">
        <b>{{ stage.index + 1 }} · {{ stage.name }}</b>
        <small>{{ stage.row_count }} 行</small>
      </button>
    </div>

    <div class="table-wrap scroll">
      <table>
        <thead>
          <tr><th class="row-index">#</th><th v-for="field in fields" :key="field">{{ field }}</th></tr>
        </thead>
        <tbody>
          <tr v-for="(row, index) in rows" :key="index">
            <td class="row-index">{{ index + 1 }}</td>
            <td v-for="field in fields" :key="field" :class="{ expanded: isExpanded(index, field) }"
                :title="cellText(row[field])" @click="toggle(index, field)">
              {{ cellText(row[field]) }}
            </td>
          </tr>
        </tbody>
      </table>
      <div v-if="!rows.length" class="empty">该阶段还没有物化的数据行。</div>
    </div>
  </section>
</template>

<style scoped>
.stage { display: flex; flex-direction: column; min-height: 260px; max-height: 440px; }
.spacer { margin-left: auto; }
.meta { max-width: 40ch; font-size: 10.5px; color: var(--text-3); }

.tabs { display: flex; gap: 5px; padding: 9px 12px; border-bottom: 1px solid var(--border); overflow-x: auto; }
.tab {
  flex: none;
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 1px;
  padding: 5px 10px;
  border-radius: var(--radius-sm);
  border: 1px solid var(--border);
  background: var(--surface-2);
  cursor: pointer;
  transition: border-color 0.14s ease, background 0.14s ease;
}
.tab b { font-size: 11.5px; font-weight: 600; }
.tab small { font-size: 10px; color: var(--text-3); }
.tab:hover { border-color: var(--border-strong); }
.tab.active { background: var(--brand-soft); border-color: var(--brand); }
.tab.active b { color: var(--brand); }

.table-wrap { flex: 1; min-height: 140px; }
table { width: 100%; border-collapse: collapse; font-size: 11.5px; }
th, td {
  text-align: left;
  padding: 7px 11px;
  border-bottom: 1px solid var(--border);
  max-width: 340px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  vertical-align: top;
}
th {
  position: sticky;
  top: 0;
  background: var(--surface-2);
  font-size: 10.5px;
  font-weight: 700;
  color: var(--text-3);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  z-index: 1;
}
td { cursor: pointer; font-family: var(--font-mono); color: var(--text-2); }
td.expanded { white-space: pre-wrap; word-break: break-word; background: var(--brand-soft); color: var(--text); }
tbody tr:hover td { background: var(--surface-2); }
tbody tr:hover td.expanded { background: var(--brand-soft); }
.row-index { width: 42px; color: var(--text-3); font-family: var(--font-mono); }
</style>
