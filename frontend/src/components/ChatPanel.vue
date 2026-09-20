<script setup>
import { computed, nextTick, ref, watch } from 'vue'
import InputSource from './InputSource.vue'
import { useWorkbench } from '../composables/useWorkbench'

const emit = defineEmits(['manage-datasets', 'configure-serving', 'open-input'])
const { chatMessages, sending, startingConversation, conversation, sendMessage, inputSummary } = useWorkbench()

const draft = ref('')
const showInput = ref(false)
const composer = ref(null)
const list = ref(null)

const suggestions = [
  '清洗 raw_content 的多余空格，按清洗结果精确去重，输出 cleaned_content',
  '对 question 生成答案，并过滤掉无法回答的问题',
  '把 raw_content 统一转为小写并去掉 HTML 实体',
]

const visible = computed(() => chatMessages.value.slice(-40))
const isFailure = (message) => ['failure_triage', 'failure_analysis'].includes(message.intent)

async function submit() {
  const text = draft.value
  if (!text.trim()) return
  const ok = await sendMessage(text)
  if (ok) draft.value = ''
  await scrollToEnd()
}

function onKeydown(event) {
  if (event.key !== 'Enter' || event.shiftKey || event.isComposing) return
  event.preventDefault()
  submit()
}

async function scrollToEnd() {
  await nextTick()
  if (list.value) list.value.scrollTop = list.value.scrollHeight
}

function use(text) {
  draft.value = text
  composer.value?.focus()
}

watch(() => chatMessages.value.length, scrollToEnd)
watch(startingConversation, (value) => {
  if (!value) {
    draft.value = ''
    nextTick(() => composer.value?.focus())
  }
})
defineExpose({ focus: () => composer.value?.focus() })
</script>

<template>
  <section class="chat card">
    <div class="card-head">
      <h2>DataFlow 助手</h2>
      <span class="spacer" />
      <span v-if="conversation?.active_run_id" class="tag brand mono truncate" :title="conversation.active_run_id">
        <span class="dot pulse" />{{ conversation.active_run_id }}
      </span>
      <span v-else class="tag">就绪</span>
    </div>

    <div ref="list" class="messages scroll">
      <div v-if="!visible.length" class="intro">
        <h3>描述一个数据任务，多个 Agent 会协作生成可运行的 DataFlow pipeline。</h3>
        <p>Planner 拆解步骤，Operator Specialist 并行检索真实算子源码，Integrator 对齐字段，最后生成原生风格的 pipeline.py。</p>
        <div class="suggestions">
          <button v-for="item in suggestions" :key="item" class="suggestion" @click="use(item)">{{ item }}</button>
        </div>
      </div>

      <div v-for="message in visible" :key="message.message_id" class="message" :class="message.role">
        <span class="avatar" :class="{ alert: isFailure(message) }">{{ message.role === 'user' ? '你' : 'DF' }}</span>
        <div class="bubble" :class="{ alert: isFailure(message) }">
          <b>{{ message.role === 'user' ? '你' : 'Controller' }}</b>
          <p>{{ message.content }}</p>
          <div v-if="isFailure(message)" class="quick">
            <button class="btn small" @click="emit('open-input'); showInput = true">检查输入数据</button>
            <button class="btn small" @click="emit('configure-serving')">配置 Serving</button>
          </div>
          <small>
            <span v-if="message.intent" class="tag" :class="isFailure(message) ? 'danger' : ''">{{ message.intent }}</span>
            <span v-if="message.live" class="tag info">实时事件</span>
            <span>revision {{ message.revision || 0 }}</span>
          </small>
        </div>
      </div>
    </div>

    <div class="composer">
      <div class="composer-row">
        <textarea ref="composer" v-model="draft" class="prompt" rows="2" :disabled="startingConversation"
                  placeholder="描述任务，或继续修改当前任务…（Enter 发送，Shift+Enter 换行）" @keydown="onKeydown" />
        <button class="btn primary send" :disabled="sending || startingConversation || !draft.trim()" @click="submit">
          {{ sending ? '发送中…' : '发送' }}
        </button>
      </div>
      <button class="toggle-input" :class="inputSummary.tone" :aria-expanded="showInput"
              :title="inputSummary.kind === 'sample' ? '仍在使用内置示例数据，点击更换为你自己的数据' : '查看或更换输入数据'"
              @click="showInput = !showInput">
        <span class="chevron" :class="{ open: showInput }">›</span>
        <span>输入</span>
        <span class="summary">{{ inputSummary.text }}</span>
        <span v-if="inputSummary.kind === 'sample'" class="tag warn">未更换</span>
      </button>
    </div>

    <InputSource v-if="showInput" @manage-datasets="emit('manage-datasets')" />
  </section>
</template>

<style scoped>
.chat { display: flex; flex-direction: column; min-height: 0; }
.messages { flex: 1; min-height: 168px; max-height: 38vh; padding: 14px; display: flex; flex-direction: column; gap: 12px; }

.intro { display: flex; flex-direction: column; gap: 9px; padding: 6px 2px 2px; }
.intro h3 { font-size: 14px; line-height: 1.5; }
.intro p { font-size: 12px; color: var(--text-2); }
.suggestions { display: flex; flex-direction: column; gap: 6px; margin-top: 4px; }
.suggestion {
  text-align: left;
  padding: 9px 11px;
  border-radius: var(--radius);
  border: 1px dashed var(--border-strong);
  background: var(--surface-2);
  color: var(--text-2);
  font-size: 12px;
  cursor: pointer;
  transition: border-color 0.14s ease, color 0.14s ease, background 0.14s ease;
}
.suggestion:hover { border-color: var(--brand); color: var(--brand); background: var(--brand-soft); border-style: solid; }

.message { display: flex; gap: 9px; animation: fade-up 0.22s ease both; }
.message.user { flex-direction: row-reverse; }
.avatar {
  flex: none;
  width: 26px;
  height: 26px;
  border-radius: 8px;
  display: grid;
  place-items: center;
  font-size: 10px;
  font-weight: 700;
  color: #fff;
  background: var(--brand-gradient);
}
.message.user .avatar { background: var(--text-2); }
.bubble {
  max-width: min(78%, 640px);
  padding: 9px 12px;
  border-radius: var(--radius);
  background: var(--surface-2);
  border: 1px solid var(--border);
}
.message.user .bubble { background: var(--brand-soft); border-color: color-mix(in srgb, var(--brand) 22%, transparent); }
.bubble b { display: block; font-size: 10.5px; color: var(--text-3); margin-bottom: 2px; }
.bubble p { font-size: 12.5px; white-space: pre-wrap; word-break: break-word; }
.bubble.alert { background: var(--danger-soft); border-color: color-mix(in srgb, var(--danger) 34%, transparent); }
.avatar.alert { background: var(--danger); }
.quick { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
.bubble small { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; margin-top: 6px; font-size: 10px; color: var(--text-3); }

.composer { padding: 10px 12px 11px; border-top: 1px solid var(--border); display: flex; flex-direction: column; gap: 7px; }
.composer-row { display: flex; gap: 8px; align-items: flex-end; }
.prompt {
  flex: 1;
  resize: none;
  border: 1px solid var(--border);
  border-radius: var(--radius);
  background: var(--surface-2);
  padding: 8px 11px;
  font-size: 12.5px;
  line-height: 1.55;
  max-height: 140px;
  transition: border-color 0.14s ease;
}
.prompt:focus { outline: none; border-color: var(--brand); background: var(--surface); }
.send { height: 36px; padding: 0 18px; }
.toggle-input {
  align-self: flex-start;
  display: inline-flex;
  align-items: center;
  gap: 5px;
  border: none;
  background: none;
  padding: 0;
  font-size: 11px;
  color: var(--text-3);
  cursor: pointer;
}
.toggle-input:hover { color: var(--brand); }
.toggle-input .summary { font-family: var(--font-mono); font-size: 10.5px; }
.toggle-input.warn { color: var(--warn); }
.toggle-input.danger { color: var(--danger); }
.toggle-input.brand { color: var(--brand); }
.chevron { display: inline-block; transition: transform 0.16s ease; font-size: 14px; line-height: 1; }
.chevron.open { transform: rotate(90deg); }
</style>
