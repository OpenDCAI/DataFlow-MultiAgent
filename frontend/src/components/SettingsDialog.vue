<script setup>
import { computed, onMounted, ref } from 'vue'
import BaseDialog from './BaseDialog.vue'
import { api } from '../api'
import { useWorkbench } from '../composables/useWorkbench'

const emit = defineEmits(['close'])
const { settings, loadSettings, notify } = useWorkbench()

const form = ref({ enabled: false, api_key: '' })
const busy = ref(false)
const testing = ref(false)
const testResult = ref(null)
const loaded = ref(false)
const build = ref(null)

const current = computed(() => settings.value?.routing || {})
const hasKey = computed(() => !!current.value.has_key)
// What routing will actually do once this dialog closes: the toggle alone is
// not enough, because a key is required for the model to be consulted.
const effective = computed(() => {
  if (!form.value.enabled) return { tone: 'muted', text: '只用关键字规则（不发起网络请求）' }
  if (hasKey.value || form.value.api_key.trim()) {
    return { tone: 'ok', text: '模型判定，不可用时自动退回规则' }
  }
  return { tone: 'warn', text: '未配置 key —— 会退回关键字规则，功能仍可用' }
})

onMounted(async () => {
  api.health().then((health) => (build.value = health.ui)).catch(() => {})
  await loadSettings()
  form.value.enabled = !!current.value.enabled
  loaded.value = true
})

async function save() {
  busy.value = true
  testResult.value = null
  try {
    const payload = { enabled: form.value.enabled }
    if (form.value.api_key.trim()) payload.api_key = form.value.api_key.trim()
    settings.value = await api.saveSettings(payload)
    form.value.api_key = ''
    notify('设置已保存', 'ok')
  } catch (error) {
    notify(error.message)
  } finally {
    busy.value = false
  }
}

async function verify() {
  testing.value = true
  testResult.value = null
  try {
    const payload = {}
    if (form.value.api_key.trim()) payload.api_key = form.value.api_key.trim()
    testResult.value = await api.testSettings(payload)
  } catch (error) {
    testResult.value = { ok: false, error: error.message }
  } finally {
    testing.value = false
  }
}

async function clearKey() {
  busy.value = true
  try {
    settings.value = await api.saveSettings({ api_key: '' })
    form.value.api_key = ''
    testResult.value = null
    notify('已清除已保存的 key', 'ok')
  } catch (error) {
    notify(error.message)
  } finally {
    busy.value = false
  }
}
</script>

<template>
  <BaseDialog title="意图路由设置" @close="emit('close')">
    <div class="body scroll">
      <div class="explain">
        <p>
          <b>Jev 是什么</b>
          TypeSafe 的决策模型。它不生成文本，只回答问题并返回一个带概率的选项，
          所以单次约 0.7 秒，且不会像对话模型那样跑偏。
        </p>
        <p>
          <b>在这里做什么</b>
          判断你发来的消息属于哪一类：新的数据处理需求、进度查询、修改要求，
          还是查看产物。之前只用关键字判断，凡是出现 <code>status</code> 就当进度查询 ——
          而在需求里 <code>status</code> 常常只是一个列名，整条需求会被误当成查询丢掉。
        </p>
        <p>
          <b>怎么拿到 Key</b>
          在 <a href="https://typesafe.ai" target="_blank" rel="noopener">typesafe.ai</a>
          注册后于控制台创建 API key，粘贴到下面的输入框。
        </p>
        <p class="muted">
          关闭，或未填写 Key 时，一律使用本地的关键字规则：不发任何网络请求，功能完整可用。
          模型连不上时也会自动退回规则，只是判断略保守。
        </p>
      </div>

      <label class="row">
        <input v-model="form.enabled" type="checkbox" :disabled="current.enabled_by === 'env'" />
        <span>
          <b>使用 Jev 决策模型判断意图</b>
          <small v-if="current.enabled_by === 'env'">
            当前由环境变量 DF_USE_JEV_ROUTING 强制指定，界面无法覆盖
          </small>
          <small v-else>关闭后只走关键字规则，不发起任何网络请求</small>
        </span>
      </label>

      <div class="field">
        <span>API Key</span>
        <div class="key-row">
          <input v-model="form.api_key" class="input" type="password" autocomplete="off"
                 :placeholder="hasKey ? `已保存 ${current.key_hint}（留空则不修改）` : '粘贴 TypeSafe API key'" />
          <button class="btn small" :disabled="busy || testing" @click="verify">
            {{ testing ? '验证中…' : '验证' }}
          </button>
          <button v-if="hasKey" class="btn small danger" :disabled="busy" @click="clearKey">清除</button>
        </div>
        <small class="note">
          密钥只写入后端 <code>config/resource-secrets.json</code>（0600，Git 忽略），
          不会出现在生成的 pipeline、运行产物或本页面上。
        </small>
      </div>

      <div v-if="testResult" class="result" :class="testResult.ok ? 'ok' : 'danger'">
        <template v-if="testResult.ok">
          验证通过：判定为 <b>{{ testResult.intent }}</b>（置信度 {{ testResult.confidence }}，
          {{ testResult.latency_ms }} ms）
          <small v-if="testResult.rules_intent !== testResult.intent">
            注意：关键字规则给出的是 {{ testResult.rules_intent }}，两者不一致
          </small>
          <small v-else>与关键字规则一致</small>
        </template>
        <template v-else>
          验证失败：{{ testResult.error }}
          <small>路由会自动退回关键字规则。</small>
        </template>
      </div>

      <dl class="status">
        <div><dt>当前生效</dt><dd :class="effective.tone">{{ effective.text }}</dd></div>
        <div v-if="loaded"><dt>凭据来源</dt><dd>{{ { env: '环境变量', registry: '本地密钥文件', none: '未配置' }[current.key_source] || '—' }}</dd></div>
        <div v-if="loaded"><dt>模型 / 接口</dt><dd class="mono">{{ current.model }} · {{ current.endpoint }}</dd></div>
        <div v-if="build"><dt>界面版本</dt><dd class="mono">{{ build.js }}</dd></div>
      </dl>

      <button class="btn primary submit" :disabled="busy || !loaded" @click="save">
        {{ busy ? '保存中…' : '保存设置' }}
      </button>
    </div>
  </BaseDialog>
</template>

<style scoped>
.body { padding: 14px 16px 16px; display: flex; flex-direction: column; gap: 13px; }
.explain { display: flex; flex-direction: column; gap: 9px; }
.explain p { font-size: 11.5px; line-height: 1.7; color: var(--text-2); }
.explain b { display: block; color: var(--text); font-size: 12px; margin-bottom: 1px; }
.explain code {
  font-family: var(--font-mono);
  font-size: 10.5px;
  padding: 1px 4px;
  border-radius: 4px;
  background: var(--surface-3);
}
.explain a { color: var(--brand); }
.explain .muted { color: var(--text-3); }

.row { display: flex; gap: 9px; align-items: flex-start; cursor: pointer; }
.row input { margin-top: 3px; accent-color: var(--brand); width: 15px; height: 15px; }
.row span { display: flex; flex-direction: column; gap: 2px; }
.row b { font-size: 12.5px; font-weight: 600; }
.row small { font-size: 11px; color: var(--text-3); }

.field { display: flex; flex-direction: column; gap: 6px; }
.field > span { font-size: 11px; font-weight: 600; color: var(--text-2); }
.key-row { display: flex; gap: 6px; }
.key-row .input { flex: 1; }
.note { font-size: 10.5px; color: var(--text-3); line-height: 1.6; }
.note code { font-family: var(--font-mono); font-size: 10px; }

.result {
  display: flex;
  flex-direction: column;
  gap: 3px;
  padding: 9px 11px;
  border-radius: var(--radius);
  font-size: 11.5px;
  background: var(--ok-soft);
  color: var(--ok);
}
.result.danger { background: var(--danger-soft); color: var(--danger); }
.result small { font-size: 10.5px; opacity: 0.85; }

.status { display: flex; flex-direction: column; gap: 5px; margin: 0; padding-top: 11px; border-top: 1px dashed var(--border); }
.status > div { display: flex; gap: 10px; font-size: 11.5px; }
.status dt { width: 76px; flex: none; color: var(--text-3); }
.status dd { margin: 0; color: var(--text-2); word-break: break-all; }
.status dd.ok { color: var(--ok); }
.status dd.warn { color: var(--warn); }
.status dd.muted { color: var(--text-3); }
.mono { font-family: var(--font-mono); font-size: 10.5px; }
.submit { height: 34px; }
</style>
