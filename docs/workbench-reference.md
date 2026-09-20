# 工作台配置、API 与排障

返回 [README](../README.md)。下文描述当前实现，而非规划中的能力。

## 运行配置

默认配置为 [config/runtime.json](../config/runtime.json)。

| 配置 / 环境变量 | 用途 |
| --- | --- |
| `backend` / `CODEX_BACKEND` | `codex` 或有限的确定性 `offline` 后端 |
| `dataflow_root` / `DATAFLOW_ROOT` | 真实 DataFlow 源码目录，默认是 `external/DataFlow` 子模块；相对配置路径按项目根目录解析 |
| `codex_bin` / `CODEX_BIN` | Codex CLI 可执行文件 |
| `model` / `CODEX_MODEL` | 编排 Agent 使用的模型 |
| `base_url` / `DF_CODEX_BASE_URL` | Responses API 的基础 URL |
| `DF_CODEX_API_KEY` | Codex 后端进程环境中的 API key |
| `timeout_seconds` | 每次 Codex 角色调用上限，默认 240 秒 |
| `agent_attempts` | 每个角色任务的尝试次数，默认 2 |
| `max_parallel_agents` | Specialist 并发数，默认 3 |
| `runtime_timeout_seconds` | DataFlow 执行子进程上限，默认 900 秒 |
| `DF_WEB_HOST` / `DF_WEB_PORT` | 默认 `127.0.0.1:8000` |

配置中的示例 provider/model 不是通用可用承诺。启动时应使用自己的地址和模型。健康检查只说明 Web 服务能响应，不证明上游模型可用。

## Serving 与密钥

Codex 使用 Responses API；Pipeline 的 `APILLMServing_request` 使用 Chat Completions。两个服务可以来自不同 provider。

- Pipeline 构造参数用 `{"$resource":"llm_default"}` 引用资源，允许先生成后注册。
- **Serving / API** 表单配置 URL、模型、`max_workers`、`max_tokens`、temperature 和密钥。
- 密钥可由 `DF_PIPELINE_*` 环境变量提供，或写入后端的 `config/resource-secrets.json`（0600 权限，Git 忽略）。
- `config/resources.json` 保存非密钥定义；执行时绑定最新注册配置。注册资源本身不触发执行。
- 缺少资源进入 `RESOURCE_REQUIRED`。补齐后再次点击 **Run pipeline**，无需重新调用 Planner。
- `.env` 文件不会自动加载；需要在启动服务前显式设置环境变量。

## 指定输入数据

Run API 接受 `input_rows` 或 `dataset_id`。无输入时使用仓库示例数据。

```bash
curl -sS http://127.0.0.1:8000/api/v1/runs \
  -H 'Content-Type: application/json' \
  -d '{
    "request": "清洗 raw_content 中的多余空格并精确去重",
    "input_rows": [
      {"raw_content": "  Hello   world  "},
      {"raw_content": "Hello world"}
    ],
    "allow_custom": false
  }'
```

注册数据集后，可用以下请求提交完整数据集：

```json
{"request":"清洗并去重 raw_content","dataset_id":"ds-…","allow_custom":false}
```

对话消息 API 也可接收首个新任务的 `input_rows` / `dataset_id`。当前前端助手提交的是页面持有的输入行，不应假定它总是传递完整注册数据集。

## 生成的 Pipeline 文件

一次生成会在 run 目录写入三类源码：

| 文件 | 内容 |
| --- | --- |
| `pipeline.py` | 原生风格 DataFlow pipeline：从算子公共包导入、`FileStorage` 与 serving 以字面参数构造、每步一个命名属性、`forward()` 逐个调用 `run(storage=self.storage.step(), …)`，末尾是 `if __name__ == "__main__"` 块。可直接 `python pipeline.py` 运行（默认读取同目录 `./input.jsonl`，写入 `./cache`） |
| `run_pipeline.py` | 工作台执行器：覆写输入与 cache 路径、检测 LLM 空响应、跑生成算子的 fixtures、`compile()` 后执行，并按 `final_keys` 投影输出、写 `runtime-report.json` |
| `custom/<Operator>.py` | 本次生成的算子（若有），由 `pipeline.py` 以 `from custom.<Operator> import <Operator>` 导入 |

`pipeline.py` 中不包含 SPEC 字典、注册表查找或命令行参数；改动应发生在 spec 或 Agent 侧，然后重新生成。工作台执行时使用的命令等价于：

```bash
python run_pipeline.py --input input.jsonl --cache cache \
  --output candidate.jsonl --report runtime-report.json --execute
```

## 失败归因

运行进入 `BLOCKED`、`REFUSED` 或 `RESOURCE_REQUIRED` 时，工作台会在该 Run 所属对话里追加两条消息。

第一条是确定性判定，由 [`diagnosis.py`](../dataflow_agents/diagnosis.py) 从运行证据推出，不调用模型：

| 类别 | 触发条件 |
| --- | --- |
| `input_data` | 运行报告 `error_code=EMPTY_STAGE`，即某一步把所有行都过滤掉了 |
| `serving` | 状态为 `RESOURCE_REQUIRED`、引用的 serving 未注册或缺密钥，或报错命中 HTTP / 认证 / 连接特征 |
| `generation` | 静态契约检查未通过，或没有生成 pipeline |
| `generated_operator` | 本次生成算子的 fixture 未通过 |
| `timeout` | `error_code=RUNTIME_TIMEOUT` |
| `refused` | Planner 判定需求超出支持范围 |
| `other` | 以上都不匹配，附原始报错 |

第二条来自 `failure_analyst` 角色：把同一份证据和上面的判定交给编排后端，要求它给出归因、至多 4 条下一步，必要时给出可直接发送的修改需求。模型不可用或返回不合规时，对话里会明确说明“模型分析不可用”，第一条判定仍然有效。离线后端只复述确定性判定。

证据在送入模型和写进对话前会做凭据脱敏（`sk-` 形式的密钥、stderr 摘录）。产物保存在 `runs/<run>/failure-analysis.json` 和 `runs/<run>/agents/failure-analyst/`；同一个失败（状态 + 报错摘要的 hash 相同）只解释一次。

## 状态与执行语义

| 状态 | 含义 |
| --- | --- |
| `READY` | spec 静态检查完成、源码已生成，尚未实际执行 |
| `RUNNING` | Web 用户启动的 Pipeline 正在执行 |
| `EXECUTED` | 运行报告成功，输出已保存；没有独立 Verifier 认可的保证 |
| `VERIFIED` | 非 Web 自动流程的实际执行和独立 Verifier 都通过 |
| `BLOCKED` | 生成、编译、执行或验证失败 |
| `REFUSED` | Planner 判断不支持当前请求 |
| `RESOURCE_REQUIRED` | 执行所需服务或凭据未配置 |
| `APPROVAL_REQUIRED` | 自动执行流程等待授权 |

Web 强制关闭 `auto_execute`。CLI 可使用独立配置文件显式设置 `auto_execute: true`，此模式对自定义算子和外部资源保留审批检查；人工批准与 spec、Pipeline、输入、custom source 的 hash 绑定。

```bash
.venv/bin/python -m dataflow_agents.cli approve runs/run-…
.venv/bin/python -m dataflow_agents.cli resume runs/run-… --config config/runtime.json
.venv/bin/python -m dataflow_agents.cli promote runs/run-… --deployment deployments/production
.venv/bin/python -m dataflow_agents.cli rollback --deployment deployments/production
```

自动执行时请将 `--config` 指向你启用了 `auto_execute` 的配置文件。只有 `VERIFIED` 产物可晋级；deployment rollback 与 Conversation revision 是不同机制。

## Prompt 模板契约

以下适配覆盖 `ReasoningQuestionFilter`、`ReasoningQuestionGenerator` 和 `ReasoningAnswerGenerator`：

```json
{"prompt_template": null}
```

`null` 让算子创建默认 math 模板，规避上游部分构造函数把模板类本身设为默认值的问题。显式选择模板：

```json
{
  "prompt_template": {
    "$prompt": "GeneralQuestionFilterPrompt",
    "args": {}
  }
}
```

Diy 模板需要构造参数：

```json
{
  "prompt_template": {
    "$prompt": "DiyQuestionFilterPrompt",
    "args": {"prompt_template": "Evaluate the question: {question}"}
  }
}
```

每个算子只接受 [prompt_templates.py](../dataflow_agents/prompt_templates.py) 列出的对应模板。未知、错配模板或缺少参数会在编译路径明确报错。历史 Run 中已知的类名字符串会在 Web 再执行时转换为结构化引用，再实例化；不会求值任意 Python 表达式，也不会修改上游 DataFlow 源码。

## API 索引

服务运行后，可查看 FastAPI 的 <http://127.0.0.1:8000/docs>。

| 接口 | 用途 |
| --- | --- |
| `GET /api/v1/health` | 本地服务健康检查 |
| `GET /api/v1/runs`、`POST /api/v1/runs` | 列出最近 100 个 Run / 创建任务 |
| `GET /api/v1/runs/{id}`、`DELETE /api/v1/runs/{id}` | 状态 / 删除已结束任务 |
| `POST /api/v1/runs/{id}/execute` | 执行已生成 Pipeline |
| `GET /api/v1/runs/{id}/events`、`…/stream` | 事件列表 / SSE |
| `GET /api/v1/runs/{id}/pipeline-code`、`…/stages` | 完整源码 / 各阶段数据 |
| `GET /api/v1/runs/{id}/collaboration`、`…/skills`、`…/evidence` | 协作、Skill 元数据与证据聚合 |
| `GET /api/v1/runs/{id}/revisions` | 当前 Run 及其直接子 revision |
| `POST /api/v1/conversations`、`GET /api/v1/conversations` | 创建 / 列出本地会话 |
| `GET /api/v1/conversations/{id}`、`POST …/{id}/messages` | 读取会话 / 发送消息 |
| `/api/v1/datasets` | 数据集注册、列出、预览与删除 |
| `/api/v1/servings`（兼容 `/resources`） | Serving 注册、列出和删除 |
| `POST /api/v1/models` | 模型列表发现 |

运行中或仍持有 leader 锁的 Run 不可删除。重复删除已移除的合法 Run ID 为幂等操作；权限或 IO 问题返回可读错误。当前删除接口尚未级联维护所有会话引用；若旧会话指向已删除 Run，应开启新对话。

## 常见问题

| 现象 | 排查顺序 |
| --- | --- |
| 页面打不开 | 确认服务仍运行、端口为 8000；执行 `curl http://127.0.0.1:8000/api/v1/health` |
| 提示 `Set DF_CODEX_API_KEY` | 将变量设置在启动后端的进程环境中；配置 Pipeline Serving 无法代替此凭据 |
| `Codex role timed out` | 查看该 attempt 的 `transport.json`、`stderr.log` 和 `codex-events.jsonl`；区分本地 240 秒上限与 provider 503/断流 |
| `Invalid prompt_template type` | 检查模板引用格式并使用新版后端重新执行；目前适配范围为上述三个 Reasoning 算子 |
| `ModuleNotFoundError` | 从项目 `.venv` 启动、完整安装 `requirements-local.txt` 并运行 `pip check` |
| Stage 没有输出 | `READY` 只生成代码；需显式执行成功后才会产生阶段缓存 |
| Skill 次数为 0 | 查看是否实际出现对应 `skill.invoked`；默认 Web 流程不会调用 Verifier |

前端开发：`npm run dev --prefix frontend`；Vite 将 `/api` 代理到本地 8000 端口。生产静态资源由 `npm run build --prefix frontend` 构建，后端从 `frontend/dist` 提供页面。
