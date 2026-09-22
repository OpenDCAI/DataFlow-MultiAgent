# 合并记录：同事版本（v2）与本分支的差异

合并时间：2026-09-22。来源：`DataFlow-MultiAgent_v2_副本.zip`。

规则：**功能或语义冲突时以 her 的设计为准**。本文记录每一处冲突、她的取舍理由，以及我为了让两边共存做的适配，便于你复核。

---

## 一、合并进来的修复与能力

### 1. 行保留约束（`dataflow_agents/row_policy.py`，新增）

用户明确说"保留全部记录""不删除任何记录""不要写过滤算子"时，之前**没有任何强制**——过滤算子照用，pipeline 显示 VERIFIED，但数据已经少了。这是**静默数据丢失**。

三层防护：

| 层 | 函数 | 时机 |
| --- | --- | --- |
| 识别约束 | `preserve_all_rows()` | 请求解析时，中英文模式 + `constraints` 显式开关 |
| 执行前拦截 | `row_filter_errors()` | 静态检查后、执行前，按算子名后缀或 catalog category 判定行过滤算子 |
| 执行后核验 | `row_loss_error()` | 输出行数 < 输入行数即判 fail |

约束会**下传给 Planner / Specialist / Integrator**（见 `contracts.py` 里追加的 PROMPTS 段落）。

**边界处理**（值得保留的设计）：区分"列"与"行"——"不得删除原始字段"不算行约束；条件例外（"不得删除 quality_label 为 reject 的记录；对其他记录去重"）不算全局禁令；用户明确要求过滤时仍然允许过滤算子。

### 2. 执行续跑（`execution-continuation.json`）

暂停（`APPROVAL_REQUIRED` / `RESOURCE_REQUIRED`）时持久化 **repair 轮次 + integrated bindings + feedback**，恢复时直接执行已审阅的产物，**不再从第 0 轮重跑 Planner/Specialist**。跨暂停与重启**保留修复次数上限**。

对**旧版本遗留的暂停 Run**（没有 continuation 文件）也会从 `static-validation.json` + `pipeline-spec.json` + `agents/integrator-N/` 反推，不强制重跑。

### 3. 字段验证模式（`dataflow_agents/verification.py`，新增）

`verification_mode` 可选 `fields` / `semantic`：

- **fields**：纯程序检查——编译执行成功、输出文件存在、输出行字段与 `final_keys` 完全一致、行数与执行报告一致。**不调用任何模型**，不评价内容语义。
- **semantic**：原来的模型 Verifier。

细节：检查**所有行**（非前 20 行），问题数上限 20 条以免撑爆修复提示；空输出仅在"执行成功且报告含所需字段"时放行；**运行错误不能被字段正确覆盖**。

### 4. 删除/损坏记录下的健壮性

- `TeamStore.connect()` 改为 `mode=rw`（URI 形式）：数据库被删后**报错而非重建空库**。原来的行为会让一个迟到的写入方把整段历史覆盖成空。**创建只在 `TeamStore.__init__` 里发生一次**（我的适配，见下）。
- `_existing_run_lock()`：后台工作在创建文件**之前**取锁并复查目录是否还在，避免已完成删除的 Run 被"复活"。失败归因与 analyst 都走这个锁。
- `_run_updated()` + `_run_summary()` 包装：单个 Run 记录损坏只影响它自己（`record_error: true`），不拖垮整个列表；排序对缺失/非法时间戳与并发删除免疫。

---

## 二、冲突点与取舍（**以她的设计为准**）

### A. `auto_execute` 不再是 Web 层强制关闭

| | 原（我们） | 现在（她） |
| --- | --- | --- |
| 代码 | `cfg = dict(cfg, auto_execute=False)` | `cfg.setdefault("auto_execute", False)` |
| 语义 | Web 永远生成到 `READY`，点击才执行 | 由配置决定；开启后生成→执行→检查一次走完 |

**影响**：如果 `config/runtime.json` 写了 `auto_execute: true`，网页新建任务会**自动执行**，不再停在 READY。

**注意**：仓库里的 `config/runtime.json` **没有**设置该项（保持默认关闭）。她本地开启过，我没有把她的本地配置带进来。**演示前请确认你要哪种流程。**

### B. 删除了关键字算子替换与确定性 fallback

原 `_workflow` 里有两处按关键词替换 Agent 结果的行为，**已按她的设计全部移除**：

1. `_fallback_binding()`：绑定失败时按 `synth`/`reasoning` 等关键词猜算子；
2. Integrator 结果里把 custom proposal 换成 `ReasoningQuestionGenerator` 等硬编码算子。

**理由**（她的判断，我认同）：这属于"用关键词覆盖模型判断"，与 `status` 被误判成进度查询是同一类错误，只是方向相反。现在绑定失败会直接报错。

### C. 验证语义变化 —— 演示时要注意

`VERIFIED` 现在可能只代表**字段契约通过**，不代表内容正确。她给出了两个可选值并如实写进 README。如果演示台词说"已验证"，建议明确讲"输出字段与 `final_keys` 一致、行数与执行报告一致"。想恢复内容判断：`verification_mode: "semantic"`。

### D. 与会议纪要的对应关系

会议纪要（徐畅 × 何润明）里两个待办问题，本次合并都已落地：

| 纪要中的问题 | 处理 |
| --- | --- |
| "修复模式开启后 Verifier 频繁判错、修复后再次失败" | 新增 `verification_mode: "fields"`：**不调用模型**，只做编译/执行/字段/行数的确定性检查。想保留模型判断仍可切回 `semantic` |
| "合成算子包含过滤算子时，Verifier 阶段判错，疑似强制替换机制引发" | **强制替换机制已删除**（见冲突点 B）。实测：一个包含 `ReasoningQuestionFilter` 的合成 pipeline，在字段模式下验证通过；只有用户**明确要求保留全部记录**时才会拦过滤算子 |
| "小问题暂不处理，先尽快 push" | 本次合并前跑通 155 个测试；另由独立 Codex 红队复核，发现并修掉一个真实缺陷（见下） |

---

## 补充：独立红队复核（合并后）

用**独立会话、独立凭据**的 Codex 对合并后的代码做对抗性复核，发现一个我们双方都漏掉的真缺陷：

**手动执行路径完全没有验证。** `POST /api/v1/runs/{id}/execute`（以及调用它的"换数据重跑"）只看 `runtime["status"] == "passed"` 就把 `candidate.jsonl` 复制成 `output.jsonl` 并标记 `EXECUTED`——**不调用 `verify_fields`，也不调用模型 Verifier**。编排器那条路是验证的，按钮这条路不是。后果：一条陈旧或错误的 pipeline 在新数据上产出畸形/丢行的结果，界面仍然报"执行成功"。

已修复：手动执行现在跑同一套确定性字段检查（编译/执行标志、输出文件存在、逐行字段与 `final_keys` 一致、行数与报告一致），有保留记录约束时一并核验；**"报告通过但没有输出文件"判为失败**；通过后写 `integrity.json` 与 `verification.json`。

红队同时确认：`row_policy.py` 未发现额外绕过；`verification.py` 的字段与行数检查足够严格；`codegen.py` 与 runner 静态审查未发现新问题；前端 `CodeDialog.vue` 的 `v-html` 因 highlight.js 输出与回退分支都已转义，**不构成 XSS**；run/artifact 端点的路径穿越防护合理。

> 红队报告的其余"启动崩溃"结论未采纳：那是它沙箱内无法创建临时目录导致的，在真实环境下 `load_config()` 正常返回 `verification_mode: semantic`（已实测）。报告本身无法写出文件也是同一原因。

---

## 三、我做的适配（非冲突，仅为了两边共存）

1. **`team.py` 的 `mode=rw` 会破坏首次建库**（全新 Run 目录没有 `team.sqlite`，`mode=rw` 直接失败）。改为：**仅当文件不存在时**用普通连接创建一次，此后一律 `mode=rw`。保留她"迟到写入方不得重建"的保护。
2. **`router` 与 her 的 `classify_message` 并存**：她的 `conversation.py` 是我拆分 `routing.py` **之前**的版本（内含关键字规则）。我保留 `routing.py`（Jev 决策模型 + 规则兜底），把她的文件内容并入现有结构，**没有覆盖**。她的 zip 里没有 Jev 相关代码。
3. **`web.py` 以我们这版为基底**（含设置界面、Jev、失败归因、缓存头），把她的 4 项健壮性改动并入。直接覆盖会丢掉设置界面与 Jev 路由。
4. **`health` 增加 `verification_mode` 字段**：她的测试依赖它。
5. **`tests/test_orchestrator.py::test_web_generation_requires_explicit_run_and_uses_latest_serving`**：该测试原本断言 Web 强制 `auto_execute=False`。按冲突规则 A 改为显式传 `auto_execute=False`（它测的是手动流程本身，语义不变）；其中 stub 的 `execute()` 返回值也从 `{"status": "passed", "rows": 2}` 补成真实报告的形状并写入输出行——否则它断言的是"没有检查"，而不是它想测的重新绑定行为。
6. **`list_runs` 改用 `_run_updated`**，与她的排序健壮性一致。

---

## 四、未合并 / 需要你决定

- **她的本地 `config/runtime.json`**（`auto_execute: true`、`verification_mode: "fields"`）**没有带入**，仓库保持默认。要不要改由你定。
- 她的 `.env.goai.local`（517 字节）**没有带入**，可能含她的凭据。
- 她的 `runtime/` 目录（另一个 runs 根）**没有带入**。
- 她的 `LICENSE` 缺失（我们这边已按 Apache-2.0 加好）。

---

## 五、验证状态

- 合并前：127 个测试。
- 合并后：**155 个测试全过**（含她的 4 个新测试文件：`test_row_policy` / `test_field_verification` / `test_execution_continuation` / `test_run_resilience`，以及 `tests/fixtures/order_no_filter.json`）。
- 未做：真实 Codex 端到端跑一遍行保留约束（需要真实 key 与时间），以及前端在 `auto_execute: true` 下的交互确认。
