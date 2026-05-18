# 分布式数据采集系统逻辑架构 v0.6 审查报告

审查对象：`docs/logical-architecture.md`（v0.6）、`docs/logical-architecture-plain-explanation.md`、`docs/logical-architecture-presentation.html`

审查日期：2026-05-19

---

## 1. 总体评价

v0.6 在 v0.5 基础上补齐了上轮审查提出的全部 P0 和 P1 项：架构图、接口契约、状态机转移、验收矩阵、备份恢复、时间语义、ConnectorType 能力契约、运维边界、部署前提。文档已经从"核心链路闭环"进入"可详细设计交接"的成熟阶段。

**结论：逻辑架构达到进入详细设计的门槛，以下问题和建议不阻塞启动详细设计，但建议在详细设计早期闭环。**

---

## 2. 已闭环项确认

上轮审查 11 条建议的闭环状态：

| 建议 | 状态 | 落地位置 |
|------|------|----------|
| 架构图与关键时序 | ✅ 已闭环 | §2 三张图 + assets 目录 |
| 接口契约清单 | ✅ 已闭环 | §23 逻辑接口契约清单 |
| 状态机转移表 | ✅ 已闭环 | §8 状态机转移规则 |
| 中心数据存储与下游消费边界 | ✅ 已闭环 | §26.1 |
| 备份恢复边界 | ✅ 已闭环 | §26.2 |
| 时间语义与时钟漂移 | ✅ 已闭环 | §26.3 |
| ConnectorType 能力契约 | ✅ 已闭环 | §26.4 |
| 手工运维边界 | ✅ 已闭环 | §26.5 |
| 验收标准与故障注入矩阵 | ✅ 已闭环 | §26.6 |
| 安全供应链与合规 | ✅ 已闭环 | §26.7 |
| 网络与部署前提 | ✅ 已闭环 | §26.8 |

---

## 3. 需关注的问题

### 3.1 [P1] storageStreamId 语义未显式定义

**位置**：§13 messageId 组成、§13 幂等提交单元、§26.1 数据唯一性约束

`messageId = agentId + storageStreamId + writeSeq`，但 storageStreamId 的含义只说了"Agent 本地稳定标识，用于区分同一 Agent 内不同任务或不同本地缓存流"，缺少精确定义：

- 它是 taskId 的别名，还是独立标识？
- 一个任务是否可能有多个 storageStreamId（如任务迁移到新 Agent 后新开一个流）？
- storageStreamId 是否参与 Gateway 幂等计算？如果参与，Agent 迁移后 replay 数据时 storageStreamId 不变还是变了？

**风险**：如果 storageStreamId 在迁移、任务重建、LocalBuffer 物理隔离粒度变化时语义不清，会导致幂等键冲突或幂等漏判。

**建议**：在 §13 或 §3 补充 storageStreamId 的定义——推荐直接等于 taskId（第一版 1 Task : 1 LocalBuffer 流），并明确迁移后不变。

### 3.2 [P1] DesiredState ENABLED → DISABLED 直接跳转的实际处置语义

**位置**：§8 状态机转移表

表格说"允许但不推荐直接跳转"，并要求"语义上应等价于要求 Agent 进入 DRAINING 后再停用"。但"应等价"是建议还是约束？

- 如果是约束：Agent 收到 DISABLED 期望时，是否仍需先进入 DRAINING 再排空？这与"直接 DISABLED"的行为区别在哪？
- 如果 Agent 收到 DISABLED 但本地仍有未 ACK 数据，它应该停实例还是先排空再停？

**风险**：实际运维中"紧急禁用"场景必然出现（安全事件、客户要求立即停止），如果没有显式定义 Agent 收到直接 DISABLED 时的行为（停实例但保留 LocalBuffer？还是也停止上传？），实现会不一致。

**建议**：补充一条规则——DesiredState 直接跳转 ENABLED → DISABLED 时，Agent 必须：1）停止 ConnectorInstance；2）停止新数据写入；3）继续上传已有 LocalBuffer 数据直到 ACK 或人工干预；4）不推进 Checkpoint。或者明确说直接 DISABLED = 立即停止一切（含上传），然后由人工决定是否需要手动触发排空。

### 3.3 [P1] Agent 心跳上报的 observed versions 与实际执行的时序关系

**位置**：§8 收敛规则

文档说"Agent 每次状态上报必须携带 observedAssignmentVersion、observedDesiredStateVersion..."，但未定义：

- observed version 是"Agent 已收到的最新版本"还是"Agent 已执行完成的版本"？
- 如果 Agent 收到 v10 但正在 DEPLOYING（还没到 RUNNING），observedDesiredStateVersion 应该报 v10 还是上一个版本？
- Gateway 判断"收敛完成"的依据是 observed version == 最新版本，还是 observed version 对应的任务已经 RUNNING？

**风险**：如果 observed version 的语义在 Agent 实现中不统一（有的实现报"已收到"，有的报"已执行完成"），Gateway 的收敛判断和超时告警都会产生误判。

**建议**：明确定义 observed version 为"Agent 已基于该版本开始执行"（即版本已被 DesiredStateCache 持久化并进入 TaskManager 处理），还是"Agent 已将该版本完全执行到目标状态"。推荐前者（更简单），但需要明确 Gateway 侧的收敛完成判断不能仅靠 observed version，还需要结合 TaskRuntimeState。

### 3.4 [P2] BatchState 缺少 ACKED 后的清理态或终态

**位置**：§8 状态机转移、§11 Batch 状态

Batch 状态枚举为 PENDING → UPLOADING → ACKED / RETRY_WAIT / FINAL_FAILED。

- ACKED 之后 Batch 在 Agent 侧的状态是什么？是留在 ACKED 还是变为"已清理"？
- FINAL_FAILED 的 Batch 保留在 LocalBuffer 中还是被标记为可删除？
- Agent 上报"未 ACK 批次数"时，FINAL_FAILED 的 Batch 是否计入？

**风险**：如果 ACKED 的 Batch 永远停留在 ACKED 状态而不被清理，监控指标"未 ACK 批次数"的语义会模糊；FINAL_FAILED 的数据如果不清除但也不重试，会占用本地空间且无法被运维感知。

**建议**：补充 ACKED → CLEANED（或 PURGED）的转移；补充 FINAL_FAILED 的保留策略（是否需要人工确认后清除）。

### 3.5 [P2] GatewayMetadataStore 一致性约束中"读己之写"的工程实现路径

**位置**：§4 GatewayMetadataStore 一致性约束

"Gateway 控制面读取任务期望状态时必须满足读己之写"——这对实现选型有直接约束。如果选用 MySQL/PostgreSQL 默认 READ COMMITTED，需要显式使用 SELECT FOR UPDATE 或同事务内读写；如果选用后续读 eventual consistency 的存储，这条约束直接不满足。

**建议**：在详细设计门禁中补充一条——GatewayMetadataStore 的存储选型必须证明满足"读己之写"（如主库读写、同会话读写、线性化读），否则需要在 Gateway 控制面实现版本缓存或 session stickiness。

### 3.6 [P2] 50B payload 假设过于乐观

**位置**：§19 容量基线

文档已承认 50B 是关键假设并要求校准，但从实际工业采集场景看，50B/sample 是偏小的：

- 带点标识 + 时间戳 + 值 + 质量码的二进制编码约 30-50B，这是最紧凑的情况。
- 但如果 Connector 还要带额外元数据（设备名、工程单位、描述、报警限值等），单条 payload 可能到 200-500B。
- 文档说"任何超过 100B 的平均 payload 都应重新计算"，建议把这句话升级为门禁——即进入详细设计前必须有首批 Connector 的实际 payload 测量结果。

**已有闭环**：§19 已列出校准数据要求，这里只是强调实际 payload 偏大的概率较高，详细设计阶段校准结果很可能推翻当前磁盘容量估算。

### 3.7 [P2] upload lease / catch-up 配额的具体语义不够明确

**位置**：§19 全局 catch-up 协调

"Agent 进入 catch-up 前应获得 Gateway 返回的上传配额或 upload lease"——这条很重要，但语义偏粗：

- lease 的生命周期是什么？Agent 拿到 lease 后持续有效直到追平，还是有 TTL 需要续租？
- 如果 Agent 拿到 lease 后网络再次中断，lease 是否过期？过期后重试是否需要重新申请？
- Gateway 分配 lease 的依据是实时剩余容量还是有预分配池？

**建议**：补充 lease 的基本属性（TTL、续租规则、过期后的 Agent 行为），或者在详细设计门禁中列为必出物。

### 3.8 [P3] 大白话版与正式版之间的内容对齐风险

**位置**：`logical-architecture-plain-explanation.md`

大白话版很好，但它和正式版之间没有版本对应声明。如果正式版后续迭代（如 v0.7、v1.0），大白话版可能过时而不自知。

**建议**：在大白话版开头增加一行版本对应声明（如"本文件对应 logical-architecture.md v0.6"），并在正式版版本记录中同步标注是否需要更新大白话版。

### 3.9 [P3] presentation.html 与 md 正文的主从关系

**位置**：`docs/logical-architecture-presentation.html`

正式 md 已声明"若图片与正文文字冲突，以正文约束为准"。presentation.html 是否也适用此规则？如果 html 是从 md 自动生成则无需额外关注；如果是手工维护的，需要确认同步机制。

**建议**：确认 presentation.html 的生成方式，如果是手工维护，建议在 html 中也加版本号和"以 md 为准"的声明。

---

## 4. 文档质量评价

| 维度 | 评分 | 说明 |
|------|------|------|
| 完整性 | ★★★★★ | 核心链路全覆盖，v0.6 补齐了架构图、接口表、状态机、验收矩阵 |
| 一致性 | ★★★★☆ | storageStreamId 语义、observed version 定义有少量模糊点 |
| 可执行性 | ★★★★☆ | 状态机、ACK 规则、幂等方案均可直接指导详细设计；少量边界（直接 DISABLED、lease）需补充 |
| 可验收性 | ★★★★★ | §26.6 故障注入矩阵 + §26.9 门禁清单非常实用 |
| 可维护性 | ★★★★☆ | 89KB 单文件已较长，后续迭代可考虑拆分 |

---

## 5. 总结

v0.6 是一版高质量的逻辑架构文档。上轮审查的 11 条建议全部闭环，文档已具备进入详细设计的条件。

需要关注的 3 个 P1 项：

1. storageStreamId 语义定义——直接影响幂等键正确性
2. ENABLED → DISABLED 直接跳转的 Agent 行为——直接影响安全禁用场景
3. observed version 的精确语义——直接影响状态收敛判断

建议在详细设计启动后的第一个迭代内闭环 P1 项，P2/P3 可在详细设计过程中逐步补充。
