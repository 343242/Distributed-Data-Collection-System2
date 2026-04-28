# 分布式数据采集系统逻辑架构方案

## 1. 设计目标

本系统用于统一管理部署在客户现场的采集节点，通过中心侧 `Gateway` 对客户现场 `Agent` 进行纳管、配置下发、程序包下发、任务启停、运行监控和数据接入，最终将采集数据写入中心数据存储，供业务系统使用。

第一版设计目标是：

- 支持多个客户，每个客户可部署多个 `Agent`
- 每个 `Agent` 可运行多个 `ConnectorInstance`
- `Gateway` 作为统一控制入口和数据接入入口
- `Agent` 负责现场执行、Connector 管理、本地可靠缓冲和恢复上传
- 数据采集语义采用 `至少一次`
- 尽最大可能避免数据静默丢失，允许重复数据
- 第一版不实现自动动态分发，但预留后续演进能力

## 2. 总体逻辑架构

系统整体由以下核心逻辑域组成：

- `Customer`：系统一级租户边界，用于划分客户、Agent、任务、数据、权限和监控视图。
- `Gateway`：公司内部中心服务，对外作为统一入口，内部逻辑拆分为控制面和数据接入面。
- `Agent`：部署在客户现场的独立服务器，负责本客户现场 Connector 的生命周期管理、本地数据缓冲、状态上报和数据上传。
- `Connector`：实际执行数据采集的程序。不同类型 Connector 对应不同程序包，同一程序包可按任务启动多个实例。
- `数据存储`：中心侧持久化存储，保存 Gateway 接入后的采集数据。
- `业务系统`：从数据存储读取数据，不直接依赖 Agent 或 Connector。

系统核心链路为：

```text
控制链路：Gateway -> Agent -> ConnectorInstance
数据链路：ConnectorInstance -> Agent LocalBuffer -> Gateway -> 数据存储
监控链路：Agent -> Gateway
```

## 3. 核心对象模型

系统核心对象关系为：

```text
Customer -> Agent -> CollectorTask -> ConnectorInstance
```

`Customer` 是客户租户对象，是数据隔离、权限隔离和任务归属的一级边界。

`Agent` 属于某个 `Customer`，表示客户现场的一台受管采集节点。

`CollectorTask` 是最小控制单元，表示“一个数据源 + 一套采集配置”。任务由 Gateway 创建并分配给某个 Agent。

`ConnectorInstance` 是任务在 Agent 上的实际运行实例。第一版采用 `1 CollectorTask : 1 ConnectorInstance`。

`ConnectorType` 表示采集器类型。

`ConnectorPackage` 表示某个 ConnectorType 的具体程序包版本。

`LocalBuffer` 是 Agent 本地可靠缓冲组件，所有采集数据必须先进入本地缓冲，再上传 Gateway。

## 4. Gateway 逻辑职责

Gateway 对外是统一入口，内部按控制面和数据接入面拆分。

控制面职责：

- Customer 管理
- Agent 注册、审批、发现和状态管理
- CollectorTask 创建、分配、启停和配置管理
- ConnectorPackage 管理、版本管理和下发
- Agent、Task、ConnectorInstance 监控汇总
- 告警、审计、运维视图
- 为后续动态分发预留调度能力

数据接入面职责：

- 接收 Agent 上传的数据批次
- 校验 Customer、Agent、Task 归属关系
- 校验批次和记录协议
- 基于幂等键识别重复数据
- 写入数据存储
- 返回明确 ACK

## 5. Agent 逻辑职责

Agent 是客户现场的执行节点和数据缓冲节点，职责包括：

- 向 Gateway 注册并维持心跳
- 接收 Gateway 下发的任务期望状态
- 下载、校验和管理 ConnectorPackage
- 启动、停止、重启和监控 ConnectorInstance
- 接收 ConnectorInstance 采集结果
- 将采集数据写入本地可靠缓冲
- 批量上传本地缓冲数据到 Gateway
- 根据 Gateway ACK 更新批次状态
- 在 ACK 后推进任务 Checkpoint
- 上报资源、任务、实例、缓冲、日志和告警摘要

Agent 是现场自治单元。Gateway 不直接管理 Connector 进程，而是通过 Agent 下发期望状态。

## 6. 任务与实例模型

第一版任务模型固定为：

```text
1 CollectorTask : 1 ConnectorInstance
```

一个 CollectorTask 表示一个数据源和一套采集配置。一个任务在同一时刻只能分配给一个 Agent，并由该 Agent 上的一个 ConnectorInstance 执行。

启停、重试、配置变更、审计均以 CollectorTask 为主粒度。

监控采用双层视图：

- 任务级监控：关注采集是否正常、Checkpoint 是否推进、上传是否滞后
- 实例级监控：关注进程是否存活、资源占用、重启次数和运行错误

## 7. 状态模型

系统采用期望状态和运行状态分离的方式。

`DesiredState` 表示 Gateway 期望任务处于什么状态：

- `ENABLED`
- `DISABLED`

`RuntimeState` 表示 Agent 上报的实际运行状态：

- `CREATED`
- `ASSIGNED`
- `DEPLOYING`
- `STARTING`
- `RUNNING`
- `THROTTLED`
- `PAUSED_BY_BACKPRESSURE`
- `STOPPING`
- `STOPPED`
- `FAILED`

Gateway 负责维护期望状态，Agent 负责上报实际状态。两者不一致时，系统进入状态收敛流程。

## 8. 数据可靠性设计

第一版数据语义确定为：

```text
至少一次：允许重复，尽量不丢
```

数据流为：

```text
ConnectorInstance -> Agent LocalBuffer -> Gateway -> 数据存储
```

处理流程：

1. ConnectorInstance 采集数据。
2. Agent 为记录生成 `messageId`。
3. Agent 将数据写入 LocalBuffer。
4. Agent 按批次上传到 Gateway。
5. Gateway 写入数据存储。
6. Gateway 返回持久化 ACK。
7. Agent 收到 ACK 后标记批次完成。
8. Agent 推进任务级 Checkpoint。
9. Agent 后续清理已确认本地缓冲数据。

关键原则：

- 数据写入 LocalBuffer 后，才算 Agent 侧接住数据
- Gateway 返回 SUCCESS 后，Agent 才能推进 Checkpoint
- 未 ACK 数据必须保留在本地缓冲中
- 部分成功时第一版不做批次内断点续传，采用整批重传
- Gateway 通过记录级幂等跳过已写成功记录

## 9. LocalBuffer 设计

Agent 本地缓冲采用：

```text
顺序日志 + 嵌入式 KV 索引
```

逻辑组件包括：

- `AppendLog`：顺序追加保存采集数据和批次内容。
- `BufferIndex`：保存 batchId、messageId、任务归属、上传状态、重试次数、日志位置、checkpointCandidate。
- `UploadCursor`：记录任务上传进度、已 ACK 位置和待重传范围。
- `RetentionManager`：清理已 ACK 且 Checkpoint 已推进的数据。
- `RepairScanner`：Agent 重启后修复未完成上传状态和索引状态。

本地存储引擎可采用 RocksDB、Pebble 或同类嵌入式持久化引擎。第一版不建议使用 Kafka 作为每个 Agent 的本地缓冲，因为部署、运维和资源负担较重。

## 10. messageId 与幂等设计

`messageId` 由 Agent 在写入本地缓冲时生成。

推荐组成：

```text
customerId + taskId + agentId + localSequence
```

要求：

- localSequence 必须持久化
- Agent 重启后不能重复生成旧序号
- 同一条本地缓冲记录重传时 messageId 不变
- Gateway 使用 `customerId + taskId + messageId` 做记录级幂等

系统级幂等依赖 messageId。业务级去重不由第一版统一保证，如有需要，应由具体 ConnectorType 或下游业务规则基于源端主键、offset、时间戳等处理。

## 11. ACK 语义

Gateway ACK 分为三类：

- `SUCCESS`：批次内所有记录已写入成功，或重复记录已确认跳过。
- `RETRYABLE_FAILURE`：结果不确定或临时失败，Agent 应稍后整批重传。
- `FINAL_FAILURE`：权限、配置、协议、归属关系等不可重试错误，需要人工介入。

只有 `SUCCESS` 允许 Agent 标记批次为 `ACKED` 并推进 Checkpoint。

## 12. Checkpoint 设计

Checkpoint 属于 CollectorTask。

Checkpoint 的具体内容由 ConnectorType 自解释：

- 轮询型 Connector 可使用时间点、主键、水位线
- 流式 Connector 可使用 offset、sequence、subscription cursor
- 多游标场景可放在任务级 Checkpoint 的内部结构中

Checkpoint 推进规则：

```text
只有 Gateway 确认持久化成功后，Agent 才能推进 Checkpoint
```

Checkpoint 不用于去重，messageId 不用于采集恢复。两者职责必须分离。

## 13. 背压策略

LocalBuffer 是背压第一触发点。

缓冲水位分为：

- `NORMAL`
- `HIGH`
- `CRITICAL`
- `FULL`
- `ERROR`

策略：

- `NORMAL` 正常采集和上传
- `HIGH` 继续采集，产生告警，提高上传优先级
- `CRITICAL` 暂停低优先级或积压贡献较大的任务
- `FULL` 停止继续写入缓冲的任务，保护磁盘
- 默认不丢弃数据

Agent 可向 ConnectorInstance 下发：

- `THROTTLE`
- `PAUSE`
- `STOP`

每种 ConnectorType 需要声明支持哪些背压动作。不能安全暂停的流式 Connector，在严重背压时允许停止，后续依赖 Checkpoint 恢复，接受重复数据。

## 14. 异常恢复策略

Agent 离线：

- Gateway 标记 Agent 为 OFFLINE
- 不立即把任务改为 STOPPED
- 不自动迁移任务
- Agent 恢复后重新上报本地状态

Gateway 不可用：

- 已运行 ConnectorInstance 可继续采集
- 数据继续进入 LocalBuffer
- 上传失败批次保持未 ACK
- Gateway 恢复后继续上传

数据存储失败：

- Gateway 返回 RETRYABLE_FAILURE
- Agent 不推进 Checkpoint
- Agent 后续重传

ConnectorInstance 崩溃：

- Agent 有限次数自动重启
- 超过阈值后任务进入 FAILED
- 采集恢复依赖 Checkpoint
- 上传恢复依赖 LocalBuffer

Agent 重启：

- 以本地持久化状态为准恢复
- 将 UPLOADING 批次回退为待上传
- 恢复任务和实例状态
- 向 Gateway 上报恢复结果

本地缓冲损坏：

- Agent 标记 DEGRADED 或 FAILED
- 受影响任务暂停
- 上报告警
- 不自动推进 Checkpoint
- 需要人工介入

## 15. 监控、日志与告警

监控对象包括：

- Customer
- Agent
- CollectorTask
- ConnectorInstance
- LocalBuffer
- Gateway
- 数据存储

Agent 上报指标：

- 在线状态
- Agent 版本
- CPU、内存、磁盘
- LocalBuffer 水位
- 未 ACK 批次数
- 最老未 ACK 数据年龄
- 上传成功率和失败次数
- 最近成功上传时间
- ConnectorInstance 数量
- 任务状态摘要

Task 监控指标：

- 期望状态
- 实际状态
- 所属 Customer 和 Agent
- Connector 类型和版本
- 最近采集时间
- 最近写入 LocalBuffer 时间
- 最近 Gateway ACK 时间
- 当前 Checkpoint
- 未 ACK 数据量
- 最近错误码

日志分为：

- 运行日志
- 审计日志
- 数据接入日志

第一版默认上传日志摘要和关键错误，详细日志按需由 Gateway 拉取。

告警至少包括：

- Agent 离线
- Agent DEGRADED
- LocalBuffer HIGH/CRITICAL/FULL
- 最老未 ACK 数据超阈值
- 任务连续失败
- Connector 连续重启
- Gateway 数据接入失败率过高
- 程序包下发失败
- Agent 注册待审批超时

## 16. 安全与权限

Customer 是权限和数据隔离一级边界。所有核心对象必须带 customerId。

Agent 注册策略：

```text
按客户白名单自动纳管，其余需要审批
```

安全规则：

- Agent 不能只靠自报 customerId 注册
- Agent 必须携带注册凭证或预置令牌
- Agent 与 Gateway 全链路加密通信
- 数据上传时 Gateway 校验 Customer、Agent、Task 归属关系
- 被禁用 Agent 的控制请求和数据上传请求都应被拒绝

ConnectorPackage 安全：

- 程序包由 Gateway 统一管理
- 程序包必须有版本号
- 程序包必须校验哈希
- 建议支持签名校验
- Agent 不允许运行未知程序包

Connector 运行隔离：

- 每个任务独立运行目录
- 每个实例只能访问自身任务配置
- Connector 不直接持有 Gateway 凭证
- Connector 不直接访问其他任务数据

管理端权限至少包括：

- 系统管理员
- 客户管理员
- 运维人员
- 审计/只读角色

高风险操作必须记录审计。

## 17. 程序包与版本管理

核心对象：

- `ConnectorType`
- `ConnectorPackage`
- `PackageVersion`
- `TaskConfigVersion`

规则：

- CollectorTask 显式绑定 ConnectorType 和 ConnectorPackage 版本
- Agent 按任务需要主动拉取程序包
- Agent 下载后校验哈希或签名
- Agent 本地维护包仓库
- 同一程序包可启动多个实例
- 程序包版本与任务配置版本分离

升级策略：

- 第一版采用任务级升级
- Gateway 修改任务期望程序包版本
- Agent 停止旧实例并启动新实例
- 未 ACK 数据和 Checkpoint 不受升级影响
- 回滚必须由 Gateway 显式发起
- 第一版灰度建议按 CollectorTask 手动灰度

## 18. 动态分发预留

第一版不实现自动动态分发。

不做：

- 自动任务迁移
- 多 Agent 同时执行同一任务
- 任务拆分并行采集
- 实时负载均衡

预留字段：

- `assignedAgentId`
- `assignmentVersion`
- `migrationState`
- `lastConfirmedCheckpoint`
- `resourceRequirement`

原则：

```text
同一时刻，一个 CollectorTask 只能有一个有效 Owner Agent
```

后续迁移必须基于：

- 最后确认 Checkpoint
- 最新 assignmentVersion
- 目标 Agent 支持对应 ConnectorType
- 目标 Agent 具备对应 ConnectorPackage
- 旧 Agent 未 ACK 数据已经处理完，或明确接受重复/人工介入

## 19. 第一版范围

第一版必须实现：

- Customer 管理与白名单纳管
- Agent 注册、审批、心跳、状态上报
- ConnectorType 和 ConnectorPackage 管理
- CollectorTask 创建、分配、启停
- 1 Task : 1 ConnectorInstance
- Agent 本地可靠缓冲
- Gateway 数据接入与 ACK
- 至少一次上传与幂等处理
- 任务级 Checkpoint
- 背压策略
- 基础监控、日志摘要、告警
- 程序包下发、版本校验、任务级升级
- 安全认证、权限隔离、审计

第一版不做：

- 自动动态分发
- 严格 exactly-once
- 批次内断点续传
- 自动灰度升级
- 复杂业务级去重
- Agent 离线后的自动任务迁移
- 本地缓冲损坏后的自动无损恢复承诺

## 20. 原始需求映射

需求 1：客户 Agent 是客户现场独立服务器，同一客户可能多个 Agent。

设计映射：引入 Customer，建立 Customer -> Agent 纳管关系。

需求 2：一个 Agent 上根据配置运行多个 Connector。

设计映射：Gateway 创建 CollectorTask，Agent 将任务落地为 ConnectorInstance。

需求 3：Connector 有不同类型，不同类型是不同程序包，同一程序包可启动多个实例。

设计映射：使用 ConnectorType 和 ConnectorPackage 建模，本地包仓库支持包复用。

需求 4：Agent 负责 Connector 创建、停止、监控等管理。

设计映射：Gateway 下发期望状态，Agent 管理实例生命周期并上报状态。

需求 5：采集数据进入数据存储。

设计映射：数据链路为 ConnectorInstance -> Agent LocalBuffer -> Gateway -> 数据存储。

需求 6：数据网关负责控制、程序包下发、监控、启停通知。

设计映射：Gateway 控制面负责注册发现、任务编排、程序包管理、启停控制和监控告警。

需求 7：异常情况下保证稳定性，最大可能保证数据不丢。

设计映射：至少一次语义、本地可靠缓冲、未 ACK 重传、Gateway 幂等、ACK 后推进 Checkpoint、背压保护。

需求 8：根据负载动态分发，注意数据完整性。

设计映射：第一版不自动动态分发，预留 assignedAgentId、assignmentVersion、负载指标和迁移状态。

## 21. 风险与边界

需要在设计中明确以下边界：

- 至少一次语义允许重复，不保证严格只写一次
- 业务级去重不由第一版统一保证
- Agent 本地磁盘损坏可能导致未上传数据丢失
- Gateway 是中心关键节点，后续需要补充高可用设计
- 流式 Connector 的恢复能力依赖源端是否支持 offset 或重放
- 嵌入式本地存储引擎会增加 Agent 资源和运维负担
- 第一版不支持自动动态分发和自动任务迁移
- 性能基线尚未定义，需要后续补充 Agent 数量、任务数量、吞吐、延迟和缓冲容量目标

## 22. 总结

第一版逻辑架构可以概括为：

以 Customer 为租户边界，以 Gateway 为中心控制与数据接入入口，以 Agent 为客户现场执行与可靠缓冲节点，以 CollectorTask 为最小控制单元，以 ConnectorInstance 为实际采集进程，通过本地可靠缓冲、批次 ACK、任务级 Checkpoint、Gateway 幂等写入和背压控制，实现至少一次的数据采集与中心化管理。
