# 自动维护可靠性收口与 metadata 测量（2026-11-13）

本文记录 `feat_hooks` 在 `8b51191b35` 审阅之后的修复和证据，不替代长期方案中
尚未落地的阶段。当前执行契约见 [README](README.md)。
没有新增或修改测试用例；复用现有回归。配置仍为
`query.recluster_accumulation_secs`，默认 600，零表示立即具备执行资格。

## 已修复的进度边界

### Refresh 部分提交后失去 replacement

`service/src/interpreters/hook/refresh_hook.rs::execute_refresh_batch` 在执行前设置
`publication_pending`，直到 executor 成功、replacement 及 covered-head bookkeeping
完成才清除。之前，提交后的 catalog/snapshot 读取错误可能使下一次仍寻找已被替换
的旧 segment，并将空计划解释为完成。现在这种不确定状态从当前 snapshot FullCheck，
不沿用 coverage baseline，也不重置失败或无进展预算。

### Recluster 物理 scope 被其他动作替换

`storages/fuse/src/operations/recluster/recluster_table.rs::do_recluster` 比较当前仍存活
的 scope 成员数量。缺失成员意味着物理 scope 和位置游标失效，不能报告 NoParts 收敛。
`interpreter_table_recluster.rs` 将其映射为 ScopeInvalidated，后台 request 丢弃无效
进度、短暂退避后重新冻结，不重新等待已满足的 accumulation window，不重置尝试预算。

普通 append 不使 scope 失效；本 job 自己成功提交的 replacement 继续由原路径扩展
scope。同表 Refresh 虽然不改变数据布局，也会替换 segment，因此必须适用失效规则。
手动 Recluster、claims、锁、OCC 及独立 Refresh/Analyze 需求保持原边界。

## 发布与取消契约

原 I12 的绝对“不取消提交”与有界关闭冲突。本次采用有限排空，而非无限屏蔽取消：

- 自动维护 context 通过共享 RAII guard 标记发布临界区；派生 context 共享同一状态。
- CommitSink 覆盖 metadata 请求及发布 bookkeeping；直接 snapshot publisher 覆盖
  Analyze 等绕过 CommitSink 的路径。进入临界区与收到取消通过同一 mutex 串行化。
- 只延迟协作式 `ABORTED_QUERY`。lease 丢失、OOM、executor 失败不被屏蔽。
- 取消发生在发布之前，仍立即中断；发生在临界区内，先登记，等 guard 释放再中断。
- 执行单元最多等待三秒排空，然后强制取消。节点关闭仍受既有剩余 grace 限制；
  强制停止或进程退出可能发生在请求已提交而响应尚未收到之间。
- 已成功返回并完成 bookkeeping 的发布、确定失败、未知结果必须区别对待。
  未知结果不能证明回滚，不能删除可能已提交的文件，也不能证明覆盖完成。
- 普通前台、同步 Compact 和临时表不进入该保护，不增加其索引构建成本。

这是 context 层的协作式保护，不是对 executor 故障、网络分区或进程退出的不可中断
保证。发布前后精确故障注入、成功提交但响应丢失仍未专项验证，不能用普通取消回归
代替。没有加入持久队列、任务接管或周期补偿。

## metadata 微基准及优化选择

### 方法与限制

独立 Rust 微基准，`rustc 1.101.0-nightly (12433c307 2026-10-24)`，
`--edition=2024 -O`，x86_64 / Intel i7-12700。基准使用两个内存中的
`Vec<(String, u64)>` 表示 source/baseline，名称形如
`123456789/123456789/_sg/00000000-0000-0000-0000-{id}_v4.mpk`，
其中 `id = ordinal.wrapping_mul(0x9e3779b97f4a7c15)` 的十六进制表示，version 为 4。

旧集合路径按生产代码使用 `std::collections::HashSet<&Location>`，五次重建取平均。
优化后的范围算法直接提取生产 `RefreshProgress::initialize_range`，对照完整精确
集合的选中数量。测量与部分编译任务重叠，时间仅用于定位量级，不是隔离性能验收。

原始脚本和日志位于本机 `/tmp/hooks_metadata_{probe,optimized}.rs`、
`/tmp/hooks-metadata-{matched-baseline,optimized}.log`，不提交生成物。
这是内存数据结构测量，**不是实际百万 segment 表的端到端规划或 cache 测量**。
RSS 来自独立进程 `/proc/self/status`，使用其默认 allocator，不代表 query 的 jemalloc、
MemStat 或前台压力下的资源占用。

### 结果

| segment 数 | 旧 baseline 集合单次重建均值 | 旧每 64 项重建的累计外推 | append 新范围初始化 | append 保留位置索引 |
| --- | ---: | ---: | ---: | ---: |
| 10,000 | 0.420 ms | 0.066 s | 0.097 ms | 0 B |
| 100,000 | 4.933 ms | 7.710 s | 2.667 ms | 0 B |
| 1,000,000 | 98.013 ms | 1,531.552 s | 21.251 ms | 0 B |

累计外推只计算反复重建集合，不是实跑全部后台 unit 的耗时，也未计入 IO。
append 场景新范围长度为 1，选中数量与精确集合一致。

百万 segment 的其他形态：

| 形态 | 范围初始化 | 范围内全部 membership 查询 | 保留位置索引 | 选中 / 精确差集 |
| --- | ---: | ---: | ---: | ---: |
| 仅中间一个 replacement | 26.811 ms | 小于 0.001 ms | 8 B | 1 / 1 |
| 首尾 replacement、内部不变 | 476.709 ms | 3,007.147 ms | 8,000,000 B | 2 / 2 |
| 同上但禁用位置索引 | 0.001 ms | 0.895 ms | 0 B | 1,000,000 / 2 |

禁用索引时保守检查全部中间 segment 是预期退化，不能把 0.895 ms 解释为实际
Refresh 更快：随后需要额外 metadata/索引 IO。带索引的独立进程 RSS 约
294,324 KiB，包含两个完整 location 向量、字符串、对照集合和 allocator 保留。
旧百万集合路径 RSS 约 286,504 KiB；不是生产服务的峰值对比。

旧 shared refresh planner 在找到靠前目标后仍扫描全部百万 location，微基准约
24.714 ms；找到后停止约 0.000642 ms。靠后或不存在的目标仍需要 O(N) 定位。
旧 Recluster live/scope 索引构造微基准约 378.908 ms，未在本次优化中改动。

### 采用的局部优化

1. 每代只计算一次共同前缀和后缀，复用已证明覆盖的不可变 location。
2. 未匹配 baseline 区间仅保存排序后的 `usize` 位置，不复制路径或持有执行 context。
   空间至多为单元 query-memory allowance 的 1/64，分配计入维护内存；超额退化为
   保守检查，不使用未预算 spill。它仍可能增加排队进度的驻留内存，不能只看 reservation。
3. baseline 历史失效时丢弃位置索引，不据此确认未验证范围。
4. 所有目标找齐即停止 location 扫描；不是新增整表 location cache。

没有据此宣称已解决 Recluster 每轮 O(N) metadata、snapshot 发布成本、真实 cache
污染、持续混合负载 RSS 或百万 segment 端到端时延。

## 发布 guard 的局部模型检查

临时模型绑定 `af983eb1c2a718396768ad0c3f4f2d0f84d3e5c1`，保存在
`/tmp/hooks-publication-model`，TLC 输出为 `/tmp/hooks-publication-tlc-checked`。
使用固定 SHA-256 为
`e20447644bfabd29d6bd0fc2c0eb1473fb301c1a0e4dfe1961938ce5a191edbf` 的 TLC jar。
模型 SHA-256 为
`c152e593f752048fcae7bca5c9d9895de2038a98e4debf97a45ff9057fe0070c`，
manifest SHA-256 为
`4661fff3bcddc7a0bb2023767e30423b6964f44925f181c6a1d7ca63c1f8e6d4`。
没有将模型、生成配置或 trace 加入仓库。

一到两个 guard、每个最多进入/退出一次的正常安全检查分别穷尽 83、257 个不同状态，
未发现违反局部不变量的状态。两个 mutation 去掉取消登记检查或发布保护，均产生
预期反例；两个 witness 确认以下交错可达：

- Begin → RequestCancel → FinishGuard → ReplayCancel → ApplyKill；
- Begin → RequestCancel → ExpireGrace → ForceCancel → ApplyKill。

每一步对应 `query_ctx_shared.rs` 中的进入、取消、析构及强制取消方法；ExpireGrace
对应执行单元的排空超时。模型在 mutex 释放处拆分，不把请求取消与实际 kill 合并为
不可交错步骤。最初的模型存在 TLA 赋值优先级错误，初版 witness 也过宽；修正后完整
重跑正常、mutation 和 witness，未修改目标安全不变量。

这只检查“取消后不新进入、正常 grace 内不协作式中止活跃发布、fatal/force 不延迟”
的局部协议。没有建模 metadata 内容、RPC 提交和响应丢失、OCC、文件、exchange 故障
或崩溃，不证明数据/索引发布安全，也不证明终止性，不能替代专项原生故障验证。

## 现有回归验证

未新增或修改测试文件。以下已有检查通过：

- `cargo fmt --all -- --check`。
- `cargo clippy -p databend-query -p databend-enterprise-query --lib -- -D warnings`。
- `cargo build -p databend-binaries --bin databend-query`。
- hook 20 项、maintenance 5 项、writer 46 项、Recluster scope/planner 6 项。
- index 集成 19 项、clustering 集成 4 项、EE materialized-view/Vacuum 相关 4 项。
- 原有 stateful 脚本的 Compact、Refresh 覆盖、缺失索引语义、rename/DDL/ID reuse 检查。
- 原有两节点 smoke：两轮自动 Recluster 提交后 NoParts，checksum 保持；分布式
  Recluster 16 项、Compact 29 项 SQL 检查通过。smoke 将 accumulation 配为零以避免
  等待默认十分钟，并非改变默认值。
- 原有分布式取消 smoke：准确终止 coordinator 进程，正常退出耗时 1.969 秒；worker
  mailbox/reservation 归零，400,004 行及 `sum(id)=9,999,800,006` 保持。
  所有服务均通过精确进程句柄停止。

这些用例提供既有行为回归，不能代替本次新边界的专项故障证据。原始日志位于
`/tmp/hooks-reliability-final-*.log`；最终源代码另复跑的 clippy/hook/maintenance 日志为
`/tmp/hooks-reliability-head-*.log`，smoke 日志为
`/tmp/hooks-reliability-{stateful,cluster,cancel-cluster}.log`。
原生代码基线为 `af983eb1c2`，后续文档提交不改变生产代码。大型 workspace 的局部
重编译和 EE feature 重编译影响验证耗时，不代表运行时性能。

## 尚未收口

- 发布前/后、成功提交但响应丢失的专项故障证据仍缺失。
- 真实大表 metadata/cache/RSS 与混合负载验证仍缺失；上述微基准只支持局部优化。
- Vector 保持当前 inline 行为，尚未将“限制线程/内存”当成成本收益结论。是否退出
  inline、退出后如何为 replacement 登记有界且独立的 Refresh，需要单独决策。
- 用户发布说明、完整 ASYNC-index 契约和 DDL FullCheck 入口未在此轮宣称完成。
- 类别配额、压力反馈、独立 spill budget 不在此轮新增范围。

因此，这次是可靠性修复和一轮有限的测量—优化闭环，不是五步路线图全部完成。
