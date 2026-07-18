# `OPTIMIZE TABLE ... REFRESH BLOCKS` 设计方案

## 1. SQL 与语义

新增：

```sql
OPTIMIZE TABLE [<catalog>.][<database>.]<table>
REFRESH BLOCKS
[WHERE <predicate>]
[LIMIT <number_of_blocks>];
```

Granule Bloom Index 回填示例：

```sql
CREATE BLOOM INDEX idx_user_id ON t(user_id);

OPTIMIZE TABLE t REFRESH BLOCKS
WHERE NOT block_has_index('idx_user_id')
LIMIT 1000;
```

`REFRESH BLOCKS` 对 pruning 选中的 Fuse block 进行严格一对一刷新：

```text
1 source block
-> 完整读取全部物理列和全部行
-> 保持 row order
-> TransformSerializeBlock
-> 1 refreshed block
```

保证：

- 逻辑数据、row count 和 row order 不变；
- 不合并或拆分 block；
- 不执行 compact 或 recluster；
- 使用表当前的统一写入配置重新生成 Parquet data block、Granule Index 和适用的同步索引。

`WHERE` 只选择需要刷新的完整 block，不过滤 block 内的 rows。Pruner 产生的 page/granule ranges 不用于 refresh data read。

`LIMIT N` 限制 pruning 最多选出并尝试刷新的 source block 数，不是 row 数或成功 commit 数。并发 mutation 导致的 skipped block 仍消耗 `LIMIT`，不再补选。

## 2. Pruning 与 `block_has_index()`

直接复用当前 Query/Mutation 的 Fuse pruning 路径，包括：

- lazy segment pruning；
- block range pruning；
- Bloom/index pruning；
- internal-column pruning；
- distributed pruning 和 bounded `PartInfo` channel。

普通数据谓词沿用现有 pruning 语义，允许 false positives；被 pruner 保留的 block 始终完整刷新。

新增仅用于 block pruning 的函数：

```sql
block_has_index('<index_name>')
```

参数是 `CREATE INDEX` 指定的 index name，而不是 index type。Binder 根据当前 table metadata 将它解析并冻结为：

```rust
pub struct BlockIndexIdentity {
    pub name: String,
    pub index_type: TableIndexType,
    pub version: String,
    pub column_ids: Vec<ColumnId>,
}
```

规则：

- 参数必须是常量字符串；
- index 不存在时直接报错，避免拼写错误导致全表刷新；
- 判断 block 是否具有该 named index 的当前 definition/version；
- 同名 index 的旧 version 返回 `false`；
- 只检查 `BlockMeta`，不执行 object storage `stat`；
- 不作为 row-level scalar function 执行。

第一阶段支持同步 Bloom Index：根据当前 index version 和 column IDs 检查对应 Granule Bloom marks。历史 block 缺少相关 metadata 时返回 `false`。

## 3. Streaming Distributed Pipeline

语句规划时，从初始 snapshot 生成固定的 lazy segment references。它们定义本条语句的有限扫描范围，因此不会再次扫描本语句生成的新 block，也不会追赶并发 INSERT。

```text
initial snapshot lazy segments
-> existing streaming Fuse pruning
-> bounded RefreshBlockPart channel
-> distributed full-block read
-> TransformSerializeBlock
-> write refreshed block and index objects
-> Exchange Merge RefreshBlockResult
-> coordinator RefreshBlocksCommitSink
```

不预先物化全量候选，不需要外层 cursor、processed-location set 或 interpreter 多轮重建 pipeline。

Part 和 worker result：

```rust
pub struct RefreshBlockPart {
    pub source_segment_location: Location,
    pub source_segment_index: usize,
    pub source_block_index: usize,
    pub source_block_meta: Arc<BlockMeta>,
}

pub struct RefreshBlockResult {
    pub source_block_meta: Arc<BlockMeta>,
    pub refreshed_block_meta: Arc<ExtendedBlockMeta>,
}
```

Worker 必须完整读取一个物理 source block，不应用 `WHERE`、projection、page ranges 或 Granule ranges。Exchange 只传递 result metadata，不传递完整 rows。

复用 `TransformSerializeBlock` 的 `BlockBuilder`/`BlockWriter`，但不复用当前以 `BlockMetaIndex` 为身份的 `MutationLogEntry::ReplacedBlock`。新增 `MutationKind::RefreshBlocks`，并输出 `RefreshBlockResult`。

Refresh 保留 source cluster stats 和 row order。只有 source block 与当前 linear cluster key 匹配时才生成对应 sparse mins；旧 cluster key、无 cluster stats 或 Hilbert block 不生成当前 linear sparse mins。

## 4. Pipeline 内部分批 Commit

新增专用 `RefreshBlocksCommitSink`。它在同一个 pipeline 中持续消费 results，达到内部 batch threshold 后 commit 一个 snapshot，清空 batch，再继续消费。

```text
collect batch
-> commit snapshot
-> clear batch
-> continue consuming
-> flush final partial batch
```

内部 batch size 是执行配置，不改变 SQL `LIMIT` 语义。当前通用 `CommitSink` 只支持一次最终 commit，不直接修改它。

Pipeline 中途失败时：

- 已成功 commit 的 batches 保留；
- 当前 pending batch 不提交；
- error path 不调用 final flush。

## 5. 无锁 OCC 与冲突处理

`REFRESH BLOCKS` 不获取 table lock。每个 batch 在 coordinator 上：

1. 读取 latest snapshot；
2. 以 `source_block_meta.location` 作为 source block 的稳定身份；
3. source location 仍存在时，在其最新 segment/position 上替换为 refreshed `BlockMeta`；
4. source location 已不存在时，说明已被 UPDATE、DELETE、COMPACT、RECLUSTER 或其他 refresh 消费，跳过该 result；
5. 重写受影响的 segment metadata；
6. CAS commit 新 snapshot。

source segment location 和 `(segment_index, block_index)` 只作为定位 hint，不能作为冲突正确性依据。

发生 `TABLE_VERSION_MISMATCHED` 时：

```text
refresh latest snapshot
-> 复用已有 RefreshBlockResult 重新定位 source locations
-> 重新生成 replacement segments
-> 再次 CAS commit
```

不重新执行 pruning、source read 或 block serialization。若整个 batch 的 source locations 都已消失，则记录 skipped 并跳过空 snapshot commit。

旧对象绝不原地覆盖；refreshed block 使用新的 object location，保证历史 snapshot 和 time travel 可继续读取。

## 6. 主要改动与验证

主要新增或扩展：

```text
OptimizeTableAction::RefreshBlocks
OptimizeRefreshBlocksPlan
OptimizeRefreshBlocksInterpreter
MutationKind::RefreshBlocks
block_has_index('<index_name>')
RefreshBlockPart
RefreshBlockResult
RefreshBlocksCommitSink
```

关键测试：

- parser、display、权限和 `LIMIT 0`；
- `block_has_index()` 的 current/old version、不存在 name 和历史 block；
- 普通谓词 pruning、false positives 及完整 block read；
- 一对一 block count、row count、row order 和查询结果不变；
- distributed pruning、worker rewrite 和 metadata-only exchange；
- 一个 pipeline 内多次独立 commit；
- 并发 INSERT、UPDATE/DELETE、COMPACT、RECLUSTER 和另一个 refresh；
- CAS retry 只重做 metadata reconciliation；
- 第 N 个 batch 失败后，之前 batches 保留且 pending batch 未提交；
- change tracking 不产生逻辑 INSERT/DELETE 记录。

GC 和 orphan 回收策略不在本方案中展开，后续单独讨论。已提交 refresh 的旧 block 仍可通过历史 snapshot 触达。
