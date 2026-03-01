# Branch 独立目录模型下的 Vacuum/GC 设计（重整版）

## 1. 背景与目标

在新模型中，`branch` 被视为一张逻辑独立表：

1. `branch` 使用独立存储目录，与 base table 并列。
2. `branch` 目录内部结构与 base 一致（snapshot/segment/block）。
3. `branch` 不共享 base 目录。
4. `branch snapshot` 可以引用 base 的 segment/block（copy-on-write）。
5. branch 新写入必须落在 branch 自己目录。

本设计目标：

1. GC 以 namespace 为最小清理单元（base + branch）。
2. 每个 namespace 独立计算 `gc_root_ts` 与 `lvt`。
3. 复用同一套 namespace 清理内核。
4. 保持 `branch-first`：先清 branch，再清 base。
5. branch 阶段产出的外部保留集合统一交给 base 使用。
6. 仅面向新版本布局，不考虑旧布局兼容。

## 2. 术语与模型

### 2.1 Namespace

namespace 是 GC 最小隔离单元：

1. base namespace：`(table_id, base)`
2. branch namespace：`(table_id, branch_id)`

每个 namespace 都有独立目录前缀，可独立 list/mark/sweep。

### 2.2 对象归属（Owner Namespace）

segment/block 的 owner 由对象路径解析得到：

1. `list/sweep` 只扫描当前 namespace 目录。
2. 删除动作只能发生在对象 owner namespace。
3. branch 阶段不会删除外部对象，只收集外部引用用于后续 base 保护。

### 2.3 Namespace 时间边界

每个 namespace 独立维护：

1. `gc_root_ts_ns`：root 截断点（决定保留 snapshot 集）。
2. `lvt_ns`：删除时间上界（仅允许删除 `<= lvt_ns` 对象）。

### 2.4 Roots

namespace roots 来源：

1. namespace 自身满足 time-travel 策略的 snapshot 链。
2. 未过期 tag 指向的 snapshot（按 owner 路由）。
3. 安全窗口内 dropped branch：不做目录 sweep，但仍要展开其 roots 收集外部保留。

## 3. 设计原则与不变量

1. 隔离性：按 namespace 清理，删除权限受 owner 约束。
2. 引用安全：任意 root 可达对象必须保留。
3. branch-first：先 active branch，再 protected dropped branch，最后 base。
4. 幂等性：重复执行不会扩大删除范围。
5. 删除边界：仅删除 `<= lvt_ns` 且不在 keep-set 的对象。
6. 约束：当前模型下 branch 外部引用只需要保护 base 对象。

## 4. 数据结构

```text
NamespaceCtx {
  ns_id
  gc_root_ts_ns
  lvt_ns
}

NamespaceKeep {
  keep_snapshots: Set<SnapshotId>
  keep_segments:  Set<Location>   # owner == ns_id
  keep_blocks:    Set<Location>   # owner == ns_id
}

# 注意：不按 NamespaceId 分桶，统一集合，供 base 清理阶段直接并入。
ExternalKeep {
  segments: Set<Location>         # 从 branch roots 展开得到的外部 segment
  blocks:   Set<Location>         # 从 branch roots 展开得到的外部 block
}

dropped_branches_ready: Vec<BranchId>
```

说明：

1. `ExternalKeep` 不再区分 `NamespaceId`。
2. `ExternalKeepStream` 移除：当前只需要一次性汇总集合给 base 清理即可，额外流式分片结构会增加复杂度。
3. 若后续出现跨 branch/跨 namespace 互引需求，再引入按 owner 分桶结构。

## 5. Vacuum Table 详细流程

### 5.1 阶段 A：元数据预处理

1. 拉取 table 的 base、active branches、tags、dropped branch 状态。
2. 删除已过期 tag；未过期 tag 作为 root 候选。
3. 对 active branch 检查 `expire_at`，到期则转 dropped，记录 `dropped_at`。
4. 形成：`active_branches`、`protected_dropped_branches`、`dropped_branches_ready`。

### 5.2 阶段 B：branch-first 清理（active branch）

对每个 active branch 执行 `vacuum_namespace_core(branch_ns)`：

1. 计算该 branch 的 `NamespaceCtx(gc_root_ts_branch, lvt_branch)`。
2. 组装 branch roots（branch 历史 roots + 路由到该 branch 的 tag roots）。
3. 展开引用链，得到：
   - 本地 keep-set（branch owner）
   - `ExternalKeep`（外部对象集合）
4. 在 branch 自己目录 sweep（`<= lvt_branch` 且不在 keep-set）。
5. 将本 branch 产出的 `ExternalKeep` 合并到 table 级总集合。

### 5.3 阶段 C：保护窗口内 dropped branch 仅做外部保留收集

对每个 `protected_dropped_branch`：

1. 不执行目录 sweep。
2. 通过 `collect_dropped_branch_roots_full_visible_range` 收集其保留 roots（至少包含 head 与 time-travel 保留链）。
3. 只展开引用并提取 `ExternalKeep`，并入 table 级总集合。

### 5.4 阶段 D：base 清理（合并外部保留集）

1. 计算 base 的 `NamespaceCtx(gc_root_ts_base, lvt_base)`。
2. 组装 base roots（base 历史 roots + 路由到 base 的 tag roots）。
3. 执行 `vacuum_namespace_core(base_ns)`。
4. 在 base sweep 前，把 table 级 `ExternalKeep` 直接并入 base keep-set。
5. base 只删除 `<= lvt_base` 且不在 keep-set 的对象。

结果：branch（包含 protected dropped branch）仍可达的 base 对象不会被误删。

### 5.5 阶段 E：清理可物理删除的 dropped branch 目录

对 `dropped_branches_ready`：

1. 二次校验 branch 仍为 dropped 且超过安全窗口。
2. 递归删除 branch namespace 目录。
3. 清理 branch 元数据残留。

### 5.6 `vacuum_namespace_core` 核心算法

`vacuum_namespace_core(ns_ctx, roots_in_ns)` 输入为 namespace 上下文与 roots：

1. 构建 `keep_snapshots`。
2. 展开 `snapshot -> segment -> block`。
3. owner == 当前 namespace：进入本地 keep-set。
4. owner != 当前 namespace：进入 `ExternalKeep`。
5. 对当前 namespace 执行本地 sweep（`<= lvt_ns`）。
6. 返回 `ExternalKeep`。

### 5.7 核心伪代码

```text
def vacuum_namespace_core(ns_ctx, roots_in_ns):
    keep = init_keep()
    external = ExternalKeep()

    keep.keep_snapshots = select_keep_snapshots(roots_in_ns, ns_ctx.gc_root_ts_ns)

    for seg in expand_segments_from_snapshots(keep.keep_snapshots):
        if owner(seg) == ns_ctx.ns_id:
            keep.keep_segments.add(seg)
        else:
            external.segments.add(seg)

        for blk in load_segment(seg).blocks:
            if owner(blk) == ns_ctx.ns_id:
                keep.keep_blocks.add(blk)
            else:
                external.blocks.add(blk)

    sweep_snapshots(ns_ctx.ns_id, keep.keep_snapshots, ns_ctx.lvt_ns)
    sweep_segments(ns_ctx.ns_id, keep.keep_segments, ns_ctx.lvt_ns)
    sweep_blocks(ns_ctx.ns_id, keep.keep_blocks, ns_ctx.lvt_ns)
    return external

external_for_base = ExternalKeep()

for branch in active_branches:
    branch_ctx = build_namespace_ctx(branch)
    branch_roots = collect_branch_roots(branch, branch_ctx.gc_root_ts_ns, tag_roots_by_ns[branch])
    external_for_base.merge(vacuum_namespace_core(branch_ctx, branch_roots))

for branch in protected_dropped_branches:
    roots = collect_dropped_branch_roots_full_visible_range(branch)
    external_for_base.merge(collect_external_keep_only(branch, roots))

base_ctx = build_namespace_ctx(base)
base_roots = collect_base_roots(base, base_ctx.gc_root_ts_ns, tag_roots_by_ns[base])
base_keep = build_base_keep(base_ctx, base_roots)
base_keep.keep_segments.union(external_for_base.segments)
base_keep.keep_blocks.union(external_for_base.blocks)
sweep_base(base_ctx, base_keep)

cleanup_dropped_branch_dirs(dropped_branches_ready)
```

## 6. Ref API 扩展设计（新增 list 能力）

目标：在 `RefApi` 中补齐 `list branch`、`list dropped branch`、`list tag`，实现模式对齐 `TableApi` 的 list 逻辑（目录扫描 + 批量读取 + 过滤）。

### 6.1 新增请求/返回类型（建议）

```text
ListTableTagsReq {
  name_ident: RefNameIdent
  include_expired: bool = false
}

ListTableBranchesReq {
  name_ident: RefNameIdent
  include_expired: bool = false
}

ListDroppedTableBranchesReq {
  name_ident: RefNameIdent
}

ListTableTagItem {
  tag_name: String
  tag: SeqV<TableTag>
}

ListTableBranchItem {
  branch_name: String
  branch: SeqV<TableBranch>
}

ListDroppedTableBranchItem {
  branch_name: String
  branch_id: u64
  dropped_meta: SeqV<TableMeta>
}
```

### 6.2 RefApi trait 新增方法（建议）

```text
async fn list_table_tags(
    &self,
    req: ListTableTagsReq,
) -> Result<Vec<ListTableTagItem>, KVAppError>;

async fn list_table_branches(
    &self,
    req: ListTableBranchesReq,
) -> Result<Vec<ListTableBranchItem>, KVAppError>;

async fn list_dropped_table_branches(
    &self,
    req: ListDroppedTableBranchesReq,
) -> Result<Vec<ListDroppedTableBranchItem>, KVAppError>;
```

### 6.3 实现逻辑（对齐 TableApi 风格）

#### 6.3.1 `list_table_tags`

1. 先 `resolve_table_id()`，保证 table 存在。
2. 前缀 key：`TableIdTagName::new(table_id, "")`。
3. `DirName + ListOptions::unlimited` 扫描 `__fd_table_tag/<table_id>/`。
4. 遍历结果：
   - `include_expired=false` 时过滤 `expire_at <= now`。
   - 输出 `tag_name + SeqV<TableTag>`。
5. 可按 `tag_name` 排序返回。

#### 6.3.2 `list_table_branches`

1. `resolve_table_id()`。
2. 前缀 key：`TableIdBranchName::new(table_id, "")`。
3. 扫描 `__fd_table_branch/<table_id>/`。
4. 过滤过期 branch（逻辑同 `get_table_branch` 的过期判断）。
5. 输出 `branch_name + SeqV<TableBranch>`。

#### 6.3.3 `list_dropped_table_branches`

当前没有“按 base_table_id 直接索引 dropped branch”的单独目录，按以下步骤组合：

1. `resolve_table_id()`。
2. 扫描 `__fd_branch_id_list/<table_id>/`，得到每个 `branch_name -> TableIdList`。
3. 扫描 `__fd_table_branch/<table_id>/`，得到 active branch name 集合。
4. 对每个 history 项：
   - 若 `branch_name` 在 active 集合中，跳过。
   - 取 `TableIdList.last()` 作为当前世代 `branch_id`。
   - 批量读取 `DroppedTableId(branch_id)`（建议 `get_pb_values_vec`）。
   - 存在则输出 dropped branch 项。

该流程与 `TableApi` 的 list/get_history 逻辑一致：

1. 先按目录列 key（`list_pb`/`list_pb_vec`）。
2. 再按 ID 批量拉取实体（`get_pb_values_vec`）。
3. 最后做状态过滤并组装返回。

### 6.4 Vacuum 对 RefApi 的依赖调整

`Vacuum Table` 阶段 A 改为依赖以下接口：

1. `list_table_tags()`：获取并过滤有效 tag roots。
2. `list_table_branches()`：获取 active branch 列表。
3. `list_dropped_table_branches()`：获取 dropped branch 列表及 drop 时间信息。

## 7. Vacuum Drop Table（级联清理）

`VACUUM DROP TABLE` 级联清理 base + branch + tag：

1. 选取超过 drop 保留窗口的 dropped table。
2. 锁定 table 级清理任务，避免并发重复执行。
3. 删除 table 下全部 tag 元数据。
4. 删除全部 branch 元数据（active/dropped）。
5. 删除 table 主元数据中的 branch/tag 索引。
6. 递归删除 base namespace 目录。
7. 递归删除所有 branch namespace 目录。
8. 删除 table tombstone 与 GC 任务记录。

## 8. 风险与后续演进

1. 当前 `ExternalKeep` 采用单集合模型，前提是 branch 外部引用仅指向 base。
2. 若未来支持跨 branch 互引或跨 table 共享对象，需要将 `ExternalKeep` 升级为按 owner 分桶。
3. `list_dropped_table_branches` 依赖 `BranchIdHistoryIdent` + `DroppedTableId` 组合查询，后续可按性能需求增加专用 dropped-branch 索引。
