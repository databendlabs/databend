// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReadPartitionsPruningMode;
use databend_common_catalog::table::ReusablePrunedMetas;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::EmptySource;
use parking_lot::Mutex;

pub const PROXY_OPT_KEY_TARGETS: &str = "targets";
pub const PROXY_OPT_KEY_DEFAULT: &str = "default";

pub struct ProxyTable {
    table_info: TableInfo,
    targets: Vec<String>,
    default_target: String,
    delegated_target_tables: Mutex<HashMap<DelegatedTableKey, Arc<dyn Table>>>,
}

#[derive(Clone)]
struct RoutedTarget {
    catalog: String,
    database: String,
    table: String,
}

struct SelectedTarget {
    target: String,
    table: Arc<dyn Table>,
    statistics: PartStatistics,
    partitions: Partitions,
    reusable_pruned_metas: Option<ReusablePrunedMetas>,
}

struct RoutingCandidate {
    target: String,
    table: Arc<dyn Table>,
    statistics: PartStatistics,
    reusable_pruned_metas: Option<ReusablePrunedMetas>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DelegatedTableKey {
    query_id: String,
    scan_id: usize,
    table_index: usize,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ProxyPartInfo {
    target: String,
    // Stored only in the leading header partition to avoid cloning full table
    // metadata into every wrapped target partition.
    target_table_info: Option<TableInfo>,
    is_lazy: bool,
    inner: Option<PartInfoPtr>,
}

impl ProxyTable {
    pub fn try_create(table_info: TableInfo) -> Result<Box<dyn Table>> {
        let targets = parse_targets(table_info.options().get(PROXY_OPT_KEY_TARGETS).ok_or_else(
            || {
                ErrorCode::TableOptionInvalid(
                    "PROXY table requires table option targets".to_string(),
                )
            },
        )?)?;

        let default_target = match table_info.options().get(PROXY_OPT_KEY_DEFAULT) {
            Some(default) => normalize_target(default)?,
            None => targets[0].clone(),
        };
        if !targets.iter().any(|target| target == &default_target) {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "PROXY table default target '{}' must be included in targets",
                default_target
            )));
        }

        Ok(Box::new(Self {
            table_info,
            targets,
            default_target,
            delegated_target_tables: Mutex::new(HashMap::new()),
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "PROXY".to_string(),
            comment: "PROXY Storage Engine".to_string(),
            support_cluster_key: false,
        }
    }

    async fn select_target(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<SelectedTarget> {
        if !has_filter(&push_downs) {
            return self.select_default_target(ctx, push_downs).await;
        }

        let mut selected: Option<RoutingCandidate> = None;
        let mut default_candidate: Option<RoutingCandidate> = None;

        for target in &self.targets {
            let Some(candidate) = self
                .estimate_target(ctx.clone(), target, push_downs.clone())
                .await?
            else {
                continue;
            };

            if target == &self.default_target {
                default_candidate = Some(RoutingCandidate {
                    target: candidate.target.clone(),
                    table: candidate.table.clone(),
                    statistics: candidate.statistics.clone(),
                    reusable_pruned_metas: candidate.reusable_pruned_metas.clone(),
                });
            }

            if selected.as_ref().is_none_or(|selected| {
                statistics_cost(&candidate.statistics) < statistics_cost(&selected.statistics)
            }) {
                selected = Some(candidate);
            }
        }

        let selected = selected.ok_or_else(|| self.no_available_target_error())?;

        if let Some(default_candidate) = default_candidate {
            if statistics_cost(&default_candidate.statistics)
                == statistics_cost(&selected.statistics)
            {
                return self
                    .read_target_partitions_from_table(
                        ctx,
                        default_candidate.target,
                        default_candidate.table,
                        push_downs,
                        default_candidate.reusable_pruned_metas,
                    )
                    .await;
            }
        }

        self.read_target_partitions_from_table(
            ctx,
            selected.target,
            selected.table,
            push_downs,
            selected.reusable_pruned_metas,
        )
        .await
    }

    async fn select_default_target(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<SelectedTarget> {
        if let Some(selected) = self
            .read_target_partitions(ctx.clone(), &self.default_target, push_downs.clone())
            .await?
        {
            return Ok(selected);
        }

        for target in &self.targets {
            if target == &self.default_target {
                continue;
            }
            if let Some(selected) = self
                .read_target_partitions(ctx.clone(), target, push_downs.clone())
                .await?
            {
                return Ok(selected);
            }
        }

        Err(self.no_available_target_error())
    }

    async fn read_target_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        target: &str,
        push_downs: Option<PushDownInfo>,
    ) -> Result<Option<SelectedTarget>> {
        let Some(table) = self.get_target_table(ctx.clone(), target).await? else {
            return Ok(None);
        };
        self.read_target_partitions_from_table(ctx, target.to_string(), table, push_downs, None)
            .await
            .map(Some)
    }

    async fn estimate_target(
        &self,
        ctx: Arc<dyn TableContext>,
        target: &str,
        push_downs: Option<PushDownInfo>,
    ) -> Result<Option<RoutingCandidate>> {
        let Some(table) = self.get_target_table(ctx.clone(), target).await? else {
            return Ok(None);
        };
        let candidate = self
            .read_target_partitions_from_table(
                ctx,
                target.to_string(),
                table,
                lightweight_push_downs(push_downs),
                None,
            )
            .await?;

        Ok(Some(RoutingCandidate {
            target: candidate.target,
            table: candidate.table,
            statistics: candidate.statistics,
            reusable_pruned_metas: candidate.reusable_pruned_metas,
        }))
    }

    async fn get_target_table(
        &self,
        ctx: Arc<dyn TableContext>,
        target: &str,
    ) -> Result<Option<Arc<dyn Table>>> {
        let target_ref = self.resolve_target(ctx.clone(), target)?;
        let table = match ctx
            .get_table(&target_ref.catalog, &target_ref.database, &target_ref.table)
            .await
        {
            Ok(table) => table,
            Err(error) if Self::is_unavailable_target_error(&error) => {
                log::warn!(
                    "PROXY table target '{}' is not available and will be skipped: {}",
                    target,
                    error
                );
                return Ok(None);
            }
            Err(error) => return Err(error),
        };

        if !table.engine().eq_ignore_ascii_case("FUSE") {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "PROXY table target '{}' must use FUSE engine, but got {}",
                target,
                table.engine()
            )));
        }
        Ok(Some(table))
    }

    async fn read_target_partitions_from_table(
        &self,
        ctx: Arc<dyn TableContext>,
        target: String,
        table: Arc<dyn Table>,
        push_downs: Option<PushDownInfo>,
        reusable_pruned_metas: Option<ReusablePrunedMetas>,
    ) -> Result<SelectedTarget> {
        let settings = ctx.get_settings();
        let distributed_pruning_enabled = settings.get_enable_distributed_pruning()?;
        if distributed_pruning_enabled {
            settings.set_setting("enable_distributed_pruning".to_string(), "0".to_string())?;
        }

        // PROXY forwards pushdowns to the target FUSE table, so target pruning
        // decisions, including TABLESAMPLE, are reflected in these partitions.
        let read_res = table
            .read_partitions_with_reusable_pruned_metas(
                ctx,
                push_downs,
                false,
                reusable_pruned_metas,
            )
            .await;

        if distributed_pruning_enabled {
            settings.set_setting("enable_distributed_pruning".to_string(), "1".to_string())?;
        }

        let (statistics, partitions, reusable_pruned_metas) = read_res?;

        Ok(SelectedTarget {
            target,
            table,
            statistics,
            partitions,
            reusable_pruned_metas,
        })
    }

    fn is_unavailable_target_error(error: &ErrorCode) -> bool {
        matches!(
            error.code(),
            ErrorCode::UNKNOWN_TABLE | ErrorCode::UNKNOWN_TABLE_ID
        )
    }

    fn no_available_target_error(&self) -> ErrorCode {
        ErrorCode::TableOptionInvalid(format!(
            "PROXY table '{}' has no available target table",
            self.table_info.name
        ))
    }

    fn resolve_target(&self, ctx: Arc<dyn TableContext>, target: &str) -> Result<RoutedTarget> {
        let parts = target
            .split('.')
            .map(|part| part.trim())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();

        let current_catalog = self.table_info.catalog().to_string();
        let current_database =
            database_from_desc(&self.table_info.desc).unwrap_or_else(|| ctx.get_current_database());

        let target_ref = match parts.as_slice() {
            [table] => RoutedTarget {
                catalog: current_catalog,
                database: current_database.clone(),
                table: (*table).to_string(),
            },
            [database, table] => RoutedTarget {
                catalog: current_catalog,
                database: (*database).to_string(),
                table: (*table).to_string(),
            },
            [catalog, database, table] => RoutedTarget {
                catalog: (*catalog).to_string(),
                database: (*database).to_string(),
                table: (*table).to_string(),
            },
            _ => {
                return Err(ErrorCode::TableOptionInvalid(format!(
                    "Invalid PROXY table target '{}'",
                    target
                )));
            }
        };

        if target_ref.catalog == self.table_info.catalog()
            && target_ref.database == current_database
            && target_ref.table == self.table_info.name
        {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "PROXY table '{}' cannot target itself",
                self.table_info.name
            )));
        }

        Ok(target_ref)
    }

    fn wrap_partitions(
        &self,
        target: &str,
        target_table_info: TableInfo,
        partitions: Partitions,
    ) -> Partitions {
        let is_lazy = partitions
            .partitions
            .first()
            .is_some_and(|part| part.part_type() == PartInfoType::LazyLevel);
        let mut wrapped_parts = Vec::with_capacity(partitions.partitions.len() + 1);
        wrapped_parts.push(Arc::new(Box::new(ProxyPartInfo {
            target: target.to_string(),
            target_table_info: Some(target_table_info),
            is_lazy,
            inner: None,
        }) as Box<dyn PartInfo>));

        wrapped_parts.extend(partitions.partitions.into_iter().map(|part| {
            let is_lazy = part.part_type() == PartInfoType::LazyLevel;
            Arc::new(Box::new(ProxyPartInfo {
                target: target.to_string(),
                target_table_info: None,
                is_lazy,
                inner: Some(part),
            }) as Box<dyn PartInfo>)
        }));

        Partitions::create(partitions.kind, wrapped_parts)
    }

    fn unwrap_partitions(
        &self,
        partitions: &Partitions,
    ) -> Result<(String, TableInfo, Partitions)> {
        let mut target = None;
        let mut target_table_info: Option<TableInfo> = None;
        let mut inner_parts = Vec::with_capacity(partitions.partitions.len());

        for part in &partitions.partitions {
            let proxy_part = part
                .as_any()
                .downcast_ref::<ProxyPartInfo>()
                .ok_or_else(|| {
                    ErrorCode::Internal(
                        "PROXY table expected partitions produced by PROXY read_partitions"
                            .to_string(),
                    )
                })?;

            match &target {
                Some(target) if target != &proxy_part.target => {
                    return Err(ErrorCode::Internal(
                        "PROXY table partitions contain multiple targets".to_string(),
                    ));
                }
                None => target = Some(proxy_part.target.clone()),
                _ => {}
            }
            if let Some(info) = &proxy_part.target_table_info {
                if let Some(existing) = &target_table_info {
                    if existing.ident != info.ident {
                        return Err(ErrorCode::Internal(
                            "PROXY table partitions contain multiple target table infos"
                                .to_string(),
                        ));
                    }
                } else {
                    target_table_info = Some(info.clone());
                }
            }
            if let Some(inner) = &proxy_part.inner {
                inner_parts.push(inner.clone());
            }
        }

        let target_table_info = target_table_info.ok_or_else(|| {
            ErrorCode::Internal("PROXY table expected target table info in partitions".to_string())
        })?;

        Ok((
            target.unwrap_or_else(|| self.default_target.clone()),
            target_table_info,
            Partitions::create(partitions.kind.clone(), inner_parts),
        ))
    }

    fn build_target_plan(&self, plan: &DataSourcePlan) -> Result<(String, DataSourcePlan)> {
        let (target, target_table_info, partitions) = self.unwrap_partitions(&plan.parts)?;
        let mut target_plan = plan.clone();
        target_plan.source_info = DataSourceInfo::TableSource(target_table_info);
        target_plan.parts = partitions;
        Ok((target, target_plan))
    }

    fn take_delegated_target_table(
        &self,
        ctx: Arc<dyn TableContext>,
        target_plan: &DataSourcePlan,
    ) -> Option<Arc<dyn Table>> {
        let DataSourceInfo::TableSource(target_table_info) = &target_plan.source_info else {
            return None;
        };

        let delegated_table = self
            .delegated_target_tables
            .lock()
            .remove(&Self::delegated_table_key(ctx, target_plan))?;
        if delegated_table.get_table_info().ident == target_table_info.ident {
            Some(delegated_table)
        } else {
            None
        }
    }

    fn store_delegated_target_table(
        &self,
        ctx: Arc<dyn TableContext>,
        target_plan: &DataSourcePlan,
        table: Arc<dyn Table>,
    ) {
        self.delegated_target_tables
            .lock()
            .insert(Self::delegated_table_key(ctx, target_plan), table);
    }

    fn delegated_table_key(ctx: Arc<dyn TableContext>, plan: &DataSourcePlan) -> DelegatedTableKey {
        DelegatedTableKey {
            query_id: ctx.get_id(),
            scan_id: plan.scan_id,
            table_index: plan.table_index,
        }
    }
}

#[async_trait::async_trait]
impl Table for ProxyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn support_column_projection(&self) -> bool {
        true
    }

    fn use_own_sample_block(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let candidate = self.select_target(ctx, push_downs).await?;
        let partitions = self.wrap_partitions(
            &candidate.target,
            candidate.table.get_table_info().clone(),
            candidate.partitions,
        );

        Ok((candidate.statistics, partitions))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        if plan.parts.is_empty() {
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        }

        let (_target, target_plan) = self.build_target_plan(plan)?;
        ctx.set_partitions(target_plan.parts.clone())?;

        let table = self
            .take_delegated_target_table(ctx.clone(), &target_plan)
            .map(Ok)
            .unwrap_or_else(|| ctx.build_table_from_source_plan(&target_plan))?;
        table.read_data(ctx, &target_plan, pipeline, put_cache)
    }

    fn build_prune_pipeline(
        &self,
        table_ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        source_pipeline: &mut Pipeline,
        plan_id: u32,
    ) -> Result<Option<Pipeline>> {
        if plan.parts.is_empty() {
            return Ok(None);
        }

        let (_target, target_plan) = self.build_target_plan(plan)?;
        let table = table_ctx.build_table_from_source_plan(&target_plan)?;
        let prune_pipeline = table.build_prune_pipeline(
            table_ctx.clone(),
            &target_plan,
            source_pipeline,
            plan_id,
        )?;

        // FUSE stores the receiver side of a lazy pruning pipeline inside the
        // table instance used to build that pipeline. Reuse the same delegated
        // target in read_data so lazy segment partitions can dispatch blocks.
        if prune_pipeline.is_some() {
            self.store_delegated_target_table(table_ctx, &target_plan, table);
        }

        Ok(prune_pipeline)
    }
}

#[typetag::serde(name = "proxy")]
impl PartInfo for ProxyPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<ProxyPartInfo>()
            .is_some_and(|other| {
                self.target == other.target
                    && self.target_table_info.as_ref().map(|info| info.ident)
                        == other.target_table_info.as_ref().map(|info| info.ident)
                    && self.is_lazy == other.is_lazy
                    && match (&self.inner, &other.inner) {
                        (Some(left), Some(right)) => left.equals(right),
                        (None, None) => true,
                        _ => false,
                    }
            })
    }

    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.target.hash(&mut hasher);
        self.target_table_info
            .as_ref()
            .map(|info| info.ident.table_id)
            .hash(&mut hasher);
        self.is_lazy.hash(&mut hasher);
        self.inner
            .as_ref()
            .map(|inner| inner.hash())
            .hash(&mut hasher);
        hasher.finish()
    }

    fn part_type(&self) -> PartInfoType {
        if self.is_lazy {
            PartInfoType::LazyLevel
        } else {
            PartInfoType::BlockLevel
        }
    }

    fn is_reshuffle_header(&self) -> bool {
        self.target_table_info.is_some() && self.inner.is_none()
    }
}

fn parse_targets(value: &str) -> Result<Vec<String>> {
    let targets = value
        .split(',')
        .map(normalize_target)
        .collect::<Result<Vec<_>>>()?;

    if targets.is_empty() {
        return Err(ErrorCode::TableOptionInvalid(
            "PROXY table option targets cannot be empty".to_string(),
        ));
    }

    Ok(targets)
}

fn normalize_target(value: &str) -> Result<String> {
    let target = value.trim();
    if target.is_empty() {
        return Err(ErrorCode::TableOptionInvalid(
            "PROXY table target cannot be empty".to_string(),
        ));
    }
    Ok(target.to_string())
}

fn has_filter(push_downs: &Option<PushDownInfo>) -> bool {
    push_downs.as_ref().is_some_and(|push_downs| {
        push_downs.filters.is_some() || push_downs.secure_filters.is_some()
    })
}

fn lightweight_push_downs(push_downs: Option<PushDownInfo>) -> Option<PushDownInfo> {
    push_downs.map(|mut push_downs| {
        push_downs.read_partitions_pruning_mode = ReadPartitionsPruningMode::Lightweight;
        push_downs
    })
}

fn statistics_cost(statistics: &PartStatistics) -> (usize, usize, usize) {
    (
        statistics.partitions_scanned,
        statistics.read_rows,
        statistics.read_bytes,
    )
}

fn database_from_desc(desc: &str) -> Option<String> {
    let raw = desc.split('.').next()?;
    raw.strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use databend_common_catalog::plan::PartitionsShuffleKind;
    use databend_common_expression::TableSchemaRefExt;
    use databend_common_meta_app::schema::TableInfo;
    use databend_common_meta_app::schema::TableMeta;
    use databend_meta_client::types::NodeInfo;

    use super::*;
    use crate::RandomPartInfo;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct LazyTestPartInfo;

    #[typetag::serde(name = "proxy_lazy_test")]
    impl PartInfo for LazyTestPartInfo {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
            info.as_any().is::<LazyTestPartInfo>()
        }

        fn hash(&self) -> u64 {
            0
        }

        fn part_type(&self) -> PartInfoType {
            PartInfoType::LazyLevel
        }
    }

    fn proxy_table_info(options: BTreeMap<String, String>) -> TableInfo {
        let mut table_info =
            TableInfo::simple("default", "proxy", TableSchemaRefExt::create(vec![]));
        table_info.meta = TableMeta {
            engine: "PROXY".to_string(),
            options,
            ..Default::default()
        };
        table_info
    }

    fn create_node(id: &str) -> Arc<NodeInfo> {
        Arc::new(NodeInfo::create(
            id.to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            id.to_string(),
        ))
    }

    #[test]
    fn test_proxy_table_options() -> Result<()> {
        let mut options = BTreeMap::new();
        options.insert(
            PROXY_OPT_KEY_TARGETS.to_string(),
            "spans_by_trace, spans_by_chat".to_string(),
        );
        options.insert(
            PROXY_OPT_KEY_DEFAULT.to_string(),
            "spans_by_chat".to_string(),
        );

        let table = ProxyTable::try_create(proxy_table_info(options))?;
        let proxy = table.as_any().downcast_ref::<ProxyTable>().unwrap();
        assert_eq!(proxy.targets, vec!["spans_by_trace", "spans_by_chat"]);
        assert_eq!(proxy.default_target, "spans_by_chat");

        Ok(())
    }

    #[test]
    fn test_proxy_empty_partition_keeps_target_info() -> Result<()> {
        let mut options = BTreeMap::new();
        options.insert(PROXY_OPT_KEY_TARGETS.to_string(), "target".to_string());
        let table = ProxyTable::try_create(proxy_table_info(options))?;
        let proxy = table.as_any().downcast_ref::<ProxyTable>().unwrap();
        let target_info = TableInfo::simple("default", "target", TableSchemaRefExt::create(vec![]));

        let wrapped = proxy.wrap_partitions(
            "target",
            target_info.clone(),
            Partitions::create(PartitionsShuffleKind::Seq, vec![]),
        );
        let (target, unwrapped_info, unwrapped) = proxy.unwrap_partitions(&wrapped)?;

        assert_eq!(target, "target");
        assert_eq!(unwrapped_info.name, target_info.name);
        assert!(unwrapped.partitions.is_empty());

        let wrapped = proxy.wrap_partitions(
            "target",
            target_info,
            Partitions::create(PartitionsShuffleKind::Seq, vec![RandomPartInfo::create(1)]),
        );
        let (_, _, unwrapped) = proxy.unwrap_partitions(&wrapped)?;
        assert_eq!(unwrapped.partitions.len(), 1);

        Ok(())
    }

    #[test]
    fn test_proxy_partition_wraps_table_info_once() -> Result<()> {
        let mut options = BTreeMap::new();
        options.insert(PROXY_OPT_KEY_TARGETS.to_string(), "target".to_string());
        let table = ProxyTable::try_create(proxy_table_info(options))?;
        let proxy = table.as_any().downcast_ref::<ProxyTable>().unwrap();
        let target_info = TableInfo::simple("default", "target", TableSchemaRefExt::create(vec![]));

        let wrapped = proxy.wrap_partitions(
            "target",
            target_info,
            Partitions::create(PartitionsShuffleKind::Seq, vec![
                RandomPartInfo::create(1),
                RandomPartInfo::create(2),
            ]),
        );

        let table_info_parts = wrapped
            .partitions
            .iter()
            .filter(|part| {
                part.as_any()
                    .downcast_ref::<ProxyPartInfo>()
                    .is_some_and(|part| part.target_table_info.is_some())
            })
            .count();

        assert_eq!(wrapped.partitions.len(), 3);
        assert_eq!(table_info_parts, 1);

        let (_, _, unwrapped) = proxy.unwrap_partitions(&wrapped)?;
        assert_eq!(unwrapped.partitions.len(), 2);

        Ok(())
    }

    #[test]
    fn test_proxy_partition_reshuffle_keeps_target_info_per_executor() -> Result<()> {
        let mut options = BTreeMap::new();
        options.insert(PROXY_OPT_KEY_TARGETS.to_string(), "target".to_string());
        let table = ProxyTable::try_create(proxy_table_info(options))?;
        let proxy = table.as_any().downcast_ref::<ProxyTable>().unwrap();
        let target_info = TableInfo::simple("default", "target", TableSchemaRefExt::create(vec![]));

        let wrapped = proxy.wrap_partitions(
            "target",
            target_info.clone(),
            Partitions::create(PartitionsShuffleKind::Seq, vec![
                RandomPartInfo::create(1),
                RandomPartInfo::create(2),
            ]),
        );
        let executors = vec![
            create_node("node-1"),
            create_node("node-2"),
            create_node("node-3"),
        ];
        let shuffled = wrapped.reshuffle(executors.clone())?;

        let mut non_empty_executors = 0;
        let mut inner_partitions = 0;
        for executor in executors {
            let partitions = shuffled.get(&executor.id).unwrap();
            if partitions.partitions.is_empty() {
                continue;
            }

            non_empty_executors += 1;
            let table_info_parts = partitions
                .partitions
                .iter()
                .filter(|part| {
                    part.as_any()
                        .downcast_ref::<ProxyPartInfo>()
                        .is_some_and(|part| part.target_table_info.is_some())
                })
                .count();
            assert_eq!(table_info_parts, 1);

            let (target, unwrapped_info, unwrapped) = proxy.unwrap_partitions(partitions)?;
            assert_eq!(target, "target");
            assert_eq!(unwrapped_info.name, target_info.name);
            assert!(!unwrapped.partitions.is_empty());
            inner_partitions += unwrapped.partitions.len();
        }

        assert_eq!(non_empty_executors, 2);
        assert_eq!(inner_partitions, 2);

        Ok(())
    }

    #[test]
    fn test_proxy_partition_keeps_lazy_part_type() -> Result<()> {
        let mut options = BTreeMap::new();
        options.insert(PROXY_OPT_KEY_TARGETS.to_string(), "target".to_string());
        let table = ProxyTable::try_create(proxy_table_info(options))?;
        let proxy = table.as_any().downcast_ref::<ProxyTable>().unwrap();
        let target_info = TableInfo::simple("default", "target", TableSchemaRefExt::create(vec![]));

        let wrapped = proxy.wrap_partitions(
            "target",
            target_info,
            Partitions::create(PartitionsShuffleKind::Seq, vec![Arc::new(
                Box::new(LazyTestPartInfo) as Box<dyn PartInfo>,
            )]),
        );

        assert!(matches!(wrapped.partitions_type(), PartInfoType::LazyLevel));

        Ok(())
    }

    #[test]
    fn test_proxy_uses_target_sampled_partitions() -> Result<()> {
        let mut options = BTreeMap::new();
        options.insert(PROXY_OPT_KEY_TARGETS.to_string(), "target".to_string());
        let table = ProxyTable::try_create(proxy_table_info(options))?;

        assert!(table.use_own_sample_block());

        Ok(())
    }
}
