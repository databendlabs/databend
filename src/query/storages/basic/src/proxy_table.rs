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
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_ast::ast;
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
use databend_common_expression::RemoteExpr;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::EmptySource;
use parking_lot::Mutex;

pub const PROXY_OPT_KEY_TARGETS: &str = "targets";
pub const PROXY_OPT_KEY_DEFAULT: &str = "default";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProxyRoutingModel {
    Statistics,
    Prefix,
}

impl ProxyRoutingModel {
    fn from_setting(value: String) -> Result<Self> {
        match value.to_lowercase().as_str() {
            "statistics" => Ok(Self::Statistics),
            "prefix" => Ok(Self::Prefix),
            _ => Err(ErrorCode::BadArguments(format!(
                "Unsupported PROXY routing model '{}'",
                value
            ))),
        }
    }
}

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
    route_score: StatisticsRouteScore,
    reusable_pruned_metas: Option<ReusablePrunedMetas>,
}

struct PrefixRoutingCandidate {
    target: String,
    table: Arc<dyn Table>,
    score: PrefixRouteScore,
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct PrefixRouteScore {
    matched_prefix: usize,
    equality_prefix: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct StatisticsRouteScore {
    prefix: PrefixRouteScore,
    cluster_key_len: usize,
}

const STATISTICS_PREFIX_COST_TOLERANCE: usize = 4;

#[derive(Default)]
struct PredicateColumns {
    equality: HashSet<String>,
    range: HashSet<String>,
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

        match ProxyRoutingModel::from_setting(ctx.get_settings().get_proxy_routing_model()?)? {
            ProxyRoutingModel::Prefix => {
                return self.select_target_by_prefix(ctx, push_downs).await;
            }
            ProxyRoutingModel::Statistics => {
                self.ensure_statistics_routing_supported(ctx.clone())
                    .await?;
            }
        }

        if self.targets.len() == 1 {
            return self.select_default_target(ctx, push_downs).await;
        }

        let mut selected: Option<RoutingCandidate> = None;
        let mut default_candidate: Option<RoutingCandidate> = None;
        let predicate_columns = predicate_columns(&push_downs);

        for target in &self.targets {
            let Some(candidate) = self
                .estimate_target(ctx.clone(), target, push_downs.clone(), &predicate_columns)
                .await?
            else {
                continue;
            };

            if target == &self.default_target {
                default_candidate = Some(RoutingCandidate {
                    target: candidate.target.clone(),
                    table: candidate.table.clone(),
                    statistics: candidate.statistics.clone(),
                    route_score: candidate.route_score,
                    reusable_pruned_metas: candidate.reusable_pruned_metas.clone(),
                });
            }

            if selected
                .as_ref()
                .is_none_or(|selected| is_better_statistics_candidate(&candidate, selected))
            {
                selected = Some(candidate);
            }
        }

        let selected = selected.ok_or_else(|| self.no_available_target_error())?;

        if let Some(default_candidate) = default_candidate {
            if statistics_cost(&default_candidate.statistics)
                == statistics_cost(&selected.statistics)
                && default_candidate.route_score == selected.route_score
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

    async fn select_target_by_prefix(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<SelectedTarget> {
        let predicate_columns = predicate_columns(&push_downs);
        let mut selected: Option<PrefixRoutingCandidate> = None;
        let mut default_candidate: Option<PrefixRoutingCandidate> = None;

        for target in &self.targets {
            let Some(table) = self.get_target_table(ctx.clone(), target).await? else {
                continue;
            };
            let score = cluster_prefix_score(table.as_ref(), &predicate_columns);
            let candidate = PrefixRoutingCandidate {
                target: target.clone(),
                table,
                score,
            };

            if target == &self.default_target {
                default_candidate = Some(PrefixRoutingCandidate {
                    target: candidate.target.clone(),
                    table: candidate.table.clone(),
                    score: candidate.score,
                });
            }

            if selected
                .as_ref()
                .is_none_or(|selected| candidate.score > selected.score)
            {
                selected = Some(candidate);
            }
        }

        let selected = selected.ok_or_else(|| self.no_available_target_error())?;
        let selected = if let Some(default_candidate) = default_candidate {
            if default_candidate.score == selected.score {
                default_candidate
            } else {
                selected
            }
        } else {
            selected
        };

        self.read_target_partitions_from_table(
            ctx,
            selected.target,
            selected.table,
            push_downs,
            None,
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
        predicate_columns: &PredicateColumns,
    ) -> Result<Option<RoutingCandidate>> {
        let Some(table) = self.get_target_table(ctx.clone(), target).await? else {
            return Ok(None);
        };
        let route_score = statistics_route_score(table.as_ref(), predicate_columns);
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
            route_score,
            reusable_pruned_metas: candidate.reusable_pruned_metas,
        }))
    }

    async fn ensure_statistics_routing_supported(&self, ctx: Arc<dyn TableContext>) -> Result<()> {
        for target in &self.targets {
            let Some(table) = self.get_target_table(ctx.clone(), target).await? else {
                continue;
            };

            if table.is_column_oriented() {
                return Err(ErrorCode::TableOptionInvalid(format!(
                    "PROXY statistics routing currently does not support column-oriented target '{}'; use proxy_routing_model='prefix' or row-oriented targets",
                    target
                )));
            }
        }

        Ok(())
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
        self.validate_target_schema(target, table.get_table_info())?;
        self.validate_target_access(ctx, &target_ref, table.as_ref())
            .await?;
        Ok(Some(table))
    }

    fn validate_target_schema(&self, target: &str, target_table_info: &TableInfo) -> Result<()> {
        if self.table_info.schema().as_ref() != target_table_info.schema().as_ref() {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "PROXY table target '{}' schema must match proxy table '{}' schema",
                target, self.table_info.name
            )));
        }

        if target_table_info.meta.row_access_policy.is_some() {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "PROXY table target '{}' uses legacy row access policy metadata",
                target
            )));
        }

        let target_policy = &target_table_info.meta.row_access_policy_columns_ids;
        let proxy_policy = &self.table_info.meta.row_access_policy_columns_ids;
        // Row-access predicates are bound for the proxy scan during planning.
        // If a target has its own policy, require the proxy to carry the same
        // metadata so those secure filters protect the delegated target read.
        if target_policy.is_some() && target_policy != proxy_policy {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "PROXY table target '{}' row access policy must match proxy table '{}' row access policy",
                target, self.table_info.name
            )));
        }

        Ok(())
    }

    async fn validate_target_access(
        &self,
        ctx: Arc<dyn TableContext>,
        target_ref: &RoutedTarget,
        table: &dyn Table,
    ) -> Result<()> {
        let grant_by_name = GrantObject::Table(
            target_ref.catalog.clone(),
            target_ref.database.clone(),
            target_ref.table.clone(),
        );
        match ctx
            .validate_privilege(&grant_by_name, UserPrivilegeType::Select, false)
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) if error.code() == ErrorCode::PERMISSION_DENIED => {}
            Err(error) => return Err(error),
        }

        let catalog = ctx.get_catalog(&target_ref.catalog).await?;
        let database = catalog
            .get_database(&ctx.get_tenant(), &target_ref.database)
            .await?;
        let db_id = database.get_db_info().database_id.db_id;
        let table_id = table.get_id();

        let ownership = OwnershipObject::Table {
            catalog_name: target_ref.catalog.clone(),
            db_id,
            table_id,
        };
        if ctx.has_ownership(&ownership, false).await? {
            return Ok(());
        }

        let grant_by_id = GrantObject::TableById(target_ref.catalog.clone(), db_id, table_id);
        match ctx
            .validate_privilege(&grant_by_id, UserPrivilegeType::Select, false)
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) if error.code() == ErrorCode::PERMISSION_DENIED => {}
            Err(error) => return Err(error),
        }

        let current_user = ctx.get_current_user()?;
        let roles = ctx
            .get_all_effective_roles()
            .await?
            .iter()
            .map(|role| role.name.clone())
            .collect::<Vec<_>>()
            .join(",");
        Err(ErrorCode::PermissionDenied(format!(
            "Permission denied: privilege [Select] is required on PROXY target table '{}'.'{}'.'{}' for user {} with roles [{}]",
            target_ref.catalog,
            target_ref.database,
            target_ref.table,
            current_user.identity().display(),
            roles
        )))
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

        // FUSE stores the receiver side of lazy pruning inside the table
        // instance used to build the pipeline. A prune-cache hit returns no
        // pipeline after wiring the receiver, so any lazy target plan must
        // reuse this delegated table in read_data.
        if target_plan.parts.partitions_type() == PartInfoType::LazyLevel {
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
        // Lightweight routing results may be reused by the selected target's
        // normal read. Do not let LIMIT truncate the reusable block metas
        // before full pruning has a chance to run.
        push_downs.limit = None;
        push_downs
    })
}

fn predicate_columns(push_downs: &Option<PushDownInfo>) -> PredicateColumns {
    let mut columns = PredicateColumns::default();
    if let Some(push_downs) = push_downs {
        if let Some(filters) = &push_downs.filters {
            collect_predicate_columns(&filters.filter, &mut columns);
        }
        if let Some(filters) = &push_downs.secure_filters {
            collect_predicate_columns(&filters.filter, &mut columns);
        }
    }
    columns
}

fn collect_predicate_columns(expr: &RemoteExpr<String>, columns: &mut PredicateColumns) {
    let RemoteExpr::FunctionCall { id, args, .. } = expr else {
        return;
    };

    let name = id.name();
    if matches!(name.as_ref(), "or" | "or_filters") {
        return;
    }

    if let Some(column) = comparison_predicate_column(name.as_ref(), args) {
        match name.as_ref() {
            "eq" => {
                columns.equality.insert(column);
            }
            "gt" | "gte" | "lt" | "lte" => {
                columns.range.insert(column);
            }
            _ => {}
        }
    }

    for arg in args {
        collect_predicate_columns(arg, columns);
    }
}

fn comparison_predicate_column(name: &str, args: &[RemoteExpr<String>]) -> Option<String> {
    if !matches!(name, "eq" | "gt" | "gte" | "lt" | "lte") || args.len() != 2 {
        return None;
    }

    match (&args[0], &args[1]) {
        (left, right) if is_constant_expr(right) => column_expr_name(left).map(ToString::to_string),
        (left, right) if is_constant_expr(left) => column_expr_name(right).map(ToString::to_string),
        _ => None,
    }
}

fn column_expr_name(expr: &RemoteExpr<String>) -> Option<&str> {
    match expr {
        RemoteExpr::ColumnRef { id, .. } => Some(id.as_str()),
        RemoteExpr::Cast { expr, .. } => column_expr_name(expr),
        _ => None,
    }
}

fn is_constant_expr(expr: &RemoteExpr<String>) -> bool {
    match expr {
        RemoteExpr::Constant { .. } => true,
        RemoteExpr::Cast { expr, .. } => is_constant_expr(expr),
        _ => false,
    }
}

fn cluster_prefix_score(
    table: &dyn Table,
    predicate_columns: &PredicateColumns,
) -> PrefixRouteScore {
    cluster_prefix_score_for_columns(&cluster_key_columns(table), predicate_columns)
}

fn cluster_prefix_score_for_columns<S: AsRef<str>>(
    cluster_key_columns: &[S],
    predicate_columns: &PredicateColumns,
) -> PrefixRouteScore {
    let mut score = PrefixRouteScore::default();
    for column in cluster_key_columns {
        let column = column.as_ref();
        if predicate_columns.equality.contains(column) {
            score.matched_prefix += 1;
            score.equality_prefix += 1;
            continue;
        }
        if predicate_columns.range.contains(column) {
            score.matched_prefix += 1;
        }
        break;
    }
    score
}

fn cluster_key_columns(table: &dyn Table) -> Vec<String> {
    table
        .resolve_cluster_keys()
        .unwrap_or_default()
        .iter()
        .map(simple_cluster_key_column)
        .collect::<Option<Vec<_>>>()
        .unwrap_or_default()
}

fn simple_cluster_key_column(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::ColumnRef { column, .. } if column.table.is_none() => {
            Some(column.column.name().to_string())
        }
        _ => None,
    }
}

fn statistics_cost(statistics: &PartStatistics) -> (usize, usize) {
    (statistics.partitions_scanned, statistics.read_rows)
}

fn statistics_route_score(
    table: &dyn Table,
    predicate_columns: &PredicateColumns,
) -> StatisticsRouteScore {
    statistics_route_score_for_columns(&cluster_key_columns(table), predicate_columns)
}

fn statistics_route_score_for_columns<S: AsRef<str>>(
    cluster_key_columns: &[S],
    predicate_columns: &PredicateColumns,
) -> StatisticsRouteScore {
    let prefix = cluster_prefix_score_for_columns(cluster_key_columns, predicate_columns);
    StatisticsRouteScore {
        prefix,
        cluster_key_len: if prefix.matched_prefix > 0 {
            cluster_key_columns.len()
        } else {
            0
        },
    }
}

fn is_better_statistics_candidate(
    candidate: &RoutingCandidate,
    selected: &RoutingCandidate,
) -> bool {
    is_better_statistics_route(
        &candidate.statistics,
        candidate.route_score,
        &selected.statistics,
        selected.route_score,
    )
}

#[inline]
fn is_better_statistics_route(
    candidate_statistics: &PartStatistics,
    candidate_score: StatisticsRouteScore,
    selected_statistics: &PartStatistics,
    selected_score: StatisticsRouteScore,
) -> bool {
    let candidate_cost = statistics_cost(candidate_statistics);
    let selected_cost = statistics_cost(selected_statistics);
    if candidate_score.prefix > selected_score.prefix
        && cost_within_prefix_tolerance(candidate_cost, selected_cost)
    {
        return true;
    }
    if selected_score.prefix > candidate_score.prefix
        && cost_within_prefix_tolerance(selected_cost, candidate_cost)
    {
        return false;
    }
    candidate_cost < selected_cost
        || (candidate_cost == selected_cost && candidate_score > selected_score)
}

fn cost_within_prefix_tolerance(
    candidate_cost: (usize, usize),
    selected_cost: (usize, usize),
) -> bool {
    if selected_cost.0 == 0 || selected_cost.1 == 0 {
        return candidate_cost == selected_cost;
    }
    candidate_cost.0 <= tolerated_cost(selected_cost.0)
        && candidate_cost.1 <= tolerated_cost(selected_cost.1)
}

fn tolerated_cost(value: usize) -> usize {
    value
        .saturating_mul(STATISTICS_PREFIX_COST_TOLERANCE)
        .max(value.saturating_add(STATISTICS_PREFIX_COST_TOLERANCE))
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

    use databend_common_catalog::plan::Filters;
    use databend_common_catalog::plan::PartitionsShuffleKind;
    use databend_common_expression::FunctionID;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchemaRef;
    use databend_common_expression::TableSchemaRefExt;
    use databend_common_expression::types::DataType;
    use databend_common_meta_app::schema::TableInfo;
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
        proxy_table_info_with_schema(options, TableSchemaRefExt::create(vec![]))
    }

    fn proxy_table_info_with_schema(
        options: BTreeMap<String, String>,
        schema: TableSchemaRef,
    ) -> TableInfo {
        let mut table_info = TableInfo::simple("default", "proxy", schema);
        table_info.meta.engine = "PROXY".to_string();
        table_info.meta.options = options;
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
    fn test_statistics_cost_ignores_read_bytes() {
        let small_file = PartStatistics::new_exact(100, 10, 2, 10);
        let large_file = PartStatistics::new_exact(100, 1000, 2, 10);

        assert_eq!(statistics_cost(&small_file), statistics_cost(&large_file));
    }

    #[test]
    fn test_statistics_route_prefers_much_lower_cost_before_cluster_score() {
        let lower_cost = PartStatistics::new_exact(100, 10, 2, 10);
        let higher_cost = PartStatistics::new_exact(1000, 10, 20, 20);
        let weak_score = StatisticsRouteScore {
            prefix: PrefixRouteScore {
                matched_prefix: 1,
                equality_prefix: 1,
            },
            cluster_key_len: 1,
        };
        let strong_score = StatisticsRouteScore {
            prefix: PrefixRouteScore {
                matched_prefix: 3,
                equality_prefix: 3,
            },
            cluster_key_len: 3,
        };

        assert!(is_better_statistics_route(
            &lower_cost,
            weak_score,
            &higher_cost,
            strong_score
        ));
        assert!(!is_better_statistics_route(
            &higher_cost,
            strong_score,
            &lower_cost,
            weak_score
        ));
    }

    #[test]
    fn test_statistics_route_prefers_stronger_prefix_when_cost_is_close() {
        let weak_cost = PartStatistics::new_exact(708, 10, 3, 10);
        let strong_cost = PartStatistics::new_exact(267, 10, 4, 10);
        let weak_score = StatisticsRouteScore {
            prefix: PrefixRouteScore {
                matched_prefix: 1,
                equality_prefix: 1,
            },
            cluster_key_len: 1,
        };
        let strong_score = StatisticsRouteScore {
            prefix: PrefixRouteScore {
                matched_prefix: 3,
                equality_prefix: 3,
            },
            cluster_key_len: 3,
        };

        assert!(is_better_statistics_route(
            &strong_cost,
            strong_score,
            &weak_cost,
            weak_score
        ));
        assert!(!is_better_statistics_route(
            &weak_cost,
            weak_score,
            &strong_cost,
            strong_score
        ));
    }

    #[test]
    fn test_statistics_route_does_not_override_zero_cost() {
        let zero_cost = PartStatistics::new_exact(0, 0, 0, 10);
        let non_zero_cost = PartStatistics::new_exact(1, 10, 1, 10);
        let weak_score = StatisticsRouteScore {
            prefix: PrefixRouteScore {
                matched_prefix: 1,
                equality_prefix: 1,
            },
            cluster_key_len: 1,
        };
        let strong_score = StatisticsRouteScore {
            prefix: PrefixRouteScore {
                matched_prefix: 3,
                equality_prefix: 3,
            },
            cluster_key_len: 3,
        };

        assert!(!is_better_statistics_route(
            &non_zero_cost,
            strong_score,
            &zero_cost,
            weak_score
        ));
    }

    #[test]
    fn test_statistics_route_breaks_cost_ties_with_cluster_score() {
        let cost = PartStatistics::new_exact(100, 10, 2, 10);
        let trace_chat = StatisticsRouteScore {
            prefix: PrefixRouteScore {
                matched_prefix: 2,
                equality_prefix: 2,
            },
            cluster_key_len: 2,
        };
        let trace_chat_user = StatisticsRouteScore {
            prefix: PrefixRouteScore {
                matched_prefix: 2,
                equality_prefix: 2,
            },
            cluster_key_len: 3,
        };

        assert!(is_better_statistics_route(
            &cost,
            trace_chat_user,
            &cost,
            trace_chat
        ));
        assert!(!is_better_statistics_route(
            &cost,
            trace_chat,
            &cost,
            trace_chat_user
        ));
    }

    #[test]
    fn test_statistics_route_ignores_cluster_len_without_prefix_match() {
        let mut columns = PredicateColumns::default();
        columns.equality.insert("span_id".to_string());
        let trace = statistics_route_score_for_columns(&["trace_id"], &columns);
        let trace_chat_user =
            statistics_route_score_for_columns(&["trace_id", "chat_id", "user_id"], &columns);

        assert_eq!(trace.prefix, PrefixRouteScore::default());
        assert_eq!(trace_chat_user.prefix, PrefixRouteScore::default());
        assert_eq!(trace.cluster_key_len, 0);
        assert_eq!(trace_chat_user.cluster_key_len, 0);
        assert_eq!(trace, trace_chat_user);

        let cost = PartStatistics::new_exact(100, 10, 2, 10);
        assert!(!is_better_statistics_route(
            &cost,
            trace_chat_user,
            &cost,
            trace
        ));
    }

    #[test]
    fn test_lightweight_push_downs_do_not_reuse_limit_truncated_metas() {
        let push_downs = PushDownInfo {
            limit: Some(1),
            ..Default::default()
        };

        let lightweight = lightweight_push_downs(Some(push_downs)).unwrap();

        assert_eq!(
            lightweight.read_partitions_pruning_mode,
            ReadPartitionsPruningMode::Lightweight
        );
        assert_eq!(lightweight.limit, None);
    }

    fn column(name: &str) -> RemoteExpr<String> {
        RemoteExpr::ColumnRef {
            span: None,
            id: name.to_string(),
            data_type: DataType::String,
            display_name: name.to_string(),
        }
    }

    fn string_literal(value: &str) -> RemoteExpr<String> {
        RemoteExpr::Constant {
            span: None,
            scalar: Scalar::String(value.to_string()),
            data_type: DataType::String,
        }
    }

    fn bool_literal(value: bool) -> RemoteExpr<String> {
        RemoteExpr::Constant {
            span: None,
            scalar: Scalar::Boolean(value),
            data_type: DataType::Boolean,
        }
    }

    fn function(name: &str, args: Vec<RemoteExpr<String>>) -> RemoteExpr<String> {
        RemoteExpr::FunctionCall {
            span: None,
            id: Box::new(FunctionID::Builtin {
                name: name.to_string(),
                id: 0,
            }),
            generics: vec![],
            args,
            return_type: DataType::Boolean,
        }
    }

    fn comparison(name: &str, column_name: &str) -> RemoteExpr<String> {
        function(name, vec![column(column_name), string_literal("v")])
    }

    fn filter(expr: RemoteExpr<String>) -> Filters {
        Filters {
            filter: expr,
            inverted_filter: bool_literal(false),
        }
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
    fn test_proxy_prefix_route_predicate_columns() {
        let filters = filter(function("and_filters", vec![
            comparison("eq", "trace_id"),
            comparison("gte", "chat_id"),
            function("or_filters", vec![
                comparison("eq", "user_id"),
                comparison("eq", "span_id"),
            ]),
        ]));
        let push_downs = Some(PushDownInfo {
            filters: Some(filters),
            ..Default::default()
        });

        let columns = predicate_columns(&push_downs);
        assert!(columns.equality.contains("trace_id"));
        assert!(columns.range.contains("chat_id"));
        assert!(!columns.equality.contains("user_id"));
        assert!(!columns.equality.contains("span_id"));
    }

    #[test]
    fn test_proxy_prefix_route_score_uses_leftmost_prefix() {
        let mut columns = PredicateColumns::default();
        columns.equality.insert("trace_id".to_string());
        columns.range.insert("chat_id".to_string());
        columns.equality.insert("user_id".to_string());

        let trace_chat_user =
            cluster_prefix_score_for_columns(&["trace_id", "chat_id", "user_id"], &columns);
        assert_eq!(trace_chat_user.matched_prefix, 2);
        assert_eq!(trace_chat_user.equality_prefix, 1);

        let chat_trace = cluster_prefix_score_for_columns(&["chat_id", "trace_id"], &columns);
        assert_eq!(chat_trace.matched_prefix, 1);
        assert_eq!(chat_trace.equality_prefix, 0);

        let user_trace = cluster_prefix_score_for_columns(&["user_id", "trace_id"], &columns);
        assert_eq!(user_trace.matched_prefix, 2);
        assert_eq!(user_trace.equality_prefix, 2);
        assert!(user_trace > trace_chat_user);
    }

    #[test]
    fn test_proxy_rejects_target_schema_mismatch() -> Result<()> {
        let mut options = BTreeMap::new();
        options.insert(PROXY_OPT_KEY_TARGETS.to_string(), "target".to_string());
        let proxy_schema = TableSchemaRefExt::create(vec![
            TableField::new("a", TableDataType::String),
            TableField::new("b", TableDataType::Boolean),
        ]);
        let table =
            ProxyTable::try_create(proxy_table_info_with_schema(options, proxy_schema.clone()))?;
        let proxy = table.as_any().downcast_ref::<ProxyTable>().unwrap();

        let matching_target = TableInfo::simple("default", "target", proxy_schema);
        proxy.validate_target_schema("target", &matching_target)?;

        let reordered_target = TableInfo::simple(
            "default",
            "target",
            TableSchemaRefExt::create(vec![
                TableField::new("b", TableDataType::Boolean),
                TableField::new("a", TableDataType::String),
            ]),
        );
        assert!(
            proxy
                .validate_target_schema("target", &reordered_target)
                .is_err()
        );

        let different_type_target = TableInfo::simple(
            "default",
            "target",
            TableSchemaRefExt::create(vec![
                TableField::new("a", TableDataType::String),
                TableField::new("b", TableDataType::String),
            ]),
        );
        assert!(
            proxy
                .validate_target_schema("target", &different_type_target)
                .is_err()
        );

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
