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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::IdentifierType;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableReference;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::is_cacheable_function;
use databend_common_meta_app::schema::SecurityPolicyColumnMap;
use databend_common_meta_app::schema::TableMeta;
use databend_common_settings::ChangeValue;
use databend_meta_client::types::MetaId;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheValue;
use databend_storages_common_cache::InMemoryLruCache;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use derive_visitor::Drive;
use derive_visitor::Visitor;
use itertools::Itertools;
use parking_lot::RwLock;
use sha2::Digest;
use sha2::Sha256;

use crate::NameResolutionContext;
use crate::Planner;
use crate::TableEntry;
use crate::normalize_identifier;
use crate::planner::planner_cache_parameter::ParameterizedStatement;
use crate::planner::planner_cache_parameter::instantiate_plan;
use crate::plans::Plan;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlanCacheEntryKind {
    BoundTemplate { parameter_count: usize },
    Optimized,
}

#[derive(Clone)]
pub struct PlanCacheItem {
    pub(crate) plan: Plan,
    pub(crate) setting_changes: Vec<(String, ChangeValue)>,
    pub(crate) variables: HashMap<String, Scalar>,
    kind: PlanCacheEntryKind,
    ddl_generation: u64,
    table_generations: Vec<(MetaId, u64)>,
}

pub const DEFAULT_PLANNER_CACHE_MAX_BYTES: usize = 512 * 1024 * 1024;

static PLAN_CACHE: LazyLock<InMemoryLruCache<PlanCacheItem>> = LazyLock::new(|| {
    InMemoryLruCache::with_bytes_capacity(
        "planner_cache".to_string(),
        DEFAULT_PLANNER_CACHE_MAX_BYTES,
    )
});
static PLAN_CACHE_DDL_GENERATION: AtomicU64 = AtomicU64::new(0);
static PLAN_CACHE_MUTATION_EPOCH: AtomicU64 = AtomicU64::new(0);
static PLAN_CACHE_TABLE_GENERATIONS: LazyLock<RwLock<HashMap<MetaId, u64>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static PLAN_CACHE_TABLE_KEYS: LazyLock<RwLock<HashMap<MetaId, HashSet<String>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static PLAN_CACHE_INSERT_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn invalidate_planner_cache_for_tables(table_ids: &[MetaId]) {
    if table_ids.is_empty() {
        return;
    }
    PLAN_CACHE_MUTATION_EPOCH.fetch_add(1, Ordering::AcqRel);

    let table_ids = table_ids.iter().copied().collect::<HashSet<_>>();
    {
        let mut generations = PLAN_CACHE_TABLE_GENERATIONS.write();
        for table_id in &table_ids {
            let generation = generations.entry(*table_id).or_default();
            *generation = generation.wrapping_add(1);
        }
    }

    let keys = {
        let mut index = PLAN_CACHE_TABLE_KEYS.write();
        let keys = table_ids
            .iter()
            .filter_map(|table_id| index.remove(table_id))
            .flatten()
            .collect::<HashSet<_>>();
        for indexed_keys in index.values_mut() {
            indexed_keys.retain(|key| !keys.contains(key));
        }
        index.retain(|_, indexed_keys| !indexed_keys.is_empty());
        keys
    };

    let cache = LazyLock::force(&PLAN_CACHE);
    for key in keys {
        cache.evict(&key);
    }
}

pub fn clear_planner_cache() {
    PLAN_CACHE_MUTATION_EPOCH.fetch_add(1, Ordering::AcqRel);
    PLAN_CACHE_DDL_GENERATION.fetch_add(1, Ordering::AcqRel);
    LazyLock::force(&PLAN_CACHE).clear();
    PLAN_CACHE_TABLE_KEYS.write().clear();
    PLAN_CACHE_TABLE_GENERATIONS.write().clear();
}

pub fn set_planner_cache_max_bytes(max_bytes: u64) {
    let max_bytes = usize::try_from(max_bytes).unwrap_or(usize::MAX);
    LazyLock::force(&PLAN_CACHE).set_bytes_capacity(max_bytes);
}

impl From<PlanCacheItem> for CacheValue<PlanCacheItem> {
    fn from(val: PlanCacheItem) -> Self {
        let plan_bytes = format!("{:?}", val.plan).len();
        let settings_bytes = val
            .setting_changes
            .iter()
            .map(|(name, value)| name.len() + format!("{value:?}").len())
            .sum::<usize>();
        let variables_bytes = val
            .variables
            .iter()
            .map(|(name, value)| name.len() + format!("{value:?}").len())
            .sum::<usize>();
        let estimated_bytes = std::mem::size_of::<PlanCacheItem>()
            .saturating_add(plan_bytes)
            .saturating_add(settings_bytes)
            .saturating_add(variables_bytes);
        CacheValue::new(val, estimated_bytes)
    }
}

impl PlanCacheItem {
    fn create(
        plan: Plan,
        setting_changes: Vec<(String, ChangeValue)>,
        variables: HashMap<String, Scalar>,
        kind: PlanCacheEntryKind,
    ) -> Option<Self> {
        let table_ids = plan_table_ids(&plan)?;
        let table_generations = current_table_generations(&table_ids);
        Some(Self {
            plan,
            setting_changes,
            variables,
            kind,
            ddl_generation: PLAN_CACHE_DDL_GENERATION.load(Ordering::Acquire),
            table_generations,
        })
    }

    fn is_current(&self) -> bool {
        generations_are_current(self.ddl_generation, &self.table_generations)
    }
}

fn plan_table_ids(plan: &Plan) -> Option<Vec<MetaId>> {
    let Plan::Query { metadata, .. } = plan else {
        return None;
    };
    let metadata = metadata.read();
    let mut table_ids = metadata
        .tables()
        .iter()
        .map(|table| table.table().get_table_info().ident.table_id)
        .collect::<Vec<_>>();
    table_ids.sort_unstable();
    table_ids.dedup();
    (!table_ids.is_empty()).then_some(table_ids)
}

fn current_table_generations(table_ids: &[MetaId]) -> Vec<(MetaId, u64)> {
    let generations = PLAN_CACHE_TABLE_GENERATIONS.read();
    table_ids
        .iter()
        .map(|table_id| (*table_id, generations.get(table_id).copied().unwrap_or(0)))
        .collect()
}

fn generations_are_current(ddl_generation: u64, table_generations: &[(MetaId, u64)]) -> bool {
    if PLAN_CACHE_DDL_GENERATION.load(Ordering::Acquire) != ddl_generation {
        return false;
    }
    let current = PLAN_CACHE_TABLE_GENERATIONS.read();
    table_generations
        .iter()
        .all(|(table_id, generation)| current.get(table_id).copied().unwrap_or(0) == *generation)
}

fn unregister_cache_key(cache_key: &str) {
    let mut index = PLAN_CACHE_TABLE_KEYS.write();
    for keys in index.values_mut() {
        keys.remove(cache_key);
    }
    index.retain(|_, keys| !keys.is_empty());
}

fn insert_cache_item(cache_key: String, item: PlanCacheItem, expected_mutation_epoch: u64) {
    if !item.is_current()
        || PLAN_CACHE_MUTATION_EPOCH.load(Ordering::Acquire) != expected_mutation_epoch
    {
        return;
    }

    let ddl_generation = item.ddl_generation;
    let table_generations = item.table_generations.clone();
    unregister_cache_key(&cache_key);
    LazyLock::force(&PLAN_CACHE).insert(cache_key.clone(), item);
    {
        let mut index = PLAN_CACHE_TABLE_KEYS.write();
        for (table_id, _) in &table_generations {
            index
                .entry(*table_id)
                .or_default()
                .insert(cache_key.clone());
        }
    }

    // The underlying LRU does not report keys evicted by its byte limit. Periodically prune the
    // reverse index so workloads over immutable tables cannot accumulate stale dependency keys.
    if PLAN_CACHE_INSERT_COUNT.fetch_add(1, Ordering::Relaxed) % 256 == 255 {
        let cache = LazyLock::force(&PLAN_CACHE);
        let mut index = PLAN_CACHE_TABLE_KEYS.write();
        for keys in index.values_mut() {
            keys.retain(|key| cache.contains_key(key));
        }
        index.retain(|_, keys| !keys.is_empty());
    }

    // Close the race with a mutation that committed between the first check and registration.
    if !generations_are_current(ddl_generation, &table_generations)
        || PLAN_CACHE_MUTATION_EPOCH.load(Ordering::Acquire) != expected_mutation_epoch
    {
        LazyLock::force(&PLAN_CACHE).evict(&cache_key);
        unregister_cache_key(&cache_key);
    }
}

impl Planner {
    pub(crate) fn planner_cache_key(format_sql: &str) -> String {
        // use sha2 to encode the sql
        format!("{:x}", Sha256::digest(format_sql))
    }

    pub(crate) fn build_plan_cache_context(
        &self,
        name_resolution_ctx: NameResolutionContext,
        stmt: &Statement,
        enable_distributed_optimization: bool,
    ) -> Result<Option<PlanCacheContext>> {
        if !matches!(stmt, Statement::Query(_))
            || !self.ctx.get_settings().get_enable_planner_cache()?
        {
            return Ok(None);
        }
        let mutation_epoch = PLAN_CACHE_MUTATION_EPOCH.load(Ordering::Acquire);

        let mut visitor = TableRefVisitor {
            ctx: self.ctx.clone(),
            table_snapshots: vec![],
            name_resolution_ctx,
            cache_miss: false,
            has_security_policy: false,
        };
        stmt.drive(&mut visitor);

        if visitor.cache_miss || visitor.table_snapshots.is_empty() {
            return Ok(None);
        }

        let optimized_namespace = if enable_distributed_optimization {
            "optimized-distributed"
        } else {
            "optimized-local"
        };
        let optimized_cache_key = self.planner_cache_key_for_sql(
            optimized_namespace,
            &stmt.to_string(),
            visitor.has_security_policy,
        )?;
        Ok(Some(PlanCacheContext {
            optimized_cache_key,
            template_cache_key: None,
            table_snapshots: visitor.table_snapshots,
            parameterized_stmt: None,
            has_security_policy: visitor.has_security_policy,
            mutation_epoch,
        }))
    }

    pub(crate) fn prepare_parameterized_cache_context(
        &self,
        cache_ctx: &mut PlanCacheContext,
        stmt: &Statement,
    ) -> Result<()> {
        let parameterized_stmt = ParameterizedStatement::create(stmt)?;
        cache_ctx.template_cache_key = parameterized_stmt
            .is_parameterized()
            .then(|| {
                self.planner_cache_key_for_sql(
                    "bound-template",
                    parameterized_stmt.cache_key_sql(),
                    cache_ctx.has_security_policy,
                )
            })
            .transpose()?;
        cache_ctx.parameterized_stmt = Some(parameterized_stmt);
        Ok(())
    }

    fn planner_cache_key_for_sql(
        &self,
        namespace: &str,
        sql: &str,
        has_security_policy: bool,
    ) -> Result<String> {
        let tenant = self.ctx.get_tenant();
        let context = format!(
            "{}\0{}\0{}",
            tenant.tenant_name(),
            self.ctx.get_current_catalog(),
            self.ctx.get_current_database(),
        );
        let key_source = if has_security_policy {
            format!(
                "{context}\0{}\0{namespace}\0{sql}",
                self.security_policy_cache_key_prefix()?
            )
        } else {
            format!("{context}\0{namespace}\0{sql}")
        };
        Ok(Self::planner_cache_key(&key_source))
    }

    fn security_policy_cache_key_prefix(&self) -> Result<String> {
        let user = self
            .ctx
            .get_current_user()?
            .identity()
            .display()
            .to_string();
        let role = self
            .ctx
            .get_current_role()
            .map(|r| r.name)
            .unwrap_or_default();

        let mut secondary_roles = self.ctx.get_secondary_roles();
        if let Some(roles) = &mut secondary_roles {
            roles.sort();
        }

        Ok(format!(
            "secure\0{}\0{}\0{}",
            user,
            role,
            Self::secondary_roles_cache_key(secondary_roles.as_deref()),
        ))
    }

    fn secondary_roles_cache_key(secondary_roles: Option<&[String]>) -> String {
        match secondary_roles {
            None => "ALL".to_string(),
            Some([]) => "NONE".to_string(),
            Some(roles) => format!("SOME:{}", roles.join(",")),
        }
    }

    fn get_cache_item(
        &self,
        cache_ctx: &PlanCacheContext,
        cache_key: &str,
        expected_kind: PlanCacheEntryKind,
    ) -> Option<PlanCacheItem> {
        debug_assert!(!cache_ctx.table_snapshots.is_empty());

        let cache = LazyLock::force(&PLAN_CACHE);
        let plan_item = cache.get(cache_key)?;
        if !plan_item.is_current() {
            cache.evict(cache_key);
            unregister_cache_key(cache_key);
            return None;
        }

        if plan_item.kind != expected_kind {
            return None;
        }

        if self.setting_changes() != plan_item.setting_changes
            || self.ctx.get_all_variables() != plan_item.variables
        {
            return None;
        }

        let Plan::Query { metadata, .. } = &plan_item.plan else {
            return None;
        };

        let metadata = metadata.read();
        if !cache_ctx.matches_metadata_tables(metadata.tables()) {
            drop(metadata);
            cache.evict(cache_key);
            unregister_cache_key(cache_key);
            return None;
        }

        Some(plan_item.as_ref().clone())
    }

    pub(crate) fn get_optimized_cache(
        &self,
        cache_ctx: &PlanCacheContext,
    ) -> Option<PlanCacheItem> {
        self.get_cache_item(
            cache_ctx,
            &cache_ctx.optimized_cache_key,
            PlanCacheEntryKind::Optimized,
        )
    }

    pub(crate) fn get_parameterized_cache(
        &self,
        cache_ctx: &PlanCacheContext,
    ) -> Option<PlanCacheItem> {
        let cache_key = cache_ctx.template_cache_key.as_deref()?;
        let kind = PlanCacheEntryKind::BoundTemplate {
            parameter_count: cache_ctx.parameter_count(),
        };
        self.get_cache_item(cache_ctx, cache_key, kind)
    }

    pub(crate) fn evict_parameterized_cache(&self, cache_ctx: &PlanCacheContext) {
        if let Some(cache_key) = &cache_ctx.template_cache_key {
            LazyLock::force(&PLAN_CACHE).evict(cache_key);
            unregister_cache_key(cache_key);
        }
    }

    pub(crate) fn instantiate_cached_plan(
        &self,
        cache_ctx: &PlanCacheContext,
        template: &Plan,
        formatted_ast: Option<String>,
    ) -> Result<Plan> {
        let parameterized_stmt = cache_ctx.parameterized_stmt.as_ref().ok_or_else(|| {
            databend_common_exception::ErrorCode::Internal(
                "planner cache tried to instantiate a non-parameterized entry".to_string(),
            )
        })?;
        instantiate_plan(template, &parameterized_stmt.values, formatted_ast)
    }

    pub(crate) fn set_parameterized_cache(&self, cache_ctx: &PlanCacheContext, plan: Plan) {
        debug_assert!(cache_ctx.is_parameterized());
        let Some(cache_key) = cache_ctx.template_cache_key.clone() else {
            return;
        };
        let Some(plan_item) = PlanCacheItem::create(
            plan,
            self.setting_changes(),
            self.ctx.get_all_variables(),
            PlanCacheEntryKind::BoundTemplate {
                parameter_count: cache_ctx.parameter_count(),
            },
        ) else {
            return;
        };
        insert_cache_item(cache_key, plan_item, cache_ctx.mutation_epoch);
    }

    pub(crate) fn set_cache(&self, cache_ctx: PlanCacheContext, plan: Plan) -> Plan {
        let setting_changes = self.setting_changes();
        let variables = self.ctx.get_all_variables();
        let Some(plan_item) = PlanCacheItem::create(
            plan.clone(),
            setting_changes,
            variables,
            PlanCacheEntryKind::Optimized,
        ) else {
            return plan;
        };
        insert_cache_item(
            cache_ctx.optimized_cache_key,
            plan_item,
            cache_ctx.mutation_epoch,
        );
        plan
    }

    fn setting_changes(&self) -> Vec<(String, ChangeValue)> {
        self.ctx
            .get_settings()
            .changes()
            .iter()
            .map(|s| (s.key().clone(), s.value().clone()))
            .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
            .collect()
    }
}

#[derive(Visitor)]
#[visitor(TableReference(enter), FunctionCall(enter))]
struct TableRefVisitor {
    ctx: Arc<dyn TableContext>,
    table_snapshots: Vec<TableSnapshot>,
    name_resolution_ctx: NameResolutionContext,
    cache_miss: bool,
    has_security_policy: bool,
}

#[derive(Clone)]
pub(crate) struct PlanCacheContext {
    optimized_cache_key: String,
    template_cache_key: Option<String>,
    table_snapshots: Vec<TableSnapshot>,
    has_security_policy: bool,
    parameterized_stmt: Option<ParameterizedStatement>,
    mutation_epoch: u64,
}

impl PlanCacheContext {
    pub(crate) fn is_parameterized(&self) -> bool {
        self.parameterized_stmt
            .as_ref()
            .is_some_and(ParameterizedStatement::is_parameterized)
    }

    pub(crate) fn bind_statement<'a>(&'a self, original: &'a Statement) -> &'a Statement {
        self.parameterized_stmt
            .as_ref()
            .filter(|stmt| stmt.is_parameterized())
            .map_or(original, |stmt| &stmt.template)
    }

    fn parameter_count(&self) -> usize {
        self.parameterized_stmt
            .as_ref()
            .map_or(0, |p| p.values.len())
    }

    fn matches_metadata_tables(&self, tables: &[TableEntry]) -> bool {
        self.table_snapshots.iter().all(|snapshot| {
            tables
                .iter()
                .any(|table| snapshot.matches_table_entry(table))
        })
    }
}

#[derive(Clone)]
struct TableSnapshot {
    catalog_name: String,
    database_name: String,
    table_name: String,
    table_id: MetaId,
    table_seq: u64,
    schema: TableSchemaRef,
    snapshot_location: String,
    security_policy: SecurityPolicySnapshot,
}

impl TableSnapshot {
    fn from_resolved_table(
        table: &dyn Table,
        catalog_name: String,
        database_name: String,
        table_name: String,
    ) -> Option<Self> {
        if table.is_temp() || table.is_stage_table() || table.is_stream() {
            return None;
        }

        let table_info = table.get_table_info();
        let snapshot_location = table.options().get(OPT_KEY_SNAPSHOT_LOCATION)?.clone();
        Some(Self {
            catalog_name,
            database_name,
            table_name,
            table_id: table_info.ident.table_id,
            table_seq: table_info.ident.seq,
            schema: table.schema(),
            snapshot_location,
            security_policy: SecurityPolicySnapshot::from(&table_info.meta),
        })
    }

    fn has_security_policy(&self) -> bool {
        self.security_policy.has_policy()
    }

    fn matches_table_entry(&self, table_entry: &TableEntry) -> bool {
        if table_entry.catalog() != self.catalog_name
            || table_entry.database() != self.database_name
            || table_entry.name() != self.table_name
        {
            return false;
        }
        let table = table_entry.table();
        let table_info = table.get_table_info();
        if table.is_temp()
            || table_info.ident.table_id != self.table_id
            || table_info.ident.seq != self.table_seq
            || table.schema().ne(&self.schema)
        {
            return false;
        }

        table.options().get(OPT_KEY_SNAPSHOT_LOCATION) == Some(&self.snapshot_location)
            && SecurityPolicySnapshot::from(&table_info.meta) == self.security_policy
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SecurityPolicySnapshot {
    column_mask_policy_columns_ids: BTreeMap<ColumnId, SecurityPolicyColumnMap>,
    row_access_policy_columns_ids: Option<SecurityPolicyColumnMap>,
}

impl SecurityPolicySnapshot {
    fn has_policy(&self) -> bool {
        !self.column_mask_policy_columns_ids.is_empty()
            || self.row_access_policy_columns_ids.is_some()
    }
}

impl From<&TableMeta> for SecurityPolicySnapshot {
    fn from(meta: &TableMeta) -> Self {
        Self {
            column_mask_policy_columns_ids: meta.column_mask_policy_columns_ids.clone(),
            row_access_policy_columns_ids: meta.row_access_policy_columns_ids.clone(),
        }
    }
}

impl TableRefVisitor {
    fn enter_function_call(&mut self, func: &FunctionCall) {
        if self.cache_miss {
            return;
        }

        let func_name = func.name.name.to_lowercase();
        // If the function is not suitable for caching, we should not cache the plan
        if !is_cacheable_function(&func_name) {
            self.cache_miss = true;
        }
    }

    fn enter_table_reference(&mut self, table_ref: &TableReference) {
        if self.cache_miss {
            return;
        }
        if matches!(
            table_ref,
            TableReference::TableFunction { .. } | TableReference::Location { .. }
        ) {
            self.cache_miss = true;
            return;
        }

        if let TableReference::Table {
            table,
            temporal,
            with_options,
            ..
        } = table_ref
        {
            if temporal.is_some() || with_options.is_some() {
                self.cache_miss = true;
                return;
            }

            let catalog = table.catalog.to_owned().unwrap_or(Identifier {
                span: None,
                name: self.ctx.get_current_catalog(),
                quote: None,
                ident_type: IdentifierType::None,
            });
            let database = table.database.to_owned().unwrap_or(Identifier {
                span: None,
                name: self.ctx.get_current_database(),
                quote: None,
                ident_type: IdentifierType::None,
            });

            let catalog_name = normalize_identifier(&catalog, &self.name_resolution_ctx).name;
            let database_name = normalize_identifier(&database, &self.name_resolution_ctx).name;
            let table_name = normalize_identifier(&table.table, &self.name_resolution_ctx).name;
            let branch = table
                .branch
                .as_ref()
                .map(|v| normalize_identifier(v, &self.name_resolution_ctx).name);

            databend_common_base::runtime::block_on(async move {
                if let Ok(table) = self
                    .ctx
                    .resolve_data_source(
                        &catalog_name,
                        &database_name,
                        &table_name,
                        branch.as_deref(),
                        None,
                    )
                    .await
                {
                    if let Some(snapshot) = TableSnapshot::from_resolved_table(
                        table.as_ref(),
                        catalog_name,
                        database_name,
                        table_name,
                    ) {
                        self.has_security_policy |= snapshot.has_security_policy();
                        self.table_snapshots.push(snapshot);
                        return;
                    }
                }
                self.cache_miss = true;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use parking_lot::Mutex;

    use super::*;

    static PLANNER_CACHE_TEST_LOCK: Mutex<()> = Mutex::new(());
    static NEXT_TEST_TABLE_ID: AtomicU64 = AtomicU64::new(u64::MAX / 2);

    fn synthetic_item(table_id: MetaId) -> PlanCacheItem {
        let generation = PLAN_CACHE_TABLE_GENERATIONS
            .read()
            .get(&table_id)
            .copied()
            .unwrap_or(0);
        PlanCacheItem {
            plan: Plan::ExplainAst {
                formatted_string: String::new(),
            },
            setting_changes: vec![],
            variables: HashMap::new(),
            kind: PlanCacheEntryKind::BoundTemplate { parameter_count: 1 },
            ddl_generation: PLAN_CACHE_DDL_GENERATION.load(Ordering::Acquire),
            table_generations: vec![(table_id, generation)],
        }
    }

    #[test]
    fn test_default_cache_capacity_is_512_mib() {
        assert_eq!(
            LazyLock::force(&PLAN_CACHE).bytes_capacity(),
            DEFAULT_PLANNER_CACHE_MAX_BYTES as u64
        );
    }

    #[test]
    fn test_dml_evicts_only_dependent_table_plans() {
        let _guard = PLANNER_CACHE_TEST_LOCK.lock();
        let first_table = NEXT_TEST_TABLE_ID.fetch_add(2, Ordering::Relaxed);
        let second_table = first_table + 1;
        let first_key = format!("planner-cache-test-{first_table}");
        let second_key = format!("planner-cache-test-{second_table}");
        let mutation_epoch = PLAN_CACHE_MUTATION_EPOCH.load(Ordering::Acquire);

        insert_cache_item(
            first_key.clone(),
            synthetic_item(first_table),
            mutation_epoch,
        );
        insert_cache_item(
            second_key.clone(),
            synthetic_item(second_table),
            mutation_epoch,
        );
        let cache = LazyLock::force(&PLAN_CACHE);
        assert!(cache.contains_key(&first_key));
        assert!(cache.contains_key(&second_key));

        invalidate_planner_cache_for_tables(&[first_table]);
        assert!(!cache.contains_key(&first_key));
        assert!(cache.contains_key(&second_key));

        invalidate_planner_cache_for_tables(&[second_table]);
        assert!(!cache.contains_key(&second_key));
    }

    #[test]
    fn test_ddl_clears_all_cached_plans() {
        let _guard = PLANNER_CACHE_TEST_LOCK.lock();
        let first_table = NEXT_TEST_TABLE_ID.fetch_add(2, Ordering::Relaxed);
        let second_table = first_table + 1;
        let first_key = format!("planner-cache-test-{first_table}");
        let second_key = format!("planner-cache-test-{second_table}");
        let mutation_epoch = PLAN_CACHE_MUTATION_EPOCH.load(Ordering::Acquire);

        insert_cache_item(
            first_key.clone(),
            synthetic_item(first_table),
            mutation_epoch,
        );
        insert_cache_item(
            second_key.clone(),
            synthetic_item(second_table),
            mutation_epoch,
        );
        let cache = LazyLock::force(&PLAN_CACHE);
        assert!(cache.contains_key(&first_key));
        assert!(cache.contains_key(&second_key));

        clear_planner_cache();
        assert!(!cache.contains_key(&first_key));
        assert!(!cache.contains_key(&second_key));
    }

    #[test]
    fn test_mutation_rejects_plan_built_before_commit() {
        let _guard = PLANNER_CACHE_TEST_LOCK.lock();
        let table_id = NEXT_TEST_TABLE_ID.fetch_add(1, Ordering::Relaxed);
        let cache_key = format!("planner-cache-test-{table_id}");
        let mutation_epoch = PLAN_CACHE_MUTATION_EPOCH.load(Ordering::Acquire);
        let stale_item = synthetic_item(table_id);

        invalidate_planner_cache_for_tables(&[table_id]);
        insert_cache_item(cache_key.clone(), stale_item, mutation_epoch);

        assert!(!LazyLock::force(&PLAN_CACHE).contains_key(&cache_key));
    }
}
