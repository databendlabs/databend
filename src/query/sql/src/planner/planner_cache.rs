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
use std::sync::Arc;
use std::sync::LazyLock;

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
use databend_common_meta_app::schema::MATERIALIZED_VIEW_ENGINE;
use databend_common_meta_app::schema::MVSourceBindingSnapshot;
use databend_common_meta_app::schema::SecurityPolicyColumnMap;
use databend_common_meta_app::schema::TableMeta;
use databend_common_settings::ChangeValue;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheValue;
use databend_storages_common_cache::InMemoryLruCache;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use derive_visitor::Drive;
use derive_visitor::Visitor;
use itertools::Itertools;
use sha2::Digest;
use sha2::Sha256;

use crate::NameResolutionContext;
use crate::Planner;
use crate::TableEntry;
use crate::normalize_identifier;
use crate::planner::binder::MVSourceBindings;
use crate::plans::Plan;

#[derive(Clone)]
pub struct PlanCacheItem {
    pub(crate) plan: Plan,
    pub(crate) setting_changes: Vec<(String, ChangeValue)>,
    pub(crate) variables: HashMap<String, Scalar>,
    /// Binding marked the query result cache unusable, for example because the statement calls
    /// `now()`. A plan cache hit skips binding, so the flag is kept here and applied on hit.
    pub(crate) result_cache_uncacheable: bool,
}

static PLAN_CACHE: LazyLock<InMemoryLruCache<PlanCacheItem>> =
    LazyLock::new(|| InMemoryLruCache::with_items_capacity("planner_cache".to_string(), 512));

impl From<PlanCacheItem> for CacheValue<PlanCacheItem> {
    fn from(val: PlanCacheItem) -> Self {
        CacheValue::new(val, 1024)
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
    ) -> Result<Option<PlanCacheContext>> {
        if !matches!(stmt, Statement::Query(_))
            || !self.ctx.get_settings().get_enable_planner_cache()?
        {
            return Ok(None);
        }

        let mut visitor = TableRefVisitor {
            ctx: self.ctx.clone(),
            table_snapshots: vec![],
            name_resolution_ctx,
            cache_miss: false,
            has_security_policy: false,
            mv_sources: vec![],
        };
        stmt.drive(&mut visitor);

        if visitor.cache_miss || visitor.table_snapshots.is_empty() {
            return Ok(None);
        }
        let Some(mv_source_bindings) = self.read_mv_source_bindings(&visitor.mv_sources) else {
            return Ok(None);
        };
        let mv_fingerprints = visitor
            .mv_sources
            .iter()
            .map(|source| mv_fingerprint(source.1, &mv_source_bindings[source]))
            .collect::<Vec<_>>();

        let cache_key =
            self.planner_cache_key_for_stmt(stmt, visitor.has_security_policy, &mv_fingerprints)?;
        Ok(Some(PlanCacheContext {
            cache_key,
            table_snapshots: visitor.table_snapshots,
            mv_source_bindings,
        }))
    }

    /// The materialized-view bindings of each source table, once per table. `None` if one cannot
    /// be read; the statement is then not cached. Read only once the statement is known to be
    /// cacheable, as keying a plan that is not cached would read them for nothing.
    fn read_mv_source_bindings(&self, sources: &[(String, u64)]) -> Option<MVSourceBindings> {
        databend_common_base::runtime::block_on(async {
            let tenant = self.ctx.get_tenant();
            let mut bindings = MVSourceBindings::with_capacity(sources.len());
            for source @ (catalog, source_table_id) in sources {
                if bindings.contains_key(source) {
                    continue;
                }
                let snapshot = self
                    .ctx
                    .get_catalog(catalog)
                    .await
                    .ok()?
                    .get_mv_source_binding_snapshot(&tenant, *source_table_id)
                    .await
                    .ok()?;
                bindings.insert(source.clone(), Arc::new(snapshot));
            }
            Some(bindings)
        })
    }

    fn planner_cache_key_for_stmt(
        &self,
        stmt: &Statement,
        has_security_policy: bool,
        mv_fingerprints: &[String],
    ) -> Result<String> {
        let mv_fingerprint = mv_fingerprints.join("\0");
        if has_security_policy {
            return Ok(Self::planner_cache_key(&format!(
                "{}\0{}\0{}",
                self.security_policy_cache_key_prefix()?,
                mv_fingerprint,
                stmt
            )));
        }

        Ok(Self::planner_cache_key(&format!(
            "{}\0{}",
            mv_fingerprint, stmt
        )))
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

    pub(crate) fn get_cache(&self, cache_ctx: &PlanCacheContext) -> Option<PlanCacheItem> {
        debug_assert!(!cache_ctx.table_snapshots.is_empty());

        let cache = LazyLock::force(&PLAN_CACHE);
        let plan_item = cache.get(&cache_ctx.cache_key)?;

        let settings = self.ctx.get_settings();
        if settings.changes().len() != plan_item.setting_changes.len()
            || self.setting_changes() != plan_item.setting_changes
            || self.ctx.get_all_variables() != plan_item.variables
        {
            return None;
        }

        let Plan::Query { metadata, .. } = &plan_item.plan else {
            return None;
        };

        let metadata = metadata.read();
        if !cache_ctx.matches_metadata_tables(metadata.tables()) {
            return None;
        }

        if plan_item.result_cache_uncacheable {
            self.ctx.result_cache_state().set_cacheable(false);
        }
        Some(plan_item.as_ref().clone())
    }

    pub(crate) fn set_cache(&self, cache_ctx: PlanCacheContext, plan: Plan) -> Result<()> {
        if self
            .ctx
            .get_function_context()?
            .query_specific_values
            .recorded()
        {
            // Planning computed a value valid only for this query, such as the value of
            // `VALUES (now())`, and the plan may hold it.
            return Ok(());
        }

        let plan_item = PlanCacheItem {
            plan,
            setting_changes: self.setting_changes(),
            variables: self.ctx.get_all_variables(),
            result_cache_uncacheable: !self.ctx.result_cache_state().cacheable(),
        };
        let cache = LazyLock::force(&PLAN_CACHE);
        cache.insert(cache_ctx.cache_key, plan_item);
        Ok(())
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
    // The catalog and id of each table read, a possible source of materialized views.
    mv_sources: Vec<(String, u64)>,
    name_resolution_ctx: NameResolutionContext,
    cache_miss: bool,
    has_security_policy: bool,
}

pub(crate) struct PlanCacheContext {
    cache_key: String,
    table_snapshots: Vec<TableSnapshot>,
    /// The bindings the key was built from. Binding the statement on a cache miss reuses them,
    /// so the plan cached under the key is built from the same bindings.
    mv_source_bindings: MVSourceBindings,
}

impl PlanCacheContext {
    pub(crate) fn mv_source_bindings(&self) -> &MVSourceBindings {
        &self.mv_source_bindings
    }

    fn matches_metadata_tables(&self, tables: &[TableEntry]) -> bool {
        self.table_snapshots.iter().all(|snapshot| {
            tables
                .iter()
                .any(|table| snapshot.matches_table_entry(table))
        })
    }
}

/// What of the materialized views of a source table a cached plan depends on: a plan rewritten
/// to read one is stale once it changes.
fn mv_fingerprint(source_table_id: u64, snapshot: &MVSourceBindingSnapshot) -> String {
    let mut fingerprint = format!(
        "source:{}:binding_generation:{}",
        source_table_id, snapshot.generation
    );
    for mv in snapshot
        .materialized_views
        .iter()
        .sorted_by_key(|mv| mv.mv_id)
    {
        let mv_options = &mv.table_meta.data.options;
        fingerprint.push_str(&format!(
            ":mv:{}:definition:{}:table_seq:{}:snapshot:{:?}:checkpoint_seq:{:?}:checkpoint_snapshot:{:?}",
            mv.mv_id,
            mv.definition.seq,
            mv.table_meta.seq,
            mv_options.get(OPT_KEY_SNAPSHOT_LOCATION),
            mv_options.get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ),
            mv_options.get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION),
        ));
    }
    fingerprint
}

#[derive(Clone)]
struct TableSnapshot {
    schema: TableSchemaRef,
    table_seq: u64,
    snapshot_location: String,
    security_policy: SecurityPolicySnapshot,
}

impl TableSnapshot {
    fn from_resolved_table(table: &dyn Table) -> Option<Self> {
        if !table.plan_can_be_cached()
            || table.is_temp()
            || table.is_stage_table()
            || table.is_stream()
        {
            return None;
        }

        let snapshot_location = table.options().get(OPT_KEY_SNAPSHOT_LOCATION)?.clone();
        Some(Self {
            schema: table.schema(),
            table_seq: table.get_table_info().ident.seq,
            snapshot_location,
            security_policy: SecurityPolicySnapshot::from(&table.get_table_info().meta),
        })
    }

    fn has_security_policy(&self) -> bool {
        self.security_policy.has_policy()
    }

    fn matches_table_entry(&self, table_entry: &TableEntry) -> bool {
        let table = table_entry.table();
        self.matches_table(table.as_ref())
    }

    fn matches_table(&self, table: &dyn Table) -> bool {
        if table.is_temp() || table.schema().ne(&self.schema) {
            return false;
        }

        table.get_table_info().ident.seq == self.table_seq
            && table.options().get(OPT_KEY_SNAPSHOT_LOCATION) == Some(&self.snapshot_location)
            && SecurityPolicySnapshot::from(&table.get_table_info().meta) == self.security_policy
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SecurityPolicySnapshot {
    column_mask_policy_columns_ids: BTreeMap<ColumnId, SecurityPolicyColumnMap>,
    row_access_policy_columns_ids: Option<SecurityPolicyColumnMap>,
    masking_where_use_unmasked_value: std::collections::BTreeSet<ColumnId>,
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
            masking_where_use_unmasked_value: meta.masking_where_use_unmasked_value.clone(),
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

            databend_common_base::runtime::block_on(async move {
                if let Ok(table) = self
                    .ctx
                    .resolve_data_source(&catalog_name, &database_name, &table_name, None)
                    .await
                {
                    if table.engine() == MATERIALIZED_VIEW_ENGINE {
                        // A direct MV read may fall back to its live source endpoint, which is not
                        // represented by the ordinary table snapshot.
                        self.cache_miss = true;
                        return;
                    }
                    // Its materialized views are read once the whole statement is known to be
                    // cacheable: see `read_mv_source_bindings`.
                    self.mv_sources.push((catalog_name, table.get_id()));
                    if let Some(snapshot) = TableSnapshot::from_resolved_table(table.as_ref()) {
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
    use databend_common_expression::TableSchema;
    use databend_common_meta_app::schema::MVDefinition;
    use databend_common_meta_app::schema::MVInfo;
    use databend_common_meta_app::schema::MVSourceBindingSnapshot;
    use databend_common_meta_app::schema::TableMeta;
    use databend_meta_client::types::SeqV;
    use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

    use super::mv_fingerprint;

    fn mv(mv_id: u64, table_seq: u64, snapshot_location: &str) -> MVInfo {
        let mut table_meta = TableMeta::default();
        table_meta.options.insert(
            OPT_KEY_SNAPSHOT_LOCATION.to_string(),
            snapshot_location.to_string(),
        );
        MVInfo {
            mv_id,
            definition: SeqV::new(1, MVDefinition {
                original_query: "SELECT a FROM t".to_string(),
                query: "SELECT a FROM t".to_string(),
                logical_schema: TableSchema::empty(),
                sync_creation: false,
            }),
            table_meta: SeqV::new(table_seq, table_meta),
        }
    }

    fn snapshot(generation: u64, materialized_views: Vec<MVInfo>) -> MVSourceBindingSnapshot {
        MVSourceBindingSnapshot {
            generation,
            materialized_views,
        }
    }

    #[test]
    fn test_mv_fingerprint_ignores_listing_order() {
        let listed = snapshot(3, vec![mv(7, 1, "s7"), mv(5, 1, "s5")]);
        let sorted = snapshot(3, vec![mv(5, 1, "s5"), mv(7, 1, "s7")]);
        assert_eq!(mv_fingerprint(10, &listed), mv_fingerprint(10, &sorted));
    }

    #[test]
    fn test_mv_fingerprint_changes_with_bindings() {
        let base = mv_fingerprint(10, &snapshot(3, vec![mv(5, 1, "s5")]));
        // Another source table, a new binding generation, a refreshed MV, an MV added.
        for changed in [
            mv_fingerprint(11, &snapshot(3, vec![mv(5, 1, "s5")])),
            mv_fingerprint(10, &snapshot(4, vec![mv(5, 1, "s5")])),
            mv_fingerprint(10, &snapshot(3, vec![mv(5, 2, "s5")])),
            mv_fingerprint(10, &snapshot(3, vec![mv(5, 1, "s5b")])),
            mv_fingerprint(10, &snapshot(3, vec![mv(5, 1, "s5"), mv(6, 1, "s6")])),
        ] {
            assert_ne!(base, changed);
        }
    }
}
