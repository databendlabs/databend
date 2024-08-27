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

use std::sync::Arc;
use std::sync::LazyLock;

use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::IdentifierType;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableReference;
use databend_common_catalog::table_context::TableContext;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheValue;
use databend_storages_common_cache::InMemoryLruCache;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use derive_visitor::Drive;
use derive_visitor::Visitor;
use sha2::Digest;
use sha2::Sha256;

use crate::normalize_identifier;
use crate::plans::Plan;
use crate::NameResolutionContext;
use crate::PlanExtras;
use crate::Planner;

#[derive(Clone)]
pub struct PlanCacheItem {
    pub(crate) plan: Plan,
    pub(crate) extras: PlanExtras,
}

static PLAN_CACHE: LazyLock<InMemoryLruCache<PlanCacheItem>> =
    LazyLock::new(|| InMemoryLruCache::with_items_capacity("planner_cache".to_string(), 512));

impl From<PlanCacheItem> for CacheValue<PlanCacheItem> {
    fn from(val: PlanCacheItem) -> Self {
        CacheValue::new(val, 1024)
    }
}

impl Planner {
    pub fn planner_cache_key(format_sql: &str) -> String {
        // use sha2 to encode the sql
        format!("{:x}", Sha256::digest(format_sql))
    }

    // return (enable_planner_cache, plan)
    pub fn get_cache(
        &self,
        name_resolution_ctx: NameResolutionContext,
        key: &str,
        stmt: &Statement,
    ) -> (bool, Option<PlanCacheItem>) {
        if !matches!(stmt, Statement::Query(_)) {
            return (false, None);
        }

        let mut visitor = TableRefVisitor {
            ctx: self.ctx.clone(),
            schema_snapshots: vec![],
            name_resolution_ctx,
            cache_miss: false,
        };
        stmt.drive(&mut visitor);

        if visitor.schema_snapshots.is_empty() || visitor.cache_miss {
            return (false, None);
        }

        let cache = LazyLock::force(&PLAN_CACHE);
        if let Some(plan) = cache.get(key) {
            if let Plan::Query { metadata, .. } = &plan.plan {
                let metadata = metadata.read();
                if visitor.schema_snapshots.iter().all(|ss| {
                    metadata.tables().iter().any(|table| {
                        !table.table().is_temp()
                            && table.table().options().get(OPT_KEY_SNAPSHOT_LOCATION) == Some(&ss.1)
                            && table.table().schema().eq(&ss.0)
                    })
                }) {
                    return (!visitor.cache_miss, Some(plan.as_ref().clone()));
                }
            }
            (!visitor.cache_miss, None)
        } else {
            (!visitor.cache_miss, None)
        }
    }

    pub fn set_cache(&self, key: String, plan: PlanCacheItem) {
        let cache = LazyLock::force(&PLAN_CACHE);
        cache.insert(key, plan);
    }
}

#[derive(Visitor)]
#[visitor(TableReference(enter))]
struct TableRefVisitor {
    ctx: Arc<dyn TableContext>,
    schema_snapshots: Vec<(TableSchemaRef, String)>,
    name_resolution_ctx: NameResolutionContext,
    cache_miss: bool,
}

impl TableRefVisitor {
    fn enter_table_reference(&mut self, table_ref: &TableReference) {
        if let TableReference::Table {
            catalog,
            database,
            table,
            temporal,
            consume,
            ..
        } = table_ref
        {
            if temporal.is_some() || *consume {
                self.cache_miss = true;
                return;
            }

            let catalog = catalog.to_owned().unwrap_or(Identifier {
                span: None,
                name: self.ctx.get_current_catalog(),
                quote: None,
                ident_type: IdentifierType::None,
            });
            let database = database.to_owned().unwrap_or(Identifier {
                span: None,
                name: self.ctx.get_current_database(),
                quote: None,
                ident_type: IdentifierType::None,
            });

            let catalog_name = normalize_identifier(&catalog, &self.name_resolution_ctx).name;
            let database_name = normalize_identifier(&database, &self.name_resolution_ctx).name;
            let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;

            databend_common_base::runtime::block_on(async move {
                if let Ok(table_meta) = self
                    .ctx
                    .get_table(&catalog_name, &database_name, &table_name)
                    .await
                {
                    if !table_meta.is_temp()
                        && let Some(sn) = table_meta.options().get(OPT_KEY_SNAPSHOT_LOCATION)
                    {
                        self.schema_snapshots
                            .push((table_meta.schema(), sn.clone()));
                        return;
                    }
                }
                self.cache_miss = true;
            });
        }
    }
}
