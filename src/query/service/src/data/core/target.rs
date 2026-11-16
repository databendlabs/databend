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
use std::time::Duration;

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_fuse::FuseSegmentFormat;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::FuseCaches;
use databend_common_storages_fuse::io::SegmentInfoReader;
use databend_common_storages_fuse::segment_format_from_location;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures::future::try_join_all;

use crate::data::core::TableRef;
use crate::data::core::table_cache::TableCache;
use crate::data::core::table_cache::TableKey;
use crate::interpreters::validate_table_read;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCachePolicy;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;

/// The table an operation reads, checked readable by the user, at its latest snapshot.
pub struct TableTarget {
    pub ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    snapshot: Option<Arc<TableSnapshot>>,
}

impl TableTarget {
    /// Resolves and checks `table` like a SQL query reading it. The table may be one
    /// resolved by an earlier request within `data_api_table_cache_ttl_ms`.
    pub async fn resolve(ctx: Arc<QueryContext>, table: TableRef<'_>) -> Result<Self> {
        let name = table.table;
        let catalog = match table.catalog {
            Some(catalog) => catalog.to_string(),
            None => ctx.get_current_catalog(),
        };
        let database = match table.database {
            Some(database) => database.to_string(),
            None => ctx.get_current_database(),
        };
        let table = resolve_table(&ctx, catalog.clone(), database.clone(), name).await?;
        validate_table_read(ctx.clone(), &catalog, &database, name).await?;

        let Ok(fuse_table) = FuseTable::try_from_table(table.as_ref()) else {
            return Err(ErrorCode::BadArguments(format!(
                "data API reads Fuse tables only, `{database}`.`{name}` is a {} table",
                table.engine()
            )));
        };
        if has_security_policy(&table.get_table_info().meta) {
            // The policies are applied by the SQL planner; the data API must not bypass them.
            return Err(ErrorCode::BadArguments(format!(
                "table `{database}`.`{name}` has a row access or masking policy, query it with SQL"
            )));
        }
        let snapshot = fuse_table.read_table_snapshot().await?;
        Ok(TableTarget {
            ctx,
            table,
            snapshot,
        })
    }

    pub fn table(&self) -> &dyn Table {
        self.table.as_ref()
    }

    pub fn fuse_table(&self) -> &FuseTable {
        FuseTable::try_from_table(self.table.as_ref()).expect("checked by resolve")
    }

    /// Every block of the snapshot, in snapshot order (segment by segment).
    pub async fn blocks(&self) -> Result<Vec<Arc<BlockMeta>>> {
        let Some(snapshot) = &self.snapshot else {
            return Ok(vec![]);
        };
        let fuse_table = self.fuse_table();
        let cache = FuseCaches::instance()
            .segments
            .with_policy(self.ctx.cache_policy());
        let reader = SegmentInfoReader::new(fuse_table.get_operator(), fuse_table.schema());
        let segments = snapshot
            .segments
            .iter()
            .cloned()
            .map(|(location, version)| {
                let cache = cache.clone();
                let reader = reader.clone();
                async move {
                    if matches!(
                        segment_format_from_location(&location),
                        FuseSegmentFormat::Column
                    ) {
                        return Err(ErrorCode::Unimplemented(
                            "data API does not read column-oriented segments yet",
                        ));
                    }
                    if let Some(blocks) = cache.lookup_block_metas(&location) {
                        return Ok(blocks);
                    }
                    let segment = cache.get(&reader, (location.clone(), version)).await?;
                    cache.block_metas(&location, || segment.block_metas())
                }
            });
        let segments = try_join_all(segments).await?;
        Ok(segments
            .iter()
            .flat_map(|blocks| blocks.iter().cloned())
            .collect())
    }
}

/// The table `catalog.database.name`, from the [`TableCache`] when the session accepts
/// one resolved within its `data_api_table_cache_ttl_ms`.
async fn resolve_table(
    ctx: &QueryContext,
    catalog: String,
    database: String,
    name: &str,
) -> Result<Arc<dyn Table>> {
    let max_age = ctx.get_settings().get_data_api_table_cache_ttl_ms()?;
    // A temporary table of the session shadows the table of the same name for this
    // session only, so it is never shared.
    if max_age == 0 || ctx.is_temp_table(&catalog, &database, name) {
        return ctx.get_table(&catalog, &database, name).await;
    }
    let key = TableKey {
        tenant: ctx.get_tenant().tenant_name().to_string(),
        catalog,
        database,
        table: name.to_string(),
    };
    let cache = TableCache::instance();
    if let Some(table) = cache.get(&key, Duration::from_millis(max_age)) {
        return Ok(table);
    }
    let table = ctx.get_table(&key.catalog, &key.database, name).await?;
    cache.insert(key, table.clone());
    Ok(table)
}

fn has_security_policy(meta: &TableMeta) -> bool {
    meta.row_access_policy.is_some()
        || meta.row_access_policy_columns_ids.is_some()
        || meta
            .column_mask_policy
            .as_ref()
            .is_some_and(|policies| !policies.is_empty())
        || !meta.column_mask_policy_columns_ids.is_empty()
}
