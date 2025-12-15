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

use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::table::DistributionLevel;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_meta_app::tenant::Tenant;
use fastrace::func_name;
use poem::IntoResponse;
use poem::web::Json;
use poem::web::Path;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct TenantTablesStatsResponse {
    pub databases: u64,
    pub internal: BTreeMap<String, TenantTableStatsInfo>,
    pub external: BTreeMap<String, TenantTableStatsInfo>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct TenantTableStatsInfo {
    pub tables: u64,
    pub rows: u64,
    pub data_bytes: u64,
    pub compressed_data_bytes: u64,
    pub index_bytes: u64,
    pub number_of_blocks: u64,
    pub number_of_segments: u64,
}

impl TenantTableStatsInfo {
    pub fn merge(&mut self, other: &TableStatistics) {
        self.tables += 1;
        self.rows += other.number_of_rows;
        self.data_bytes += other.data_bytes;
        self.compressed_data_bytes += other.compressed_data_bytes;
        self.index_bytes += other.index_data_bytes;
        self.number_of_blocks += other.number_of_blocks.unwrap_or(0);
        self.number_of_segments += other.number_of_segments.unwrap_or(0);
    }
}

async fn load_tenant_tables_stats(tenant: &Tenant) -> Result<TenantTablesStatsResponse> {
    let catalog = CatalogManager::instance().get_default_catalog(Default::default())?;

    let databases = catalog.list_databases(tenant).await?;

    let mut database_count = 0;
    let mut internal_stats: BTreeMap<String, TenantTableStatsInfo> = BTreeMap::new();
    let mut external_stats: BTreeMap<String, TenantTableStatsInfo> = BTreeMap::new();
    let mut warnings: Vec<String> = vec![];

    let system_dbs = ["information_schema", "system"];
    for database in databases {
        let db_name = database.name().to_lowercase();
        if system_dbs.contains(&db_name.as_str()) {
            continue;
        }
        database_count += 1;
        let tables = match catalog.list_tables(tenant, database.name()).await {
            Ok(v) => v,
            Err(err) => {
                warnings.push(format!(
                    "failed to list tables of database {}.{}: {}",
                    tenant.tenant_name(),
                    database.name(),
                    err
                ));
                continue;
            }
        };
        for table in tables {
            // local tables are not included in the stats
            if matches!(table.distribution_level(), DistributionLevel::Local) {
                continue;
            }
            let engine = table.engine().to_string();
            let stats = &table.get_table_info().meta.statistics;
            let is_external = table.get_table_info().meta.storage_params.is_some();
            if is_external {
                external_stats.entry(engine).or_default().merge(stats);
            } else {
                internal_stats.entry(engine).or_default().merge(stats);
            }
        }
    }

    Ok(TenantTablesStatsResponse {
        databases: database_count,
        internal: internal_stats,
        external: external_stats,
        warnings,
    })
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn get_tenant_tables_stats_handler(
    Path(tenant): Path<String>,
) -> poem::Result<impl IntoResponse> {
    let tenant =
        Tenant::new_or_err(&tenant, func_name!()).map_err(poem::error::InternalServerError)?;

    let resp = load_tenant_tables_stats(&tenant)
        .await
        .map_err(poem::error::InternalServerError)?;
    Ok(Json(resp))
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn get_tables_stats_handler() -> poem::Result<impl IntoResponse> {
    let tenant = &GlobalConfig::instance().query.tenant_id;

    let resp = load_tenant_tables_stats(tenant)
        .await
        .map_err(poem::error::InternalServerError)?;
    Ok(Json(resp))
}
