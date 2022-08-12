// Copyright 2022 Datafuse Labs.
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

use chrono::DateTime;
use chrono::Utc;
use common_catalog::catalog::{CATALOG_DEFAULT, CatalogManager};
use common_exception::Result;
use poem::web::Data;
use poem::web::Json;
use poem::web::Path;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;
use crate::catalogs::CatalogManagerHelper;

use crate::sessions::SessionManager;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct TenantTablesResponse {
    pub tables: Vec<TenantTableInfo>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct TenantTableInfo {
    pub table: String,
    pub database: String,
    pub engine: String,
    pub created_on: DateTime<Utc>,
    pub updated_on: DateTime<Utc>,
    pub rows: u64,
    pub data_bytes: u64,
    pub compressed_data_bytes: u64,
    pub index_bytes: u64,
}

async fn load_tenant_tables(tenant: &str) -> Result<Vec<TenantTableInfo>> {
    let catalog = CatalogManager::instance()?
        .get_catalog(CATALOG_DEFAULT)?;
    let databases = catalog.list_databases(tenant).await?;

    let mut table_infos: Vec<TenantTableInfo> = vec![];
    for database in databases {
        let tables = catalog.list_tables(tenant, database.name()).await?;
        for table in tables {
            let stats = &table.get_table_info().meta.statistics;
            table_infos.push(TenantTableInfo {
                table: table.name().to_string(),
                database: database.name().to_string(),
                engine: table.engine().to_string(),
                created_on: table.get_table_info().meta.created_on,
                updated_on: table.get_table_info().meta.updated_on,
                rows: stats.number_of_rows,
                data_bytes: stats.data_bytes,
                compressed_data_bytes: stats.compressed_data_bytes,
                index_bytes: stats.index_data_bytes,
            });
        }
    }
    Ok(table_infos)
}

// This handler returns the statistics about the tables of a tenant. It's only enabled in management mode.
#[poem::handler]
pub async fn list_tenant_tables_handler(
    Path(tenant): Path<String>,
    session_mgr: Data<&Arc<SessionManager>>,
) -> poem::Result<impl IntoResponse> {
    let tables = load_tenant_tables(&tenant)
        .await
        .map_err(poem::error::InternalServerError)?;
    Ok(Json(TenantTablesResponse { tables }))
}

// This handler returns the statistics about the tables of the current tenant.
#[poem::handler]
pub async fn list_tables_handler(
    session_mgr: Data<&Arc<SessionManager>>,
) -> poem::Result<impl IntoResponse> {
    let conf = session_mgr.get_conf();
    let tenant = &conf.query.tenant_id;
    if tenant.is_empty() {
        return Ok(Json(TenantTablesResponse { tables: vec![] }));
    }

    let tables = load_tenant_tables(tenant)
        .await
        .map_err(poem::error::InternalServerError)?;
    Ok(Json(TenantTablesResponse { tables }))
}
