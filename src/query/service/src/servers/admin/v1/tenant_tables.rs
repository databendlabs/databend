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

use chrono::DateTime;
use chrono::Utc;
use databend_common_ast::parser::Dialect;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_meta_app::tenant::Tenant;
use databend_storages_common_txn::TxnManager;
use minitrace::func_name;
use poem::web::Json;
use poem::web::Path;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;

use crate::interpreters::ShowCreateQuerySettings;
use crate::interpreters::ShowCreateTableInterpreter;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct TenantTablesResponse {
    pub tables: Vec<TenantTableInfo>,
    pub warnings: Vec<String>,
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
    pub number_of_blocks: Option<u64>,
    pub number_of_segments: Option<u64>,
    pub table_id: u64,
    pub create_query: String,
}

async fn load_tenant_tables(tenant: &Tenant) -> Result<TenantTablesResponse> {
    let catalog = CatalogManager::instance().get_default_catalog(TxnManager::init())?;

    let databases = catalog.list_databases(tenant).await?;

    let mut table_infos: Vec<TenantTableInfo> = vec![];
    let mut warnings: Vec<String> = vec![];

    let settings = ShowCreateQuerySettings {
        sql_dialect: Dialect::PostgreSQL,
        quoted_ident_case_sensitive: true,
        hide_options_in_show_create_table: false,
    };

    for database in databases {
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
            let create_query = ShowCreateTableInterpreter::show_create_query(
                database.name(),
                table.as_ref(),
                &settings,
            )?;

            let table_id = table.get_table_info().ident.table_id;
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
                number_of_blocks: stats.number_of_blocks,
                number_of_segments: stats.number_of_segments,
                table_id,
                create_query,
            });
        }
    }
    Ok(TenantTablesResponse {
        tables: table_infos,
        warnings,
    })
}

// This handler returns the statistics about the tables of a tenant. It's only enabled in management mode.
#[poem::handler]
#[async_backtrace::framed]
pub async fn list_tenant_tables_handler(
    Path(tenant): Path<String>,
) -> poem::Result<impl IntoResponse> {
    let tenant =
        Tenant::new_or_err(&tenant, func_name!()).map_err(poem::error::InternalServerError)?;

    let resp = load_tenant_tables(&tenant)
        .await
        .map_err(poem::error::InternalServerError)?;
    Ok(Json(resp))
}

// This handler returns the statistics about the tables of the current tenant.
#[poem::handler]
#[async_backtrace::framed]
pub async fn list_tables_handler() -> poem::Result<impl IntoResponse> {
    let tenant = &GlobalConfig::instance().query.tenant_id;

    let resp = load_tenant_tables(tenant)
        .await
        .map_err(poem::error::InternalServerError)?;
    Ok(Json(resp))
}
