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
use databend_common_catalog::catalog::CatalogManager;
use databend_common_exception::Result;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Path;
use poem::IntoResponse;
use serde::Serialize;

use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Eq, PartialEq, Debug, Default)]
pub struct ListDatabaseTablesResponse {
    pub tables: Vec<TableInfo>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Eq, PartialEq, Debug, Default)]
pub struct TableInfo {
    pub name: String,
    pub table_type: String,
    pub database: String,
    pub catalog: String,
    pub owner: String,
    pub engine: String,
    pub cluster_by: String,
    pub create_time: DateTime<Utc>,
    pub num_rows: u64,
    pub data_size: u64,
    pub data_compressed_size: u64,
    pub index_size: u64,
}

#[async_backtrace::framed]
async fn handle(ctx: &HttpQueryContext, database: String) -> Result<ListDatabaseTablesResponse> {
    let tenant = ctx.session.get_current_tenant();
    let user = ctx.session.get_current_user()?;
    let visibility_checker = ctx.session.get_visibility_checker(false).await?;

    let catalog = CatalogManager::instance().get_default_catalog(Default::default())?;
    let database = catalog.get_database(&tenant, &database).await?;

    if !visibility_checker.check_database_visibility(
        catalog.name().as_str(),
        database.name(),
        database.get_db_info().database_id.db_id,
    ) {
        return Ok(ListDatabaseTablesResponse {
            tables: vec![],
            warnings: vec![],
        });
    }

    let warnings = vec![];
    let tables = database
        .list_tables()
        .await?
        .into_iter()
        .filter(|tbl| {
            visibility_checker.check_table_visibility(
                catalog.name().as_str(),
                database.name(),
                tbl.name(),
                tbl.get_table_info().database_id.db_id,
                tbl.get_table_info().table_id,
            )
        })
        .map(|tbl| {
            let stats = tbl.get_table_info().meta.statistics;
            TableInfo {
                name: tbl.name().to_string(),
                table_type: tbl.table_type().to_string(),
                database: database.name().to_string(),
                catalog: catalog.name().to_string(),
                owner: user.name().to_string(),
                engine: tbl.engine().to_string(),
                cluster_by: tbl.cluster_by().to_string(),
                create_time: tbl.create_time(),
                num_rows: stats.num_rows,
                data_size: stats.data_size,
                data_compressed_size: stats.data_compressed_size,
                index_size: stats.index_size,
            }
        })
        .collect::<Vec<_>>();

    Ok(ListDatabaseTablesResponse { tables, warnings })
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn list_database_tables_handler(
    ctx: &HttpQueryContext,
    Path(database): Path<String>,
) -> PoemResult<impl IntoResponse> {
    let response = handle(ctx, database).await.map_err(InternalServerError)?;
    Ok(Json(response))
}
