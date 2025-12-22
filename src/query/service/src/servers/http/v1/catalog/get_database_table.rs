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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_users::Object;
use poem::IntoResponse;
use poem::error::InternalServerError;
use poem::error::NotFound;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Path;
use serde::Deserialize;
use serde::Serialize;

use crate::interpreters::ShowCreateQuerySettings;
use crate::interpreters::ShowCreateTableInterpreter;
use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct GetDatabaseTableResponse {
    pub table: Option<TableDetail>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct TableDetail {
    pub name: String,
    pub database: String,
    pub catalog: String,
    pub engine: String,
    pub create_time: DateTime<Utc>,
    pub num_rows: u64,
    pub data_size: u64,
    pub data_compressed_size: u64,
    pub index_size: u64,
    pub create_query: String,
}

#[async_backtrace::framed]
async fn handle(
    ctx: &HttpQueryContext,
    database: String,
    table: String,
) -> Result<GetDatabaseTableResponse> {
    let tenant = ctx.session.get_current_tenant();
    let visibility_checker = ctx
        .session
        .get_visibility_checker(false, Object::All)
        .await?;

    let catalog = CatalogManager::instance().get_default_catalog(Default::default())?;

    let db = catalog.get_database(&tenant, &database).await?;
    if !visibility_checker.check_database_visibility(
        catalog.name().as_str(),
        db.name(),
        db.get_db_info().database_id.db_id,
    ) {
        return Err(ErrorCode::UnknownDatabase(format!(
            "[HTTP-CATALOG] Unknown database: '{}'",
            database
        )));
    }

    let tbl = db.get_table(&table).await?;
    if !visibility_checker.check_table_visibility(
        catalog.name().as_str(),
        db.name(),
        tbl.name(),
        db.get_db_info().database_id.db_id,
        tbl.get_table_info().ident.table_id,
    ) {
        return Err(ErrorCode::UnknownTable(format!(
            "[HTTP-CATALOG] Unknown table: '{}'",
            table
        )));
    }

    let info = tbl.get_table_info();
    let mut warnings = vec![];

    let settings = ShowCreateQuerySettings {
        sql_dialect: Dialect::PostgreSQL,
        force_quoted_ident: false,
        quoted_ident_case_sensitive: true,
        hide_options_in_show_create_table: false,
    };
    let create_query = ShowCreateTableInterpreter::show_create_query(
        catalog.as_ref(),
        db.name(),
        tbl.as_ref(),
        &settings,
    )
    .await
    .unwrap_or_else(|e| {
        let msg = format!(
            "[HTTP-CATALOG] Failed to generate CREATE query for table {}.{}.{}: {}",
            catalog.name(),
            database,
            table,
            e
        );
        log::warn!("{}", msg);
        warnings.push(msg);
        "".to_owned()
    });

    Ok(GetDatabaseTableResponse {
        table: Some(TableDetail {
            name: tbl.name().to_string(),
            database: db.name().to_string(),
            catalog: catalog.name().to_string(),
            engine: info.meta.engine.clone(),
            create_time: info.meta.created_on,
            num_rows: info.meta.statistics.number_of_rows,
            data_size: info.meta.statistics.data_bytes,
            data_compressed_size: info.meta.statistics.compressed_data_bytes,
            index_size: info.meta.statistics.index_data_bytes,
            create_query,
        }),
        warnings,
    })
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn get_database_table_handler(
    ctx: &HttpQueryContext,
    Path((database, table)): Path<(String, String)>,
) -> PoemResult<impl IntoResponse> {
    let resp = handle(ctx, database, table)
        .await
        .map_err(|e| match e.code() {
            ErrorCode::UNKNOWN_DATABASE | ErrorCode::UNKNOWN_TABLE => NotFound(e),
            _ => InternalServerError(e),
        })?;
    Ok(Json(resp))
}
