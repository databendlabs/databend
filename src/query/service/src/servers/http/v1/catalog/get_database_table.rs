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
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Path;
use poem::IntoResponse;
use serde::Serialize;

use crate::interpreters::ShowCreateQuerySettings;
use crate::interpreters::ShowCreateTableInterpreter;
use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Eq, PartialEq, Debug, Default)]
pub struct GetDatabaseTableResponse {
    pub table: Option<TableDetails>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Eq, PartialEq, Debug, Default)]
pub struct TableDetails {
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
    let user = ctx.session.get_current_user()?;
    let visibility_checker = ctx.session.get_visibility_checker(false).await?;

    let catalog = CatalogManager::instance().get_default_catalog(Default::default())?;

    let db = catalog.get_database(&tenant, &database).await?;
    if !visibility_checker.check_database_visibility(
        catalog.name().as_str(),
        db.name(),
        db.get_db_info().database_id.db_id,
    ) {
        return Ok(GetDatabaseTableResponse {
            table: None,
            warnings: vec![format!("database `{}` not found", database)],
        });
    }

    let tbl = db.get_table(&tenant, &table).await?;
    if !visibility_checker.check_table_visibility(
        catalog.name().as_str(),
        db.name(),
        tbl.name(),
        db.get_db_info().database_id.db_id,
        tbl.get_table_info().ident.table_id,
    ) {
        return Ok(GetDatabaseTableResponse {
            table: None,
            warnings: vec![format!("table `{}`.`{}` not found", database, table)],
        });
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
        tbl.name(),
        &settings,
    )
    .await
    .unwrap_or_else(|e| {
        let msg = format!(
            "show create query of {}.{}.{} failed(ignored): {}",
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
        table: Some(TableDetails {
            name: info.ident.table_name.clone(),
            database: info.ident.db_name.clone(),
            catalog: catalog.name().to_string(),
            engine: info.engine.to_string(),
            create_time: info.create_time,
            num_rows: info.num_rows,
            data_size: info.data_size,
            data_compressed_size: info.data_compressed_size,
            index_size: info.index_size,
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
        .map_err(InternalServerError)?;
    Ok(Json(resp))
}
