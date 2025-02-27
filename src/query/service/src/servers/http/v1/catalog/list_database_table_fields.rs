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
use poem::error::InternalServerError;
use poem::error::NotFound;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Path;
use poem::IntoResponse;
use serde::Serialize;

use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Eq, PartialEq, Debug, Default)]
pub struct ListDatabaseTableFieldsResponse {
    pub fields: Vec<FieldInfo>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Eq, PartialEq, Debug, Default)]
pub struct FieldInfo {
    pub name: String,
    pub r#type: String,
    pub nullable: bool,
    pub default: Option<String>,
}

#[async_backtrace::framed]
async fn handle(
    ctx: &HttpQueryContext,
    database: String,
    table: String,
) -> Result<ListDatabaseTableFieldsResponse> {
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
        return Err(ErrorCode::UnknownDatabase(format!(
            "Unknown database '{}'",
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
            "Unknown table '{}'",
            table
        )));
    }

    let warnings = vec![];
    let mut fields = vec![];
    for field in &tbl.get_table_info().schema().fields {
        fields.push(FieldInfo {
            name: field.name.clone(),
            r#type: field.data_type.to_string(),
            nullable: field.is_nullable(),
            default: field.default_expr.clone(),
        });
    }
    Ok(ListDatabaseTableFieldsResponse { fields, warnings })
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn list_database_table_fields_handler(
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
