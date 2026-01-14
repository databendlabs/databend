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

use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct ListDatabaseTableFieldsResponse {
    pub fields: Vec<FieldInfo>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
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

    let warnings = vec![];
    let mut fields = vec![];
    for field in &tbl.schema().fields {
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
