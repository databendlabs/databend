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

use databend_common_base::runtime::GlobalQueryRuntime;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::infer_table_schema;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_users::Object;
use poem::IntoResponse;
use poem::error::InternalServerError;
use poem::error::NotFound;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Path;
use poem::web::Query;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::http::v1::HttpQueryContext;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;
use crate::sql::Planner;

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

#[derive(Deserialize, Debug, Default)]
pub struct ListDatabaseTableFieldsQuery {
    pub catalog: Option<String>,
    #[serde(default)]
    pub resolve_view: bool,
}

async fn resolve_catalog(
    ctx: &HttpQueryContext,
    catalog_name: &str,
) -> Result<(Arc<dyn Catalog>, Option<Arc<QueryContext>>)> {
    if catalog_name.eq_ignore_ascii_case(CATALOG_DEFAULT) {
        let catalog = CatalogManager::instance()
            .get_default_catalog(Default::default())?
            .disable_table_info_refresh()?;
        return Ok((catalog, None));
    }

    let query_ctx = ctx.session.create_query_context(ctx.version).await?;
    let catalog = query_ctx.get_catalog(catalog_name).await?;
    Ok((catalog, Some(query_ctx)))
}

async fn resolve_schema(
    ctx: &HttpQueryContext,
    query_ctx: Option<Arc<QueryContext>>,
    table: &dyn databend_common_catalog::table::Table,
    resolve_view: bool,
) -> Result<TableSchemaRef> {
    if !resolve_view || !table.engine().eq_ignore_ascii_case(VIEW_ENGINE) {
        return Ok(table.schema());
    }

    let query = table.options().get(QUERY).ok_or_else(|| {
        ErrorCode::Internal("Logical error, View Table must have a SelectQuery inside.")
    })?;
    let query_ctx = match query_ctx {
        Some(query_ctx) => query_ctx,
        None => ctx.session.create_query_context(ctx.version).await?,
    };
    let query = query.clone();
    GlobalQueryRuntime::instance()
        .runtime()
        .spawn(async move {
            let mut planner = Planner::new(query_ctx);
            let (plan, _) = planner.plan_sql(&query).await?;
            infer_table_schema(&plan.schema())
        })
        .await?
}

#[async_backtrace::framed]
async fn handle(
    ctx: &HttpQueryContext,
    database: String,
    table: String,
    query: ListDatabaseTableFieldsQuery,
) -> Result<ListDatabaseTableFieldsResponse> {
    let tenant = ctx.session.get_current_tenant();
    let visibility_checker = ctx
        .session
        .get_visibility_checker(false, Object::All)
        .await?;

    let catalog_name = query.catalog.as_deref().unwrap_or(CATALOG_DEFAULT);
    let (catalog, query_ctx) = resolve_catalog(ctx, catalog_name).await?;
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

    let schema = resolve_schema(ctx, query_ctx, tbl.as_ref(), query.resolve_view).await?;
    let warnings = vec![];
    let mut fields = vec![];
    for field in &schema.fields {
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
    Query(query): Query<ListDatabaseTableFieldsQuery>,
) -> PoemResult<impl IntoResponse> {
    let resp = handle(ctx, database, table, query)
        .await
        .map_err(|e| match e.code() {
            ErrorCode::UNKNOWN_CATALOG | ErrorCode::UNKNOWN_DATABASE | ErrorCode::UNKNOWN_TABLE => {
                NotFound(e)
            }
            _ => InternalServerError(e),
        })?;
    Ok(Json(resp))
}
