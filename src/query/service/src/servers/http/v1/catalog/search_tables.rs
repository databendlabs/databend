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
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::http::v1::HttpQueryContext;

#[derive(Deserialize, Clone)]
struct SearchTablesRequest {
    pub keywords: String,
}

#[derive(Serialize, Eq, PartialEq, Debug, Default)]
pub struct SearchTablesResponse {
    pub tables: Vec<TableInfo>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Eq, PartialEq, Debug, Default)]
pub struct TableInfo {
    pub catalog: String,
    pub database: String,
    pub database_id: u64,
    pub name: String,
    pub table_id: u64,
    pub total_columns: u64,
    pub engine: String,
    pub engine_full: String,
    pub cluster_by: String,
    pub is_transient: bool,
    pub is_attach: bool,
    pub created_on: DateTime<Utc>,
    pub dropped_on: Option<DateTime<Utc>>,
    pub updated_on: DateTime<Utc>,
    pub num_rows: u64,
    pub data_size: u64,
    pub data_compressed_size: u64,
    pub index_size: u64,
    pub number_of_segments: u64,
    pub number_of_blocks: u64,
    pub owner: String,
    pub comment: String,
    pub table_type: String,
}

#[async_backtrace::framed]
async fn handle(ctx: &HttpQueryContext, keywords: String) -> Result<SearchTablesResponse> {
    let tenant = ctx.session.get_current_tenant();
    let user = ctx.session.get_current_user()?;
    let visibility_checker = ctx.session.get_visibility_checker(false).await?;

    let catalog = CatalogManager::instance().get_default_catalog(Default::default())?;

    let tables = vec![];
    let warnings = vec![];

    Ok(SearchTablesResponse { tables, warnings })
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn search_tables_handler(
    ctx: &HttpQueryContext,
    Json(req): Json<SearchTablesRequest>,
) -> PoemResult<impl IntoResponse> {
    let resp = handle(ctx, req.keywords)
        .await
        .map_err(InternalServerError)?;
    Ok(Json(resp))
}
