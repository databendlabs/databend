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
use databend_common_exception::Result;
use databend_common_users::Object;
use poem::IntoResponse;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::http::v1::HttpQueryContext;

#[derive(Deserialize, Clone)]
struct SearchDatabasesRequest {
    pub keyword: String,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct SearchDatabasesResponse {
    pub databases: Vec<DatabaseInfo>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct DatabaseInfo {
    pub name: String,
}

#[async_backtrace::framed]
async fn handle(ctx: &HttpQueryContext, keyword: String) -> Result<SearchDatabasesResponse> {
    let tenant = ctx.session.get_current_tenant();
    let visibility_checker = ctx
        .session
        .get_visibility_checker(false, Object::All)
        .await?;

    let catalog = CatalogManager::instance().get_default_catalog(Default::default())?;

    let mut databases = vec![];
    let warnings = vec![];

    for db in catalog.list_databases(&tenant).await? {
        if !db.name().contains(&keyword) {
            continue;
        }
        if !visibility_checker.check_database_visibility(
            catalog.name().as_str(),
            db.name(),
            db.get_db_info().database_id.db_id,
        ) {
            continue;
        }
        databases.push(DatabaseInfo {
            name: db.name().to_string(),
        });
    }

    Ok(SearchDatabasesResponse {
        databases,
        warnings,
    })
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn search_databases_handler(
    ctx: &HttpQueryContext,
    Json(req): Json<SearchDatabasesRequest>,
) -> PoemResult<impl IntoResponse> {
    let resp = handle(ctx, req.keyword)
        .await
        .map_err(InternalServerError)?;
    Ok(Json(resp))
}
