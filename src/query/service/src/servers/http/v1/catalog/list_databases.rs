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
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::IntoResponse;
use serde::Serialize;

use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Eq, PartialEq, Debug, Default)]
pub struct ListDatabasesResponse {
    pub databases: Vec<DatabaseInfo>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Eq, PartialEq, Debug, Default)]
pub struct DatabaseInfo {
    pub name: String,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn list_databases_handler(ctx: &HttpQueryContext) -> PoemResult<impl IntoResponse> {
    let tenant = ctx.session.get_current_tenant();
    let user = ctx
        .session
        .get_current_user()
        .map_err(InternalServerError)?;
    let visibility_checker = ctx
        .session
        .get_visibility_checker(false)
        .await
        .map_err(InternalServerError)?;

    let catalog = CatalogManager::instance().get_default_catalog(Default::default())?;
    let dbs = catalog
        .list_databases(&tenant)
        .await
        .map_err(InternalServerError)?;

    let databases = dbs
        .into_iter()
        .filter(|db| {
            visibility_checker.check_database_visibility(
                catalog.name().as_str(),
                db.name(),
                db.get_db_info().database_id.db_id,
            )
        })
        .map(|db| DatabaseInfo {
            name: db.name().to_string(),
        })
        .collect::<Vec<_>>();
    let warnings = vec![];

    Ok(Json(ListDatabasesResponse {
        databases,
        warnings,
    }))
}
