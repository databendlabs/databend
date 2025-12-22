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

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct CatalogStatsResponse {
    pub tables: u64,
    pub databases: u64,
}

#[async_backtrace::framed]
async fn handle(ctx: &HttpQueryContext) -> Result<CatalogStatsResponse> {
    let tenant = ctx.session.get_current_tenant();
    let visibility_checker = ctx
        .session
        .get_visibility_checker(false, Object::All)
        .await?;

    let catalog = CatalogManager::instance().get_default_catalog(Default::default())?;

    let mut tables: u64 = 0;
    let mut databases: u64 = 0;

    for db in catalog.list_databases(&tenant).await? {
        if !visibility_checker.check_database_visibility(
            catalog.name().as_str(),
            db.name(),
            db.get_db_info().database_id.db_id,
        ) {
            continue;
        }
        databases += 1;
        for table in db.list_tables().await? {
            if !visibility_checker.check_table_visibility(
                catalog.name().as_str(),
                db.name(),
                table.name(),
                db.get_db_info().database_id.db_id,
                table.get_table_info().ident.table_id,
            ) {
                continue;
            }
            tables += 1;
        }
    }

    Ok(CatalogStatsResponse { tables, databases })
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn catalog_stats_handler(ctx: &HttpQueryContext) -> PoemResult<impl IntoResponse> {
    let resp = handle(ctx).await.map_err(InternalServerError)?;
    Ok(Json(resp))
}
