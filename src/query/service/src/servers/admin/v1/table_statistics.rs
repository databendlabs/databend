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

use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_system::StatisticsTable;
use databend_common_storages_system::TableColumnStatistics;
use poem::IntoResponse;
use poem::web::Json;
use poem::web::Path;

use crate::sessions::SessionManager;

#[poem::handler]
#[async_backtrace::framed]
pub async fn get_table_stats_handler(
    Path((tenant, database, table)): Path<(String, String, String)>,
) -> poem::Result<impl IntoResponse> {
    match dump_columns(&tenant, &database, &table).await {
        Ok(columns) => Ok(Json(columns)),
        Err(error) => Err(poem::error::InternalServerError(error)),
    }
}

async fn dump_columns(
    tenant: &str,
    database: &str,
    table: &str,
) -> Result<Vec<TableColumnStatistics>> {
    let dummy_session = SessionManager::instance()
        .create_session(SessionType::Dummy)
        .await?;

    let session = SessionManager::instance().register_session(dummy_session)?;

    let ctx: Arc<dyn TableContext> = session
        .create_query_context(&databend_common_version::BUILD_INFO)
        .await?;

    let catalog = CatalogManager::instance().get_default_catalog(Default::default())?;

    let table = catalog
        .get_table(&Tenant::new_literal(tenant), database, table)
        .await?;

    StatisticsTable::dump_table_columns(&ctx, &catalog, database, &table).await
}
