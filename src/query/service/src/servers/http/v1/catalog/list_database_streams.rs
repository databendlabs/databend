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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_users::Object;
use poem::error::InternalServerError;
use poem::error::NotFound;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Path;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct ListDatabaseStreamsResponse {
    pub streams: Vec<StreamInfo>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct StreamInfo {
    pub name: String,
    pub database: String,
    pub catalog: String,
    pub stream_id: u64,
    pub created_on: DateTime<Utc>,
    pub updated_on: DateTime<Utc>,
    pub mode: String,
    pub comment: String,
    pub table_name: Option<String>,
    pub table_id: Option<u64>,
    pub table_version: Option<u64>,
    pub snapshot_location: Option<String>,
    pub invalid_reason: String,
    pub owner: Option<String>,
}

#[async_backtrace::framed]
async fn handle(ctx: &HttpQueryContext, database: String) -> Result<ListDatabaseStreamsResponse> {
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

    let warnings = vec![];
    let tables = db.list_tables().await?;

    let streams = tables
        .into_iter()
        .filter(|tbl| {
            visibility_checker.check_table_visibility(
                catalog.name().as_str(),
                db.name(),
                tbl.name(),
                db.get_db_info().database_id.db_id,
                tbl.get_table_info().ident.table_id,
            ) && tbl.is_stream()
        })
        .filter_map(|tbl| {
            let info = tbl.get_table_info();

            // Try to convert to StreamTable, skip if conversion fails
            let stream_table =
                match databend_common_storages_stream::stream_table::StreamTable::try_from_table(
                    tbl.as_ref(),
                ) {
                    Ok(st) => st,
                    Err(_) => {
                        // Skip this table if it's not a valid stream
                        return None;
                    }
                };

            Some(StreamInfo {
                name: tbl.name().to_string(),
                database: db.name().to_string(),
                catalog: catalog.name().clone(),
                stream_id: info.ident.table_id,
                created_on: info.meta.created_on,
                updated_on: info.meta.updated_on,
                mode: stream_table.mode().to_string(),
                comment: info.meta.comment.clone(),
                table_name: stream_table.source_table_id().unwrap_or_default(),
                table_id: stream_table.source_table_id().ok(),
                table_version: stream_table.offset().ok(),
                snapshot_location: stream_table.snapshot_loc(),
                invalid_reason: String::new(), // This would need to be calculated
                owner: None,                   // This would need to be retrieved from ownership
            })
        })
        .collect::<Vec<_>>();

    Ok(ListDatabaseStreamsResponse { streams, warnings })
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn list_database_streams_handler(
    ctx: &HttpQueryContext,
    Path(database): Path<String>,
) -> PoemResult<impl IntoResponse> {
    let resp = handle(ctx, database).await.map_err(|e| match e.code() {
        ErrorCode::UNKNOWN_DATABASE => NotFound(e),
        _ => InternalServerError(e),
    })?;
    Ok(Json(resp))
}
