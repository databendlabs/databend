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

//! `POST /v1/data/<op>`: the HTTP adapter of the [`crate::data`] operations. Each
//! operation takes its request as JSON and answers with the fields and rows like those of
//! `/v1/query`, plus its own stats.

use std::sync::Arc;

use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use http::StatusCode;
use poem::error::Result as PoemResult;
use poem::web::Json;
use serde::Serialize;

use crate::clusters::ClusterDiscovery;
use crate::data::search;
use crate::data::search::SearchRequest;
use crate::data::search::SearchStats;
use crate::servers::http::error::HttpErrorCode;
use crate::servers::http::v1::BlocksCollector;
use crate::servers::http::v1::BlocksSerializer;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::v1::QueryResponseField;
use crate::sessions::QueryContext;
use crate::sessions::TableContextQueryInfo;

/// The response of a data operation with stats `S`.
#[derive(Serialize)]
pub struct DataResponse<S> {
    pub schema: Vec<QueryResponseField>,
    pub data: BlocksSerializer,
    pub stats: S,
}

pub type DataSearchResponse = DataResponse<SearchStats>;

/// `POST /v1/data/search`: a [`SearchRequest`].
#[poem::handler]
#[async_backtrace::framed]
pub async fn data_search_handler(
    ctx: &HttpQueryContext,
    Json(request): Json<SearchRequest>,
) -> PoemResult<Json<DataSearchResponse>> {
    respond(
        async {
            let query_ctx = query_context(ctx)?;
            let output = search::execute(query_ctx.clone(), request).await?;
            response(&query_ctx, &output.schema, output.block, output.stats)
        }
        .await,
    )
}

/// Data operations run on this node only, so they skip cluster discovery and its meta-service
/// read.
fn query_context(ctx: &HttpQueryContext) -> Result<Arc<QueryContext>> {
    let cluster = ClusterDiscovery::local_cluster(&GlobalConfig::instance());
    ctx.session
        .create_query_context_with_cluster(cluster, ctx.version, None)
}

fn response<S>(
    query_ctx: &QueryContext,
    schema: &DataSchema,
    block: DataBlock,
    stats: S,
) -> Result<DataResponse<S>> {
    let format = query_ctx.get_output_format_settings()?;
    let mut collector = BlocksCollector::new();
    collector.append_block(block);
    Ok(DataResponse {
        schema: QueryResponseField::from_schema(schema),
        data: collector.into_serializer(format),
        stats,
    })
}

fn respond<T>(result: Result<T>) -> PoemResult<Json<T>> {
    result.map(Json).map_err(|error| {
        let status = status_of(&error);
        HttpErrorCode::new(error, status).into()
    })
}

fn status_of(error: &ErrorCode) -> StatusCode {
    match error.code() {
        ErrorCode::BAD_ARGUMENTS
        | ErrorCode::UNKNOWN_DATABASE
        | ErrorCode::UNKNOWN_TABLE
        | ErrorCode::UNKNOWN_COLUMN
        | ErrorCode::UNIMPLEMENTED
        | ErrorCode::REFRESH_INDEX_ERROR
        | ErrorCode::TANTIVY_QUERY_PARSER_ERROR => StatusCode::BAD_REQUEST,
        ErrorCode::PERMISSION_DENIED | ErrorCode::NEED_CHANGE_PASSWORD_DENIED => {
            StatusCode::FORBIDDEN
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
