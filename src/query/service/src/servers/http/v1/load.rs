// Copyright 2021 Datafuse Labs.
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

use std::future::Future;
use std::sync::Arc;

use common_base::base::ProgressValues;
use common_base::base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::FormatFactory;
use common_io::prelude::parse_escape_string;
use futures::StreamExt;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::http::StatusCode;
use poem::web::Json;
use poem::web::Multipart;
use poem::Request;
use serde::Deserialize;
use serde::Serialize;
use tracing::error;

use super::HttpQueryContext;
use crate::interpreters::InterpreterFactoryV2;
use crate::servers::http::v1::multipart_format::MultipartFormat;
use crate::sessions::QueryContext;
use crate::sessions::SessionType;
use crate::sessions::TableContext;
use crate::sql::plans::InsertInputSource;
use crate::sql::plans::Plan;
use crate::sql::Planner;

#[derive(Serialize, Deserialize, Debug)]
pub struct LoadResponse {
    pub id: String,
    pub state: String,
    pub stats: ProgressValues,
    pub error: Option<String>,
    pub files: Vec<String>,
}

#[allow(clippy::manual_async_fn)]
fn execute_query(context: Arc<QueryContext>, plan: Plan) -> impl Future<Output = Result<()>> {
    async move {
        let interpreter = InterpreterFactoryV2::get(context.clone(), &plan).await?;

        if let Err(cause) = interpreter.start().await {
            error!("interpreter.start error: {:?}", cause);
        }

        let mut data_stream = interpreter.execute(context).await?;

        while let Some(_block) = data_stream.next().await {}

        // Write Finish to query log table.
        if let Err(cause) = interpreter.finish().await {
            error!("interpreter.finish error: {:?}", cause);
        }

        Ok(())
    }
}

#[poem::handler]
pub async fn streaming_load(
    ctx: &HttpQueryContext,
    req: &Request,
    multipart: Multipart,
) -> PoemResult<Json<LoadResponse>> {
    let session = ctx.get_session(SessionType::HTTPStreamingLoad);
    let context = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;

    let insert_sql = req
        .headers()
        .get("insert_sql")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let settings = context.get_settings();
    for (key, value) in req.headers().iter() {
        if settings.has_setting(key.as_str()) {
            let value = value.to_str().map_err(InternalServerError)?;
            let value = value.trim_matches(|p| p == '"' || p == '\'');
            let value = parse_escape_string(value.as_bytes());
            settings
                .set_settings(key.to_string(), value, false)
                .map_err(InternalServerError)?
        }
    }

    let mut planner = Planner::new(context.clone());
    let (mut plan, _, _) = planner
        .plan_sql(insert_sql)
        .await
        .map_err(InternalServerError)?;
    context.attach_query_str(insert_sql);

    let format_settings = context.get_format_settings().map_err(InternalServerError)?;
    let schema = plan.schema();
    match &mut plan {
        Plan::Insert(insert) => match &mut insert.source {
            InsertInputSource::StrWithFormat((sql_rest, format)) => {
                if !sql_rest.is_empty() {
                    Err(poem::Error::from_string(
                        "should NOT have data after `Format` in streaming load.",
                        StatusCode::BAD_REQUEST,
                    ))
                } else if FormatFactory::instance().has_input(format.as_str()) {
                    let new_format = format!("{}WithNames", format);
                    if format_settings.skip_header > 0
                        && FormatFactory::instance().has_input(new_format.as_str())
                    {
                        *format = new_format;
                    }

                    let format_settings =
                        context.get_format_settings().map_err(InternalServerError)?;

                    let (mut worker, builder) = MultipartFormat::input_sources(
                        format,
                        context.clone(),
                        multipart,
                        schema,
                        format_settings.clone(),
                    )
                    .map_err(InternalServerError)?;

                    insert.source = InsertInputSource::StreamingWithFormat(
                        format.to_string(),
                        builder.finalize(),
                    );

                    let handler = context.spawn(execute_query(context.clone(), plan));

                    worker.work().await;
                    let files = worker.get_files();

                    match handler.await {
                        Ok(Ok(_)) => Ok(Json(LoadResponse {
                            error: None,
                            state: "SUCCESS".to_string(),
                            id: uuid::Uuid::new_v4().to_string(),
                            stats: context.get_scan_progress_value(),
                            files,
                        })),
                        Ok(Err(cause)) => Err(cause),
                        Err(_) => Err(ErrorCode::TokioError("Maybe panic.")),
                    }
                    .map_err(InternalServerError)
                } else {
                    Err(poem::Error::from_string(
                        format!("format not supported for streaming load {}", format),
                        StatusCode::BAD_REQUEST,
                    ))
                }
            }
            _non_supported_source => Err(poem::Error::from_string(
                "Only supports streaming upload. e.g. INSERT INTO $table FORMAT CSV, got insert ... select.",
                StatusCode::BAD_REQUEST,
            )),
        },
        non_insert_plan => Err(poem::Error::from_string(
            format!(
                "Only supports INSERT statement in streaming load, but got {}",
                non_insert_plan
            ),
            StatusCode::BAD_REQUEST,
        )),
    }
}
