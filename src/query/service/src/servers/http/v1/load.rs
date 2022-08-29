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
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::FormatFactory;
use common_io::prelude::parse_escape_string;
use common_io::prelude::FormatSettings;
use common_planners::InsertInputSource;
use common_planners::PlanNode;
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
use crate::interpreters::InterpreterFactory;
use crate::pipelines::SourcePipeBuilder;
use crate::servers::http::v1::multipart_format::MultipartFormat;
use crate::servers::http::v1::multipart_format::MultipartWorker;
use crate::sessions::QueryContext;
use crate::sessions::SessionType;
use crate::sessions::TableContext;
use crate::sql::PlanParser;

#[derive(Serialize, Deserialize, Debug)]
pub struct LoadResponse {
    pub id: String,
    pub state: String,
    pub stats: ProgressValues,
    pub error: Option<String>,
}

fn get_input_format(node: &PlanNode) -> Result<&str> {
    match node {
        PlanNode::Insert(insert) => match &insert.source {
            InsertInputSource::StreamingWithFormat(format) => Ok(format),
            _ => Err(ErrorCode::UnknownFormat("Not found format name in plan")),
        },
        _ => Err(ErrorCode::UnknownFormat("Not found format name in plan")),
    }
}

#[allow(clippy::manual_async_fn)]
fn execute_query(
    context: Arc<QueryContext>,
    node: PlanNode,
    source_builder: SourcePipeBuilder,
) -> impl Future<Output = Result<()>> {
    async move {
        let interpreter = InterpreterFactory::get(context, node)?;

        if let Err(cause) = interpreter.start().await {
            error!("interpreter.start error: {:?}", cause);
        }

        // TODO(Winter): very hack code. need remove it.
        interpreter.set_source_pipe_builder(Option::from(source_builder))?;

        let mut data_stream = interpreter.execute().await?;

        while let Some(_block) = data_stream.next().await {}

        // Write Finish to query log table.
        if let Err(cause) = interpreter.finish().await {
            error!("interpreter.finish error: {:?}", cause);
        }

        Ok(())
    }
}

async fn new_processor_format(
    ctx: &Arc<QueryContext>,
    node: &PlanNode,
    multipart: Multipart,
) -> Result<Json<LoadResponse>> {
    let format = get_input_format(node)?;
    let format_settings = ctx.get_format_settings()?;

    let (mut worker, builder) =
        format_source_pipe_builder(format, ctx, node.schema(), multipart, &format_settings)?;

    let handler = ctx.spawn(execute_query(ctx.clone(), node.clone(), builder));

    worker.work().await;

    match handler.await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(cause)) => Err(cause),
        Err(_) => Err(ErrorCode::TokioError("Maybe panic.")),
    }?;

    Ok(Json(LoadResponse {
        error: None,
        state: "SUCCESS".to_string(),
        id: uuid::Uuid::new_v4().to_string(),
        stats: ctx.get_scan_progress_value(),
    }))
}

#[poem::handler]
pub async fn streaming_load(
    ctx: &HttpQueryContext,
    req: &Request,
    mut multipart: Multipart,
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

    let mut plan = PlanParser::parse(context.clone(), insert_sql)
        .await
        .map_err(InternalServerError)?;
    context.attach_query_str(insert_sql);

    // Block size.
    let _max_block_size = settings.get_max_block_size().map_err(InternalServerError)? as usize;

    let format_settings = context.get_format_settings().map_err(InternalServerError)?;
    let source_pipe_builder: SourcePipeBuilder = match &mut plan {
        PlanNode::Insert(insert) => match &mut insert.source {
            InsertInputSource::StreamingWithFormat(format) => {
                if FormatFactory::instance().has_input(format.as_str()) {
                    let new_format = format!("{}WithNames", format);
                    if format_settings.skip_header > 0
                        && FormatFactory::instance().has_input(new_format.as_str())
                    {
                        *format = new_format;
                    }

                    return match new_processor_format(&context, &plan, multipart).await {
                        Ok(res) => Ok(res),
                        Err(cause) => Err(InternalServerError(cause)),
                    };
                }

                Err(poem::Error::from_string(
                    format!(
                        "Streaming load only supports csv format, but got {}",
                        format
                    ),
                    StatusCode::BAD_REQUEST,
                ))
            }
            _non_supported_source => Err(poem::Error::from_string(
                "Only supports streaming upload. e.g. INSERT INTO $table FORMAT CSV",
                StatusCode::BAD_REQUEST,
            )),
        },
        non_insert_plan => Err(poem::Error::from_string(
            format!(
                "Only supports INSERT statement in streaming load, but got {}",
                non_insert_plan.name()
            ),
            StatusCode::BAD_REQUEST,
        )),
    }?;
    let interpreter =
        InterpreterFactory::get(context.clone(), plan.clone()).map_err(InternalServerError)?;
    let _ = interpreter
        .set_source_pipe_builder(Some(source_pipe_builder))
        .map_err(|e| error!("interpreter.set_source_pipe_builder.error: {:?}", e));
    interpreter.start().await.map_err(InternalServerError)?;
    let mut data_stream = interpreter.execute().await.map_err(InternalServerError)?;
    while let Some(_block) = data_stream.next().await {}
    // Write Finish to query log table.
    let _ = interpreter
        .finish()
        .await
        .map_err(|e| error!("interpreter.finish error: {:?}", e));

    // TODO generate id
    // TODO duplicate by insert_label
    let mut id = uuid::Uuid::new_v4().to_string();
    Ok(Json(LoadResponse {
        id,
        state: "SUCCESS".to_string(),
        stats: context.get_scan_progress_value(),
        error: None,
    }))
}

fn format_source_pipe_builder(
    format: &str,
    context: &Arc<QueryContext>,
    schema: DataSchemaRef,
    multipart: Multipart,
    format_settings: &FormatSettings,
) -> Result<(Box<dyn MultipartWorker>, SourcePipeBuilder)> {
    MultipartFormat::input_sources(
        format,
        context.clone(),
        multipart,
        schema,
        format_settings.clone(),
    )
}
