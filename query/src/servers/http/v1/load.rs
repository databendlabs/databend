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

use std::sync::Arc;

use async_compat::CompatExt;
use async_stream::stream;
use common_base::ProgressValues;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_io::prelude::parse_escape_string;
use common_io::prelude::FormatSettings;
use common_meta_types::UserInfo;
use common_planners::InsertInputSource;
use common_planners::PlanNode;
use common_streams::CsvSourceBuilder;
use common_streams::NDJsonSourceBuilder;
use common_streams::ParquetSourceBuilder;
use common_streams::SendableDataBlockStream;
use common_streams::Source;
use common_tracing::tracing;
use futures::io::Cursor;
use futures::StreamExt;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::http::StatusCode;
use poem::web::Data;
use poem::web::Json;
use poem::web::Multipart;
use poem::Request;
use serde::Deserialize;
use serde::Serialize;

use crate::interpreters::InterpreterFactory;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
use crate::sql::PlanParser;

#[derive(Serialize, Deserialize, Debug)]
pub struct LoadResponse {
    pub id: String,
    pub state: String,
    pub stats: ProgressValues,
    pub error: Option<String>,
}

#[poem::handler]
pub async fn streaming_load(
    req: &Request,
    mut multipart: Multipart,
    user_info: Data<&UserInfo>,
    sessions_extension: Data<&Arc<SessionManager>>,
) -> PoemResult<Json<LoadResponse>> {
    let session_manager = sessions_extension.0;
    let session = session_manager
        .create_session(SessionType::HTTPStreamingLoad)
        .await
        .map_err(InternalServerError)?;

    // TODO: list user's grant list and check client address
    session.set_current_user(user_info.0.clone());

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
            let value = value.to_str().unwrap();
            let value = value.trim_matches(|p| p == '"' || p == '\'');
            let value = parse_escape_string(value.as_bytes());
            settings
                .set_settings(key.to_string(), value, false)
                .map_err(InternalServerError)?
        }
    }

    let plan = PlanParser::parse(context.clone(), insert_sql)
        .await
        .map_err(InternalServerError)?;
    context.attach_query_str(insert_sql);

    // Block size.
    let max_block_size = context
        .get_settings()
        .get_max_block_size()
        .map_err(InternalServerError)? as usize;

    let format_settings = context.get_format_settings().map_err(InternalServerError)?;

    // validate plan
    let source_stream = match &plan {
        PlanNode::Insert(insert) => match &insert.source {
            InsertInputSource::StreamingWithFormat(format) => {
                if format.to_lowercase().as_str() == "csv" {
                    build_csv_stream(&plan, &format_settings, multipart, max_block_size)
                } else if format.to_lowercase().as_str() == "parquet" {
                    build_parquet_stream(&plan, multipart)
                } else if format.to_lowercase().as_str() == "ndjson"
                    || format.to_lowercase().as_str() == "jsoneachrow"
                {
                    build_ndjson_stream(&plan, multipart)
                } else {
                    Err(poem::Error::from_string(
                        format!(
                            "Streaming load only supports csv format, but got {}",
                            format
                        ),
                        StatusCode::BAD_REQUEST,
                    ))
                }
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

    // Write Start to query log table.
    let _ = interpreter
        .start()
        .await
        .map_err(|e| tracing::error!("interpreter.start.error: {:?}", e));

    // this runs inside the runtime of poem, load is not cpu densive so it's ok
    let mut data_stream = interpreter
        .execute(Some(source_stream))
        .await
        .map_err(InternalServerError)?;
    while let Some(_block) = data_stream.next().await {}

    // Write Finish to query log table.
    let _ = interpreter
        .finish()
        .await
        .map_err(|e| tracing::error!("interpreter.finish error: {:?}", e));

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

fn build_parquet_stream(
    plan: &PlanNode,
    mut multipart: Multipart,
) -> PoemResult<SendableDataBlockStream> {
    let builder = ParquetSourceBuilder::create(plan.schema());
    let stream = stream! {
        while let Ok(Some(field)) = multipart.next_field().await {
            let bytes = field.bytes().await.map_err_to_code(ErrorCode::BadBytes,  || "Read part to field bytes error")?;
            let cursor = Cursor::new(bytes);

            let mut source = builder.build(cursor)?;

            loop {
                let block = source.read().await;
                match block {
                    Ok(None) => break,
                    Ok(Some(b)) =>  yield(Ok(b)),
                    Err(e) => yield(Err(e)),
                }
            }
        }
    };

    Ok(Box::pin(stream))
}

fn build_ndjson_stream(
    plan: &PlanNode,
    mut multipart: Multipart,
) -> PoemResult<SendableDataBlockStream> {
    let builder = NDJsonSourceBuilder::create(plan.schema());
    let stream = stream! {
        while let Ok(Some(field)) = multipart.next_field().await {
            let bytes = field.bytes().await.map_err_to_code(ErrorCode::BadBytes,  || "Read part to field bytes error")?;
            let cursor = std::io::Cursor::new(bytes);
            let mut source = builder.build(cursor)?;

            loop {
                let block = source.read().await;
                match block {
                    Ok(None) => break,
                    Ok(Some(b)) =>  yield(Ok(b)),
                    Err(e) => yield(Err(e)),
                }
            }
        }
    };

    Ok(Box::pin(stream))
}

fn build_csv_stream(
    plan: &PlanNode,
    format_settings: &FormatSettings,
    mut multipart: Multipart,
    block_size: usize,
) -> PoemResult<SendableDataBlockStream> {
    let mut builder = CsvSourceBuilder::create(plan.schema(), format_settings.clone());
    builder.block_size(block_size);

    let stream = stream! {
        while let Ok(Some(field)) = multipart.next_field().await {
            let reader = field.into_async_read();
            let mut source = builder.build(reader.compat())?;

            loop {
                let block = source.read().await;
                match block {
                    Ok(None) => break,
                    Ok(Some(b)) =>  yield(Ok(b)),
                    Err(e) => yield(Err(e)),
                }
            }
        }
    };

    Ok(Box::pin(stream))
}
