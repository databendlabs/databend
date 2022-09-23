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

use common_base::base::tokio;
use common_base::base::tokio::io::AsyncRead;
use common_base::base::tokio::io::AsyncReadExt;
use common_base::base::ProgressValues;
use common_base::base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::parse_escape_string;
use common_pipeline_sources::processors::sources::input_formats::InputContext;
use common_pipeline_sources::processors::sources::input_formats::StreamingReadBatch;
use futures::StreamExt;
use poem::error::BadRequest;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::http::StatusCode;
use poem::web::Json;
use poem::web::Multipart;
use poem::Request;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::HttpQueryContext;
use crate::interpreters::InterpreterFactory;
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
        let interpreter = InterpreterFactory::get(context.clone(), &plan).await?;

        let mut data_stream = interpreter.execute(context).await?;

        while let Some(_block) = data_stream.next().await {}

        Ok(())
    }
}

fn remove_quote(s: &[u8]) -> &[u8] {
    let mut r = s;
    let l = r.len();
    if l > 1 {
        let a = r[0];
        let b = r[l - 1];
        if (a == b'"' && b == b'"') || (a == b'\'' && b == b'\'') {
            r = &r[1..l - 1]
        }
    }
    r
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
            let value = parse_escape_string(remove_quote(value.as_bytes()));
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
    context.attach_query_str(plan.to_string(), insert_sql);

    let schema = plan.schema();
    match &mut plan {
        Plan::Insert(insert) => match &mut insert.source {
            InsertInputSource::StreamingWithFormat(format, start, input_context_ref) => {
                let sql_rest = &insert_sql[*start..].trim();
                if !sql_rest.is_empty() {
                    return Err(poem::Error::from_string(
                        "should NOT have data after `Format` in streaming load.",
                        StatusCode::BAD_REQUEST,
                    ));
                };
                let (tx, rx) = tokio::sync::mpsc::channel(2);
                let input_context = Arc::new(
                    InputContext::try_create_from_insert(
                        format.as_str(),
                        rx,
                        context.get_settings(),
                        schema,
                        context.get_scan_progress(),
                    )
                    .await
                    .map_err(InternalServerError)?,
                );
                *input_context_ref = Some(input_context.clone());
                tracing::info!("streaming load {:?}", input_context);

                let handler = context.spawn(execute_query(context.clone(), plan));
                let files = read_multi_part(multipart, tx, &input_context).await?;

                match handler.await {
                    Ok(Ok(_)) => Ok(Json(LoadResponse {
                        error: None,
                        state: "SUCCESS".to_string(),
                        id: uuid::Uuid::new_v4().to_string(),
                        stats: context.get_scan_progress_value(),
                        files,
                    })),
                    Ok(Err(cause)) => Err(poem::Error::from_string(
                        format!("execute fail: {}", cause.message()),
                        StatusCode::BAD_REQUEST,
                    )),
                    Err(_) => Err(poem::Error::from_string(
                        "Maybe panic.",
                        StatusCode::INTERNAL_SERVER_ERROR,
                    )),
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

async fn read_multi_part(
    mut multipart: Multipart,
    tx: Sender<Result<StreamingReadBatch>>,
    input_context: &Arc<InputContext>,
) -> poem::Result<Vec<String>> {
    let mut files = vec![];
    loop {
        match multipart.next_field().await {
            Err(cause) => {
                if let Err(cause) = tx
                    .send(Err(ErrorCode::BadBytes(format!(
                        "Parse multipart error, cause {:?}",
                        cause
                    ))))
                    .await
                {
                    tracing::warn!("Multipart channel disconnect. {}", cause);
                }
                return Err(cause.into());
            }
            Ok(None) => {
                break;
            }
            Ok(Some(field)) => {
                let filename = field.file_name().unwrap_or("file_with_no_name").to_string();
                let compression = input_context
                    .get_compression_alg(&filename)
                    .map_err(BadRequest)?;
                tracing::debug!("Multipart start read {}", &filename);
                files.push(filename.clone());
                let mut async_reader = field.into_async_read();
                let mut is_start = true;
                loop {
                    let mut batch = vec![0u8; input_context.read_batch_size];
                    let n = read_full(&mut async_reader, &mut batch[0..])
                        .await
                        .map_err(InternalServerError)?;
                    if n == 0 {
                        break;
                    } else {
                        batch.truncate(n);
                        tracing::debug!("Multipart read {} bytes", n);
                        if let Err(e) = tx
                            .send(Ok(StreamingReadBatch {
                                data: batch,
                                path: filename.clone(),
                                is_start,
                                compression,
                            }))
                            .await
                        {
                            tracing::warn!(" Multipart fail to send ReadBatch: {}", e);
                        }
                        is_start = false;
                    }
                }
            }
        }
    }
    Ok(files)
}

pub async fn read_full<R: AsyncRead + Unpin>(reader: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut buf = &mut buf[0..];
    let mut n = 0;
    while !buf.is_empty() {
        let read = reader.read(buf).await?;
        if read == 0 {
            break;
        }
        n += read;
        buf = &mut buf[read..]
    }
    Ok(n)
}
