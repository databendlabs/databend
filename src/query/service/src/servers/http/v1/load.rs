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

use std::future::Future;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_base::base::tokio::io::AsyncRead;
use databend_common_base::base::tokio::io::AsyncReadExt;
use databend_common_base::base::unescape_string;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::infer_table_schema;
use databend_common_pipeline_sources::input_formats::InputContext;
use databend_common_pipeline_sources::input_formats::StreamingReadBatch;
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::Plan;
use databend_common_sql::Planner;
use futures::StreamExt;
use http::StatusCode;
use log::debug;
use log::info;
use log::warn;
use poem::error::BadRequest;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Multipart;
use poem::Request;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use super::HttpQueryContext;
use crate::interpreters::InterpreterFactory;
use crate::servers::http::middleware::sanitize_request_headers;
use crate::sessions::QueriesQueueManager;
use crate::sessions::QueryContext;
use crate::sessions::QueryEntry;
use crate::sessions::SessionType;
use crate::sessions::TableContext;

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
#[async_backtrace::framed]
pub async fn streaming_load(
    ctx: &HttpQueryContext,
    req: &Request,
    mut multipart: Multipart,
) -> PoemResult<Json<LoadResponse>> {
    info!(
        "new streaming load request:, headers={:?}",
        sanitize_request_headers(req.headers()),
    );
    let session = ctx.upgrade_session(SessionType::HTTPStreamingLoad)?;
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
        if settings
            .has_setting(key.as_str())
            .map_err(InternalServerError)?
        {
            let value = value.to_str().map_err(InternalServerError)?;
            let unquote =
                std::str::from_utf8(remove_quote(value.as_bytes())).map_err(InternalServerError)?;
            let value = unescape_string(unquote).map_err(InternalServerError)?;
            settings
                .set_setting(key.to_string(), value.to_string())
                .map_err(InternalServerError)?
        }
    }

    let mut planner = Planner::new(context.clone());
    let (mut plan, extras) = planner
        .plan_sql(insert_sql)
        .await
        .map_err(|err| err.display_with_sql(insert_sql))
        .map_err(InternalServerError)?;

    let entry = QueryEntry::create(&context, &plan, &extras).map_err(InternalServerError)?;
    let _guard = QueriesQueueManager::instance()
        .acquire(entry)
        .await
        .map_err(InternalServerError)?;

    match &mut plan {
        Plan::Insert(insert) => {
            let schema = insert.dest_schema();
            match &mut insert.source {
                InsertInputSource::StreamingWithFileFormat {
                    format,
                    on_error_mode,
                    start,
                    input_context_option,
                } => {
                    let sql_rest = &insert_sql[*start..].trim();
                    if !sql_rest.is_empty() {
                        return Err(poem::Error::from_string(
                            "should NOT have data after `FILE_FORMAT` in streaming load.",
                            StatusCode::BAD_REQUEST,
                        ));
                    };
                    let to_table = context
                        .get_table(&insert.catalog, &insert.database, &insert.table)
                        .await
                        .map_err(|err| err.display_with_sql(insert_sql))
                        .map_err(InternalServerError)?;
                    let (tx, rx) = tokio::sync::mpsc::channel(2);

                    let table_schema = infer_table_schema(&schema)
                        .map_err(|err| err.display_with_sql(insert_sql))
                        .map_err(InternalServerError)?;
                    let input_context = Arc::new(
                        InputContext::try_create_from_insert_file_format(
                            context.clone(),
                            rx,
                            context.get_settings(),
                            format.clone(),
                            table_schema,
                            context.get_scan_progress(),
                            false,
                            to_table.get_block_thresholds(),
                            on_error_mode.clone(),
                        )
                        .await
                        .map_err(|err| err.display_with_sql(insert_sql))
                        .map_err(InternalServerError)?,
                    );
                    *input_context_option = Some(input_context.clone());
                    info!("streaming load with file_format {:?}", input_context);

                    let query_id = context.get_id();
                    let handler = context.spawn(query_id, execute_query(context.clone(), plan));
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
                            format!(
                                "execute fail: {}",
                                cause.display_with_sql(insert_sql).message()
                            ),
                            StatusCode::BAD_REQUEST,
                        )),
                        Err(_) => Err(poem::Error::from_string(
                            "Maybe panic.",
                            StatusCode::INTERNAL_SERVER_ERROR,
                        )),
                    }
                }
                InsertInputSource::StreamingWithFormat(_, _, _) => Err(poem::Error::from_string(
                    "'INSERT INTO $table FORMAT <type> is now only supported in clickhouse handler,\
                        please use 'FILE_FORMAT = (type = <type> ...)' instead.",
                    StatusCode::BAD_REQUEST,
                )),
                _non_supported_source => Err(poem::Error::from_string(
                    format!(
                        "streaming upload only support 'INSERT INTO $table FILE_FORMAT = (type = <type> ...)' got {}.",
                        plan
                    ),
                    StatusCode::BAD_REQUEST,
                )),
            }
        }
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
                    warn!("Multipart channel disconnect. {}", cause);
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
                debug!("Multipart start read {}", &filename);
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
                        debug!("Multipart read {} bytes", n);
                        if let Err(e) = tx
                            .send(Ok(StreamingReadBatch {
                                data: batch,
                                path: filename.clone(),
                                is_start,
                                compression,
                            }))
                            .await
                        {
                            warn!(" Multipart fail to send ReadBatch: {}", e);
                        }
                        is_start = false;
                    }
                }
            }
        }
    }
    Ok(files)
}

#[async_backtrace::framed]
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
