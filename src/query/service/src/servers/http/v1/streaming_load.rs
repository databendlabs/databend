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

// Logs from this module will show up as "[HTTP-STREAMING-LOAD] ...".
databend_common_tracing::register_module_tag!("[HTTP-STREAMING-LOAD]");

use std::future::Future;
use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_base::headers::HEADER_QUERY_CONTEXT;
use databend_common_base::headers::HEADER_SQL;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_catalog::session_type::SessionType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_sql::Planner;
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::Plan;
use databend_common_storages_stage::BytesBatch;
use databend_storages_common_session::TxnState;
use fastrace::future::FutureExt;
use futures::StreamExt;
use http::StatusCode;
use log::debug;
use log::info;
use log::warn;
use poem::IntoResponse;
use poem::Request;
use poem::Response;
use poem::error::BadRequest;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Multipart;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::Sender;

use super::HttpQueryContext;
use super::HttpSessionConf;
use super::HttpSessionStateInternal;
use crate::interpreters::InterpreterFactory;
use crate::servers::http::error::HttpErrorCode;
use crate::servers::http::error::JsonErrorOnly;
use crate::servers::http::error::QueryError;
use crate::servers::http::middleware::json_header::encode_json_header;
use crate::servers::http::middleware::sanitize_request_headers;
use crate::servers::http::v1::http_query_handlers::get_http_tracing_span;
use crate::sessions::QueriesQueueManager;
use crate::sessions::QueryContext;
use crate::sessions::QueryEntry;
use crate::sessions::TableContext;

#[derive(Serialize, Deserialize, Debug)]
pub struct LoadResponse {
    pub id: String,
    pub stats: ProgressValues,
}

#[allow(clippy::manual_async_fn)]
fn execute_query(
    http_query_context: HttpQueryContext,
    query_context: Arc<QueryContext>,
    plan: Plan,
    mem_stat: Arc<MemStat>,
) -> impl Future<Output = Result<()>> {
    let id = http_query_context.query_id.clone();
    let fut = async move {
        let interpreter = InterpreterFactory::get(query_context.clone(), &plan).await?;

        let mut data_stream = interpreter.execute(query_context).await?;

        while let Some(_block) = data_stream.next().await {}

        Ok(())
    };
    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.query_id = Some(id.clone());
    tracking_payload.mem_stat = Some(mem_stat);

    let root = get_http_tracing_span("http::execute_query", &http_query_context, &id);
    tracking_payload.tracking(fut.in_span(root))
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn streaming_load_handler(
    ctx: &HttpQueryContext,
    req: &Request,
    mut multipart: Multipart,
) -> Response {
    let query_mem_stat = MemStat::create(ctx.query_id.clone());
    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.query_id = Some(ctx.query_id.clone());
    tracking_payload.mem_stat = Some(query_mem_stat.clone());

    let root = get_http_tracing_span("http::streaming_load_handler", ctx, &ctx.query_id);
    let mut session_conf: Option<HttpSessionConf> =
        match req.headers().get(HEADER_QUERY_CONTEXT) {
            Some(v) => {
                let s = v.to_str().unwrap().to_string();
                match serde_json::from_str(s.trim()) {
                    Ok(s) => Some(s),
                    Err(e) => return poem::Error::from_string(
                        format!(
                            "invalid value for header {HEADER_QUERY_CONTEXT}({s}) in request: {e}"
                        ),
                        StatusCode::BAD_REQUEST,
                    )
                    .into_response(),
                }
            }
            None => None,
        };
    let res = tracking_payload
        .tracking(
            streaming_load_handler_inner(ctx, req, multipart, query_mem_stat, &session_conf)
                .in_span(root),
        )
        .await;
    let is_failed = res.is_err();

    let mut resp = match res {
        Ok(r) => r.into_response(),
        Err(err) => (
            err.status(),
            Json(JsonErrorOnly {
                error: QueryError {
                    code: err.status().as_u16(),
                    message: err.to_string(),
                    detail: None,
                },
            }),
        )
            .into_response(),
    };
    if let Some(s) = &mut session_conf {
        if let Some(internal) = &mut s.internal {
            internal.last_query_ids = vec![ctx.query_id.clone()];
        } else {
            s.internal = Some(HttpSessionStateInternal {
                last_query_ids: vec![ctx.query_id.clone()],
                ..Default::default()
            })
        }
        if is_failed {
            if let Some(t) = &mut s.txn_state {
                if *t == TxnState::Active {
                    *t = TxnState::Fail
                }
            }
        }
        let v = encode_json_header(&s);
        resp.headers_mut()
            .insert(HEADER_QUERY_CONTEXT, v.parse().unwrap());
    };
    resp
}

#[async_backtrace::framed]
async fn streaming_load_handler_inner(
    http_context: &HttpQueryContext,
    req: &Request,
    multipart: Multipart,
    mem_stat: Arc<MemStat>,
    session_conf: &Option<HttpSessionConf>,
) -> PoemResult<Json<LoadResponse>> {
    info!(
        "New streaming load request, headers={:?}",
        sanitize_request_headers(req.headers()),
    );
    let (_, query_context) = http_context
        .create_session(session_conf, SessionType::HTTPStreamingLoad)
        .await
        .map_err(HttpErrorCode::bad_request)?;

    let sql = req
        .headers()
        .get(HEADER_SQL)
        .ok_or(poem::Error::from_string(
            format!("Missing required header {HEADER_SQL} in request"),
            StatusCode::BAD_REQUEST,
        ))?;
    let sql = sql.to_str().map_err(|e| {
        poem::Error::from_string(
            format!("Invalid UTF-8 in value of header {HEADER_SQL}: {}", e),
            StatusCode::BAD_REQUEST,
        )
    })?;

    let settings = query_context.get_settings();

    let mut planner = Planner::new(query_context.clone());
    let (mut plan, extras) = planner
        .plan_sql(sql)
        .await
        .map_err(|err| err.display_with_sql(sql))
        .map_err(BadRequest)?;

    let entry = QueryEntry::create(&query_context, &plan, &extras).map_err(InternalServerError)?;
    let _guard = QueriesQueueManager::instance()
        .acquire(entry)
        .await
        .map_err(InternalServerError)?;

    let input_read_buffer_size = settings
        .get_input_read_buffer_size()
        .expect("get_input_read_buffer_size should not fail")
        as usize;

    match &mut plan {
        Plan::Insert(insert) => match &mut insert.source {
            InsertInputSource::StreamingLoad(streaming_load) => {
                if !streaming_load.file_format.support_streaming_load() {
                    return Err(poem::Error::from_string(
                        format!(
                            "Unsupported file format: {}",
                            streaming_load.file_format.get_type()
                        ),
                        StatusCode::BAD_REQUEST,
                    ));
                }
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                *streaming_load.receiver.lock() = Some(rx);

                let format = streaming_load.file_format.clone();
                let handler = query_context
                    .try_spawn(execute_query(
                        http_context.clone(),
                        query_context.clone(),
                        plan,
                        mem_stat,
                    ))
                    .map_err(InternalServerError)?;
                read_multi_part(multipart, &format, tx, input_read_buffer_size).await?;

                match handler.await {
                    Ok(Ok(_)) => Ok(Json(LoadResponse {
                        id: http_context.query_id.clone(),
                        stats: query_context.get_write_progress().get_values(),
                    })),
                    Ok(Err(cause)) => {
                        info!("Query execution failed: {:?}", cause);
                        Err(poem::Error::from_string(
                            format!(
                                "Query execution failed: {}",
                                cause.display_with_sql(sql).message()
                            ),
                            StatusCode::BAD_REQUEST,
                        ))
                    }
                    Err(err) => {
                        info!("Internal server error: {:?}", err);
                        Err(poem::Error::from_string(
                            "Internal server error: execution thread panicked",
                            StatusCode::INTERNAL_SERVER_ERROR,
                        ))
                    }
                }
            }
            _non_supported_source => Err(poem::Error::from_string(
                format!(
                    "Unsupported INSERT source. Streaming upload only supports 'INSERT INTO $table FILE_FORMAT = (type = <type> ...)'. Got: {}",
                    plan
                ),
                StatusCode::BAD_REQUEST,
            )),
        },
        non_insert_plan => Err(poem::Error::from_string(
            format!(
                "Only INSERT statements are supported in streaming load. Got: {}",
                non_insert_plan
            ),
            StatusCode::BAD_REQUEST,
        )),
    }
}

async fn read_multi_part(
    mut multipart: Multipart,
    file_format: &FileFormatParams,
    tx: Sender<Result<DataBlock>>,
    input_read_buffer_size: usize,
) -> poem::Result<()> {
    loop {
        match multipart.next_field().await {
            Err(cause) => {
                if let Err(cause) = tx
                    .send(Err(ErrorCode::BadBytes(format!(
                        "Failed to parse multipart data: {:?}",
                        cause
                    ))))
                    .await
                {
                    warn!("Multipart channel disconnected: {}", cause);
                }
                return Err(cause.into());
            }
            Ok(None) => {
                break;
            }
            Ok(Some(field)) => {
                let name = field.name();
                // resolve the ability to utilize name
                if name != Some("upload") {
                    return Err(poem::Error::from_string(
                        "Invalid field name in form-data: must be 'upload'",
                        StatusCode::BAD_REQUEST,
                    ));
                }
                let filename = field.file_name().unwrap_or("file_with_no_name").to_string();
                debug!("Started reading file: {}", &filename);
                let mut reader = field.into_async_read();
                match file_format {
                    FileFormatParams::Parquet(_)
                    | FileFormatParams::Avro(_)
                    | FileFormatParams::Orc(_) => {
                        let mut data = Vec::new();
                        let mut buf = vec![0; input_read_buffer_size];
                        loop {
                            let sz = reader.read(&mut buf[..]).await.map_err(BadRequest)?;
                            if sz > 0 {
                                data.extend_from_slice(&buf[..sz]);
                            } else {
                                break;
                            }
                        }
                        debug!("Read file {} with {} bytes", filename, data.len());
                        let batch = BytesBatch {
                            data,
                            path: filename.clone(),
                            offset: 0,
                            is_eof: true,
                        };
                        let block = DataBlock::empty_with_meta(Box::new(batch));
                        if let Err(e) = tx.send(Ok(block)).await {
                            warn!("Failed to send data to pipeline: {}", e);
                            break;
                        }
                    }
                    FileFormatParams::Csv(_)
                    | FileFormatParams::Tsv(_)
                    | FileFormatParams::NdJson(_) => {
                        let mut offset = 0;
                        loop {
                            let mut buf = vec![0; input_read_buffer_size];
                            let n = reader
                                .read(&mut buf[..])
                                .await
                                .map_err(InternalServerError)?;
                            buf.truncate(n);
                            let batch = BytesBatch {
                                data: buf.clone(),
                                path: filename.clone(),
                                offset,
                                is_eof: n == 0,
                            };
                            let block = DataBlock::empty_with_meta(Box::new(batch));
                            if let Err(e) = tx.send(Ok(block)).await {
                                warn!("Failed to send data to pipeline: {}", e);
                                // the caller get the actual error from interpreter
                                return Ok(());
                            }
                            if n == 0 {
                                debug!(
                                    "Finished reading file: {}, total size: {} bytes",
                                    &filename, offset
                                );
                                break;
                            }
                            offset += n;
                        }
                    }
                    _ => {
                        return Err(poem::Error::from_string(
                            format!(
                                "Unsupported file format for streaming load: {}",
                                file_format
                            ),
                            StatusCode::BAD_REQUEST,
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}
