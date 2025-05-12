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
use databend_common_base::base::tokio::io::AsyncReadExt;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::Plan;
use databend_common_sql::Planner;
use databend_common_storages_stage::BytesBatch;
use fastrace::func_path;
use fastrace::future::FutureExt;
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
    let _tracking_guard = ThreadTracker::tracking(tracking_payload);
    let root = crate::servers::http::v1::http_query_handlers::get_http_tracing_span(
        func_path!(),
        &http_query_context,
        &id,
    );
    ThreadTracker::tracking_future(fut.in_span(root))
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn streaming_load_handler(
    ctx: &HttpQueryContext,
    req: &Request,
    mut multipart: Multipart,
) -> PoemResult<Json<LoadResponse>> {
    let query_mem_stat = MemStat::create(ctx.query_id.clone());
    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.query_id = Some(ctx.query_id.clone());
    tracking_payload.mem_stat = Some(query_mem_stat.clone());
    let _tracking_guard = ThreadTracker::tracking(tracking_payload);
    let root = crate::servers::http::v1::http_query_handlers::get_http_tracing_span(
        func_path!(),
        ctx,
        &ctx.query_id,
    );
    ThreadTracker::tracking_future(
        streaming_load_handler_inner(ctx, req, multipart, query_mem_stat).in_span(root),
    )
    .await
}

#[async_backtrace::framed]
pub async fn streaming_load_handler_inner(
    http_context: &HttpQueryContext,
    req: &Request,
    multipart: Multipart,
    mem_stat: Arc<MemStat>,
) -> PoemResult<Json<LoadResponse>> {
    info!(
        "new streaming load request, headers={:?}",
        sanitize_request_headers(req.headers()),
    );

    let session = http_context.upgrade_session(SessionType::HTTPStreamingLoad)?;
    let query_context = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;

    let sql = req.headers().get("sql").ok_or(poem::Error::from_string(
        "require HEADER `sql`",
        StatusCode::BAD_REQUEST,
    ))?;
    let sql = sql.to_str().map_err(|e| {
        poem::Error::from_string(
            format!("value of HEADER `sql` not a valid UTF8 string: {}", e),
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
        Plan::Insert(insert) => {
            match &mut insert.source {
                InsertInputSource::StreamingLoad {
                    file_format: format,
                    receiver,
                    ..
                } => {
                    if !matches!(&**format, FileFormatParams::Csv(_) | FileFormatParams::Tsv(_) | FileFormatParams::NdJson(_))  {
                        return Err(poem::Error::from_string( format!( "file format {} not supported yet", format.get_type() ), StatusCode::BAD_REQUEST));
                    }
                    let (tx, rx) = tokio::sync::mpsc::channel(1);
                    *receiver.lock() = Some(rx);

                    let format = format.clone();
                    let handler = query_context.spawn(execute_query(http_context.clone(), query_context.clone(), plan, mem_stat));
                    read_multi_part(multipart, &format, tx, input_read_buffer_size).await?;

                    match handler.await {
                        Ok(Ok(_)) => Ok(Json(LoadResponse {
                            id: http_context.query_id.clone(),
                            stats: query_context.get_write_progress().get_values(),
                        })),
                        Ok(Err(cause)) => Err(poem::Error::from_string(
                            format!(
                                "execute fail: {}",
                                cause.display_with_sql(sql).message()
                            ),
                            StatusCode::BAD_REQUEST,
                        )),
                        Err(_) => Err(poem::Error::from_string(
                            "Maybe panic.",
                            StatusCode::INTERNAL_SERVER_ERROR,
                        )),
                    }
                }
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
    file_format: &FileFormatParams,
    tx: Sender<Result<DataBlock>>,
    input_read_buffer_size: usize,
) -> poem::Result<()> {
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
                let name = field.name();
                // resolve the ability to utilize name
                if name != Some("upload") {
                    return Err(poem::Error::from_string(
                        "the name of field in form-data must `upload`",
                        StatusCode::BAD_REQUEST,
                    ));
                }
                let filename = field.file_name().unwrap_or("file_with_no_name").to_string();
                debug!("Streaming load start read {}", &filename);
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
                        debug!(
                            "streaming load read file {} of {} bytes",
                            filename,
                            data.len()
                        );
                        let batch = BytesBatch {
                            data,
                            path: filename.clone(),
                            offset: 0,
                            is_eof: true,
                        };
                        let block = DataBlock::empty_with_meta(Box::new(batch));
                        if let Err(e) = tx.send(Ok(block)).await {
                            warn!("Streaming load fail to send data to pipeline: {}", e);
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
                                warn!("Streaming load fail to send data to pipeline: {}", e);
                                // the caller get the actual error from interpreter
                                return Ok(());
                            }
                            if n == 0 {
                                debug!("Streaming load end read {}, size = {}", &filename, offset);
                                break;
                            }
                            offset += n;
                        }
                    }
                    _ => {
                        return Err(poem::Error::from_string(
                            format!("streaming load not support format {}", file_format),
                            StatusCode::BAD_REQUEST,
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}
