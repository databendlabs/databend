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

use std::collections::HashMap;
use std::sync::Arc;

use async_stream::stream;
use databend_common_base::base::short_sql;
use databend_common_base::base::tokio::task::JoinHandle;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_expression::infer_table_schema;
use databend_common_expression::DataSchemaRef;
use databend_common_formats::ClickhouseFormatType;
use databend_common_formats::FileFormatOptionsExt;
use databend_common_formats::FileFormatTypeExt;
use databend_common_sql::Planner;
use fastrace::func_path;
use fastrace::prelude::*;
use futures::StreamExt;
use futures::TryStreamExt;
use http::HeaderMap;
use http::StatusCode;
use log::info;
use naive_cityhash::cityhash128;
use poem::error::BadRequest;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::get;
use poem::post;
use poem::web::Query;
use poem::web::WithContentType;
use poem::Body;
use poem::Endpoint;
use poem::EndpointExt;
use poem::IntoResponse;
use poem::Route;
use serde::Deserialize;
use serde::Serialize;

use crate::interpreters::interpreter_plan_sql;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterPtr;
use crate::servers::http::middleware::sanitize_request_headers;
use crate::servers::http::v1::HttpQueryContext;
use crate::sessions::QueriesQueueManager;
use crate::sessions::QueryContext;
use crate::sessions::QueryEntry;
use crate::sessions::SessionType;
use crate::sessions::TableContext;

// accept all clickhouse params, so they do not go to settings.
#[derive(Serialize, Deserialize, Debug)]
pub struct StatementHandlerParams {
    query: Option<String>,
    #[allow(unused)]
    query_id: Option<String>,
    database: Option<String>,
    default_format: Option<String>,
    compress: Option<u8>,
    #[allow(unused)]
    decompress: Option<u8>,
    #[allow(unused)]
    buffer_size: Option<usize>,
    #[allow(unused)]
    max_result_bytes: Option<usize>,
    #[allow(unused)]
    wait_end_of_query: Option<u8>,
    #[allow(unused)]
    session_id: Option<String>,
    #[allow(unused)]
    session_check: Option<u8>,
    #[allow(unused)]
    session_timeout: Option<u64>,
    // in secs
    #[allow(unused)]
    with_stacktrace: Option<u8>,
    #[serde(flatten)]
    settings: HashMap<String, String>,
}

impl StatementHandlerParams {
    pub fn compress(&self) -> bool {
        self.compress.unwrap_or(0u8) == 1u8
    }

    pub fn query(&self) -> String {
        self.query.clone().unwrap_or_default()
    }
}

async fn execute(
    ctx: Arc<QueryContext>,
    interpreter: InterpreterPtr,
    schema: DataSchemaRef,
    format: ClickhouseFormatType,
    params: StatementHandlerParams,
    handle: Option<JoinHandle<()>>,
) -> Result<WithContentType<Body>> {
    let format_typ = format.typ.clone();

    // the reason of spawning new task to execute the interpreter:
    // (FIXME describe this in a more concise way)
    //
    // - there are executions of interpreters that will block the caller (NOT async wait)
    //   e.g. PipelineCompleteExecutor::execute, will spawn thread that executes the pipeline,
    //   and then, join the thread handle.
    // - async mutex (tokio::sync::Mutex) are used while executing the queries/statements
    //   An async task may yield while holding the lock of an async mutex. e.g. embedded meta store
    // - this method(execute) is running with default tokio runtime (the "tokio-runtime-worker" thread)
    //
    // if executes the interpreter "directly" (by using current thread), the following deadlock may happen:
    //
    // - thread A acquired a lock of async mutex and yield (without releasing the lock)
    // - thread A as a tokio processor, grab the task, which will unlock the async mutex
    //   but before execute the task, preemptively scheduled to the following task:
    //   - spawns a new native thread B, which also trying to acquire lock of the same mutex
    //   - and then (pthread-)joining the handle of thread B
    //   thus the following deadlock occurs
    //   - thread A is blocked in joining thread B
    //     the async task(thread A grabbed) which will release the lock will not be executed
    //   - thread B is trying to acquire a lock of the same mutex
    //
    //  to avoid the above scenario, one of the ways is to let the thread that blocked in pthread_join
    //  not in charge of running async task that will release the lock.
    //
    //  thus here we spawn the task of executing the interpreter to ctx runtime :
    //    - "pthread_join" will happen in "query-ctx" thread
    //    - "acquire" and "release" the async mutex lock will happen in other threads (it depends)
    //       e.g. "CompleteExecutor" threads
    //
    //  P.S. I think it will be better/more reasonable if we could avoid using pthread_join inside an async stack.

    ctx.try_spawn({
        let ctx = ctx.clone();
        async move {
            let mut data_stream = interpreter.execute(ctx.clone()).await?;
            let table_schema = infer_table_schema(&schema)?;
            let mut output_format = FileFormatOptionsExt::get_output_format_from_clickhouse_format(
                format,
                table_schema,
                &ctx.get_settings(),
            )?;

            let prefix = Ok(output_format.serialize_prefix()?);

            let compress_fn = move |rb: Result<Vec<u8>>| -> Result<Vec<u8>> {
                if params.compress() {
                    match rb {
                        Ok(b) => compress_block(b),
                        Err(e) => Err(e),
                    }
                } else {
                    rb
                }
            };

            // try to catch runtime error before http response, so user can client can get http 500
            let first_block = match data_stream.next().await {
                Some(block) => match block {
                    Ok(block) => Some(compress_fn(output_format.serialize_block(&block))),
                    Err(err) => return Err(err),
                },
                None => None,
            };

            let session = ctx.get_current_session();
            let stream = stream! {
                yield compress_fn(prefix);
                let mut ok = true;
                // do not pull data_stream if we already meet a None
                if let Some(block) = first_block {
                    yield block;
                    while let Some(block) = data_stream.next().await {
                        match block{
                            Ok(block) => {
                                yield compress_fn(output_format.serialize_block(&block));
                            },
                            Err(err) => {
                                let message = format!("{}", err);
                                yield compress_fn(Ok(message.into_bytes()));
                                ok = false;
                                break
                            }
                        };
                    }
                }
                if ok {
                    yield compress_fn(output_format.finalize());
                }
                // to hold session ref until stream is all consumed
                let _ = session.get_id();
            };
            if let Some(handle) = handle {
                handle.await.expect("must")
            }

            let stream = stream.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err));
            Ok(Body::from_bytes_stream(stream).with_content_type(format_typ.get_content_type()))
        }
    })?
    .await
    .map_err(|err| {
        ErrorCode::from_string(format!(
            "clickhouse handler failed to join interpreter thread: {err:?}"
        ))
    })?
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn clickhouse_handler_get(
    ctx: &HttpQueryContext,
    Query(params): Query<StatementHandlerParams>,
    headers: &HeaderMap,
) -> PoemResult<WithContentType<Body>> {
    let root = Span::root(func_path!(), SpanContext::random());
    async {
        let session = ctx.upgrade_session(SessionType::ClickHouseHttpHandler)?;
        if let Some(db) = &params.database {
            session.set_current_database(db.clone());
        }
        let context = session
            .create_query_context()
            .await
            .map_err(InternalServerError)?;

        let settings = session.get_settings();
        settings
            .set_batch_settings(&params.settings)
            .map_err(BadRequest)?;

        if !settings
            .get_enable_clickhouse_handler()
            .map_err(InternalServerError)?
        {
            return Err(poem::Error::from_string(
                "default settings: enable_clickhouse_handler is 0".to_string(),
                StatusCode::METHOD_NOT_ALLOWED,
            ));
        }

        let default_format = get_default_format(&params, headers).map_err(BadRequest)?;
        let sql = params.query();
        // Use interpreter_plan_sql, we can write the query log if an error occurs.
        let (plan, extras) = interpreter_plan_sql(context.clone(), &sql)
            .await
            .map_err(|err| err.display_with_sql(&sql))
            .map_err(BadRequest)?;

        let query_entry = QueryEntry::create(&context, &plan, &extras).map_err(BadRequest)?;
        let _guard = QueriesQueueManager::instance()
            .acquire(query_entry)
            .await
            .map_err(BadRequest)?;
        let format = get_format_with_default(extras.format, default_format)?;
        let interpreter = InterpreterFactory::get(context.clone(), &plan)
            .await
            .map_err(|err| err.display_with_sql(&sql))
            .map_err(BadRequest)?;
        execute(context, interpreter, plan.schema(), format, params, None)
            .await
            .map_err(|err| err.display_with_sql(&sql))
            .map_err(InternalServerError)
    }
    .in_span(root)
    .await
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn clickhouse_handler_post(
    ctx: &HttpQueryContext,
    body: Body,
    Query(params): Query<StatementHandlerParams>,
    headers: &HeaderMap,
) -> PoemResult<impl IntoResponse> {
    let root = Span::root(func_path!(), SpanContext::random());

    async {
        info!(
            "new clickhouse handler request: headers={:?}, params={:?}",
            sanitize_request_headers(headers),
            params,
        );
        let session = ctx.upgrade_session(SessionType::ClickHouseHttpHandler)?;
        if let Some(db) = &params.database {
            session.set_current_database(db.clone());
        }
        let ctx = session
            .create_query_context()
            .await
            .map_err(InternalServerError)?;

        let settings = session.get_settings();
        settings
            .set_batch_settings(&params.settings)
            .map_err(BadRequest)?;

        if !settings
            .get_enable_clickhouse_handler()
            .map_err(InternalServerError)?
        {
            return Err(poem::Error::from_string(
                "default settings: enable_clickhouse_handler is 0".to_string(),
                StatusCode::METHOD_NOT_ALLOWED,
            ));
        }

        let default_format = get_default_format(&params, headers).map_err(BadRequest)?;
        let mut sql = params.query();
        if !sql.is_empty() {
            sql.push(' ');
        }
        sql.push_str(body.into_string().await?.as_str());
        let n = 64;
        // other parts of the request already logged in middleware
        let len = sql.len();
        let msg = if len > n {
            format!("{}...(omit {} bytes)", short_sql(sql.clone()), len - n)
        } else {
            sql.to_string()
        };
        info!("receive clickhouse http post, (query + body) = {}", &msg);

        let mut planner = Planner::new(ctx.clone());
        let (mut plan, extras) = planner
            .plan_sql(&sql)
            .await
            .map_err(|err| err.display_with_sql(&sql))
            .map_err(BadRequest)?;

        let entry = QueryEntry::create(&ctx, &plan, &extras).map_err(BadRequest)?;
        let _guard = QueriesQueueManager::instance()
            .acquire(entry)
            .await
            .map_err(BadRequest)?;

        let mut handle = None;
        let output_schema = plan.schema();

        let format = get_format_with_default(extras.format, default_format)?;
        let interpreter = InterpreterFactory::get(ctx.clone(), &plan)
            .await
            .map_err(|err| err.display_with_sql(&sql))
            .map_err(BadRequest)?;

        execute(ctx, interpreter, output_schema, format, params, handle)
            .await
            .map_err(|err| err.display_with_sql(&sql))
            .map_err(InternalServerError)
    }
    .in_span(root)
    .await
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn clickhouse_ping_handler() -> String {
    "OK.\n".to_string()
}

pub fn clickhouse_router() -> impl Endpoint {
    Route::new()
        .at(
            "/",
            post(clickhouse_handler_post).get(clickhouse_handler_get),
        )
        .at("/ping", get(clickhouse_ping_handler))
        .at("/replicas_status", get(clickhouse_ping_handler))
        .with(poem::middleware::Compression::default())
}

// default codec is always lz4
fn compress_block(input: Vec<u8>) -> Result<Vec<u8>> {
    if input.is_empty() {
        Ok(vec![])
    } else {
        // TODO(youngsofun): optimize buffer usages
        let uncompressed_size = input.len();
        let compressed =
            lz4::block::compress(&input, Some(lz4::block::CompressionMode::FAST(1)), false)
                .map_err_to_code(ErrorCode::BadBytes, || "lz4 compress error")?;

        // 9 bytes header: 1 byte for method, 4 bytes for compressed size, 4 bytes for uncompressed size
        let header_size = 9;
        let method_byte_lz4 = 0x82u8;
        let mut compressed_with_header = Vec::with_capacity(compressed.len() + header_size);
        compressed_with_header.push(method_byte_lz4);
        let compressed_size = (compressed.len() + header_size) as u32;
        let uncompressed_size = uncompressed_size as u32;
        compressed_with_header.extend_from_slice(&compressed_size.to_le_bytes());
        compressed_with_header.extend_from_slice(&uncompressed_size.to_le_bytes());
        compressed_with_header.extend_from_slice(&compressed);

        // 16 bytes checksum
        let mut output = Vec::with_capacity(compressed_with_header.len() + 16);
        let checksum = cityhash128(&compressed_with_header);
        output.extend_from_slice(&checksum.lo.to_le_bytes());
        output.extend_from_slice(&checksum.hi.to_le_bytes());
        output.extend_from_slice(&compressed_with_header);
        Ok(output)
    }
}

fn get_default_format(
    params: &StatementHandlerParams,
    headers: &HeaderMap,
) -> Result<ClickhouseFormatType> {
    let name = match &params.default_format {
        None => match headers.get("X-CLICKHOUSE-FORMAT") {
            None => "TSV",
            Some(v) => v.to_str().map_err_to_code(
                ErrorCode::BadBytes,
                || "value of X-CLICKHOUSE-FORMAT is not string",
            )?,
        },
        Some(s) => s,
    };
    ClickhouseFormatType::parse_clickhouse_format(name)
}

fn get_format_with_default(
    format: Option<String>,
    default_format: ClickhouseFormatType,
) -> PoemResult<ClickhouseFormatType> {
    match format {
        None => Ok(default_format),
        Some(name) => ClickhouseFormatType::parse_clickhouse_format(&name).map_err(BadRequest),
    }
}
