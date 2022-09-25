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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_stream::stream;
use common_base::base::tokio;
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::TrySpawn;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_formats::output_format::OutputFormatType;
use common_pipeline_sources::processors::sources::input_formats::InputContext;
use common_pipeline_sources::processors::sources::input_formats::StreamingReadBatch;
use futures::StreamExt;
use http::HeaderMap;
use naive_cityhash::cityhash128;
use opendal::io_util::CompressAlgorithm;
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
use tracing::info;

use crate::interpreters::InterpreterFactory;
use crate::interpreters::InterpreterPtr;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::CLickHouseFederated;
use crate::sessions::QueryContext;
use crate::sessions::SessionType;
use crate::sessions::TableContext;
use crate::sql::plans::InsertInputSource;
use crate::sql::plans::Plan;
use crate::sql::Planner;

// accept all clickhouse params, so they do not go to settings.
#[derive(Serialize, Deserialize)]
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
    format: OutputFormatType,
    params: StatementHandlerParams,
    handle: Option<JoinHandle<()>>,
) -> Result<WithContentType<Body>> {
    let mut data_stream = interpreter.execute(ctx.clone()).await?;
    let format_setting = ctx.get_format_settings()?;
    let mut output_format = format.create_format(schema, format_setting);
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

    Ok(Body::from_bytes_stream(stream).with_content_type(format.get_content_type()))
}

#[poem::handler]
pub async fn clickhouse_handler_get(
    ctx: &HttpQueryContext,
    Query(params): Query<StatementHandlerParams>,
    headers: &HeaderMap,
) -> PoemResult<WithContentType<Body>> {
    let session = ctx.get_session(SessionType::ClickHouseHttpHandler);
    if let Some(db) = &params.database {
        session.set_current_database(db.clone());
    }
    let context = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;

    session
        .get_settings()
        .set_batch_settings(&params.settings, false)
        .map_err(BadRequest)?;

    let default_format = get_default_format(&params, headers).map_err(BadRequest)?;
    let sql = params.query();
    if let Some(block) = CLickHouseFederated::check(&sql) {
        return serialize_one_block(context.clone(), block, &sql, &params, default_format)
            .map_err(InternalServerError);
    }

    let mut planner = Planner::new(context.clone());
    let (plan, _, fmt) = planner.plan_sql(&sql).await.map_err(BadRequest)?;
    let format = get_format_with_default(fmt, default_format)?;
    let format = get_format_from_plan(&plan, format)?;

    context.attach_query_str(plan.to_string(), &sql);
    let interpreter = InterpreterFactory::get(context.clone(), &plan)
        .await
        .map_err(BadRequest)?;
    execute(context, interpreter, plan.schema(), format, params, None)
        .await
        .map_err(InternalServerError)
}

#[poem::handler]
pub async fn clickhouse_handler_post(
    ctx: &HttpQueryContext,
    body: Body,
    Query(params): Query<StatementHandlerParams>,
    headers: &HeaderMap,
) -> PoemResult<impl IntoResponse> {
    let session = ctx.get_session(SessionType::ClickHouseHttpHandler);
    if let Some(db) = &params.database {
        session.set_current_database(db.clone());
    }
    let ctx = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;

    let settings = ctx.get_settings();
    settings
        .set_batch_settings(&params.settings, false)
        .map_err(BadRequest)?;

    let default_format = get_default_format(&params, headers).map_err(BadRequest)?;
    let mut sql = params.query();
    sql.push_str(body.into_string().await?.as_str());
    let n = 100;
    // other parts of the request already logged in middleware
    let msg = if sql.len() > n {
        format!(
            "{}...(omit {} bytes)",
            &sql[0..n].to_string(),
            sql.len() - n
        )
    } else {
        sql.to_string()
    };
    info!("receive clickhouse http post, (query + body) = {}", &msg);

    if let Some(block) = CLickHouseFederated::check(&sql) {
        return serialize_one_block(ctx.clone(), block, &sql, &params, default_format)
            .map_err(InternalServerError);
    }
    let mut planner = Planner::new(ctx.clone());
    let (mut plan, _, fmt) = planner.plan_sql(&sql).await.map_err(BadRequest)?;
    let schema = plan.schema();
    ctx.attach_query_str(plan.to_string(), &sql);
    let mut handle = None;
    if let Plan::Insert(insert) = &mut plan {
        if let InsertInputSource::StreamingWithFormat(format, start, input_context_ref) =
            &mut insert.source
        {
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            let input_context = Arc::new(
                InputContext::try_create_from_insert(
                    format.as_str(),
                    rx,
                    ctx.get_settings(),
                    schema,
                    ctx.get_scan_progress(),
                    false,
                )
                .await
                .map_err(InternalServerError)?,
            );
            *input_context_ref = Some(input_context.clone());
            info!(
                "clickhouse insert with format {:?}, value {}",
                input_context, *start
            );
            let compression_alg = input_context.get_compression_alg("").map_err(BadRequest)?;
            let start = *start;
            handle = Some(ctx.spawn(async move {
                gen_batches(
                    sql,
                    start,
                    input_context.read_batch_size,
                    tx,
                    compression_alg,
                )
                .await
            }));
        }
    };

    let format = get_format_with_default(fmt, default_format)?;
    let format = get_format_from_plan(&plan, format)?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan)
        .await
        .map_err(BadRequest)?;

    execute(ctx, interpreter, plan.schema(), format, params, handle)
        .await
        .map_err(InternalServerError)
}

#[poem::handler]
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
        .with(poem::middleware::Compression)
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

fn serialize_one_block(
    ctx: Arc<QueryContext>,
    block: DataBlock,
    sql: &str,
    params: &StatementHandlerParams,
    default_format: OutputFormatType,
) -> Result<WithContentType<Body>> {
    let format_setting = ctx.get_format_settings()?;
    let fmt = match CLickHouseFederated::get_format(sql) {
        Some(format) => OutputFormatType::from_str(format.as_str())?,
        None => default_format,
    };
    let mut output_format = fmt.create_format(block.schema().clone(), format_setting);
    let mut res = output_format.serialize_prefix()?;
    let mut data = output_format.serialize_block(&block)?;
    if params.compress() {
        data = compress_block(data)?;
    }
    res.append(&mut data);
    res.append(&mut output_format.finalize()?);
    Ok(Body::from(res).with_content_type(fmt.get_content_type()))
}

fn get_default_format(
    params: &StatementHandlerParams,
    headers: &HeaderMap,
) -> Result<OutputFormatType> {
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
    OutputFormatType::from_str(name)
}

fn get_format_from_plan(
    plan: &Plan,
    default_format: OutputFormatType,
) -> PoemResult<OutputFormatType> {
    let format = match plan.clone() {
        Plan::Query {
            s_expr: _,
            metadata: _,
            bind_context,
            ..
        } => bind_context.format.clone(),
        _ => None,
    };
    get_format_with_default(format, default_format)
}

fn get_format_with_default(
    format: Option<String>,
    default_format: OutputFormatType,
) -> PoemResult<OutputFormatType> {
    match format {
        None => Ok(default_format),
        Some(name) => OutputFormatType::from_str(&name).map_err(BadRequest),
    }
}

async fn gen_batches(
    data: String,
    start: usize,
    batch_size: usize,
    tx: Sender<Result<StreamingReadBatch>>,
    compression: Option<CompressAlgorithm>,
) {
    let buf = &data.trim_start().as_bytes()[start..];
    let buf_size = buf.len();
    let mut is_start = true;
    let mut start = 0;
    let path = "clickhouse_handler_body".to_string();
    while start < buf_size {
        let data = if buf_size - start >= batch_size {
            buf[start..batch_size].to_vec()
        } else {
            buf[start..].to_vec()
        };

        tracing::debug!("sending read {} bytes", data.len());
        if let Err(e) = tx
            .send(Ok(StreamingReadBatch {
                data,
                path: path.clone(),
                is_start,
                compression,
            }))
            .await
        {
            tracing::warn!(" Multipart fail to send ReadBatch: {}", e);
        }
        is_start = false;
        start += batch_size
    }
}
