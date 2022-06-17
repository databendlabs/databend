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
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_planners::PlanNode;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::StreamExt;
use naive_cityhash::cityhash128;
use poem::error::BadRequest;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::get;
use poem::post;
use poem::web::Query;
use poem::Body;
use poem::Endpoint;
use poem::EndpointExt;
use poem::IntoResponse;
use poem::Route;
use serde::Deserialize;
use serde::Serialize;

use crate::formats::output_format::OutputFormatType;
use crate::interpreters::InterpreterFactory;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::StreamSource;
use crate::pipelines::new::SourcePipeBuilder;
use crate::servers::http::v1::HttpQueryContext;
use crate::sessions::QueryContext;
use crate::sessions::SessionType;
use crate::sql::PlanParser;

#[derive(Serialize, Deserialize)]
pub struct StatementHandlerParams {
    query: Option<String>,
    compress: Option<u8>,
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
    plan: PlanNode,
    format: Option<String>,
    input_stream: Option<SendableDataBlockStream>,
    compress: bool,
) -> Result<impl IntoResponse> {
    let interpreter = InterpreterFactory::get(ctx.clone(), plan.clone())?;
    let _ = interpreter
        .start()
        .await
        .map_err(|e| tracing::error!("interpreter.start.error: {:?}", e));
    let data_stream: SendableDataBlockStream =
        if ctx.get_settings().get_enable_new_processor_framework()? != 0
            && ctx.get_cluster().is_empty()
        {
            let output_port = OutputPort::create();
            let stream_source =
                StreamSource::create(ctx.clone(), input_stream, output_port.clone())?;
            let mut source_pipe_builder = SourcePipeBuilder::create();
            source_pipe_builder.add_source(output_port, stream_source);
            let _ = interpreter
                .set_source_pipe_builder(Option::from(source_pipe_builder))
                .map_err(|e| tracing::error!("interpreter.set_source_pipe_builder.error: {:?}", e));
            interpreter.execute(None).await?
        } else {
            interpreter.execute(input_stream).await?
        };
    let mut data_stream = ctx.try_create_abortable(data_stream)?;
    let format_setting = ctx.get_format_settings()?;
    let mut fmt = OutputFormatType::TSV;
    if let Some(format) = format {
        fmt = OutputFormatType::from_str(format.as_str())?;
    }

    let mut output_format = fmt.create_format(plan.schema(), format_setting);
    let prefix = Ok(output_format.serialize_prefix()?);

    let compress_fn = move |rb: Result<Vec<u8>>| -> Result<Vec<u8>> {
        if compress {
            match rb {
                Ok(b) => compress_block(b),
                Err(e) => Err(e),
            }
        } else {
            rb
        }
    };
    let stream = stream! {
        yield compress_fn(prefix);
        while let Some(block) = data_stream.next().await {
            match block{
                Ok(block) => {
                    yield compress_fn(output_format.serialize_block(&block));
                },
                Err(err) => yield(Err(err)),
            };
        }
        yield compress_fn(output_format.finalize());
        let _ = interpreter
            .finish()
            .await
            .map_err(|e| tracing::error!("interpreter.finish error: {:?}", e));
    };

    Ok(Body::from_bytes_stream(stream).with_content_type(fmt.get_content_type()))
}

#[poem::handler]
pub async fn clickhouse_handler_get(
    ctx: &HttpQueryContext,
    Query(params): Query<StatementHandlerParams>,
) -> PoemResult<impl IntoResponse> {
    let session = ctx.get_session(SessionType::ClickHouseHttpHandler);
    let context = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;

    session
        .get_settings()
        .set_batch_settings(&params.settings, false)
        .map_err(BadRequest)?;

    let sql = params.query();

    let (plan, format) = PlanParser::parse_with_format(context.clone(), &sql)
        .await
        .map_err(BadRequest)?;

    context.attach_query_str(&sql);
    execute(context, plan, format, None, params.compress())
        .await
        .map_err(InternalServerError)
}

#[poem::handler]
pub async fn clickhouse_handler_post(
    ctx: &HttpQueryContext,
    body: Body,
    Query(params): Query<StatementHandlerParams>,
) -> PoemResult<impl IntoResponse> {
    let session = ctx.get_session(SessionType::ClickHouseHttpHandler);
    let ctx = session
        .create_query_context()
        .await
        .map_err(InternalServerError)?;

    session
        .get_settings()
        .set_batch_settings(&params.settings, false)
        .map_err(BadRequest)?;

    let mut sql = params.query();
    sql.push_str(body.into_string().await?.as_str());
    let (plan, format) = PlanParser::parse_with_format(ctx.clone(), &sql)
        .await
        .map_err(BadRequest)?;

    ctx.attach_query_str(&sql);
    execute(ctx, plan, format, None, params.compress())
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
