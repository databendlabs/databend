// Copyright 2022 Datafuse Labs.
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

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::Compression;
use common_io::prelude::FormatSettings;
use opendal::io_util::CompressAlgorithm;
use opendal::io_util::DecompressDecoder;
use poem::web::Multipart;

use crate::formats::FormatFactory;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::SourcePipeBuilder;
use crate::servers::http::v1::parallel_format_source::ParallelInputFormatSource;
use crate::servers::http::v1::parallel_format_source::ParallelMultipartWorker;
use crate::servers::http::v1::sequential_format_source::SequentialInputFormatSource;
use crate::servers::http::v1::sequential_format_source::SequentialMultipartWorker;
use crate::sessions::QueryContext;

#[async_trait::async_trait]
pub trait MultipartWorker: Send {
    async fn work(&mut self);
}

pub struct MultipartFormat;

impl MultipartFormat {
    pub fn input_sources(
        name: &str,
        ctx: Arc<QueryContext>,
        multipart: Multipart,
        schema: DataSchemaRef,
        settings: FormatSettings,
    ) -> Result<(Box<dyn MultipartWorker>, SourcePipeBuilder)> {
        let mut source_pipe_builder = SourcePipeBuilder::create();
        let input_format =
            FormatFactory::instance().get_input(name, schema.clone(), settings.clone())?;

        let query_settings = ctx.get_settings();
        if query_settings.get_max_threads()? != 1 && input_format.support_parallel() {
            let max_threads = query_settings.get_max_threads()? as usize;

            let (tx, rx) = async_channel::bounded(10);

            for _index in 0..max_threads {
                let schema = schema.clone();
                let settings = settings.clone();
                let output_port = OutputPort::create();
                let scan_progress = ctx.get_scan_progress();

                source_pipe_builder.add_source(
                    output_port.clone(),
                    ParallelInputFormatSource::create(
                        output_port,
                        scan_progress,
                        FormatFactory::instance().get_input(name, schema, settings)?,
                        rx.clone(),
                    )?,
                );
            }

            Ok((
                Box::new(ParallelMultipartWorker::create(multipart, tx, input_format)),
                source_pipe_builder,
            ))
        } else {
            let compress_algo = match settings.compression {
                Compression::None => None,
                Compression::Auto => {
                    return Err(ErrorCode::UnImplement(
                        "compress type auto is unimplemented",
                    ));
                }
                Compression::Gzip => Some(CompressAlgorithm::Gzip),
                Compression::Bz2 => Some(CompressAlgorithm::Bz2),
                Compression::Brotli => Some(CompressAlgorithm::Brotli),
                Compression::Zstd => Some(CompressAlgorithm::Zstd),
                Compression::Deflate => Some(CompressAlgorithm::Zlib),
                Compression::RawDeflate => Some(CompressAlgorithm::Deflate),
                Compression::Lzo => {
                    return Err(ErrorCode::UnImplement("compress type lzo is unimplemented"));
                }
                Compression::Snappy => {
                    return Err(ErrorCode::UnImplement(
                        "compress type snappy is unimplemented",
                    ));
                }
            };
            let input_decompress = compress_algo.map(DecompressDecoder::new);

            let output = OutputPort::create();

            let (tx, rx) = common_base::base::tokio::sync::mpsc::channel(2);

            source_pipe_builder.add_source(
                output.clone(),
                SequentialInputFormatSource::create(
                    output,
                    input_format,
                    rx,
                    input_decompress,
                    ctx.get_scan_progress(),
                )?,
            );

            Ok((
                Box::new(SequentialMultipartWorker::create(multipart, tx)),
                source_pipe_builder,
            ))
        }
    }
}
