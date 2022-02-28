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

use std::future::Future;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::S3FileReader;
use common_meta_types::StageFileFormatType;
use common_meta_types::StageStorage;
use common_meta_types::UserStageInfo;
use common_planners::UserStagePlan;
use common_streams::CsvSourceBuilder;
use common_streams::SendableDataBlockStream;
use common_streams::SourceStream;
use futures::StreamExt;
use opendal::Reader;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::AsyncSource;
use crate::pipelines::new::processors::AsyncSourcer;
use crate::sessions::QueryContext;

pub struct ExternalSource {
    ctx: Arc<QueryContext>,
    plan: UserStagePlan,
    file_name: Option<String>,
    initialized: bool,
    wrap_stream: Option<SendableDataBlockStream>,
}

impl ExternalSource {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        output: Arc<OutputPort>,
        plan: UserStagePlan,
        file_name: Option<String>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(output, ExternalSource {
            ctx,
            plan,
            file_name,
            initialized: false,
            wrap_stream: None,
        })
    }

    // Get csv source stream.
    async fn csv_stream(
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        stage_info: &UserStageInfo,
        reader: Reader,
    ) -> Result<SourceStream> {
        let mut builder = CsvSourceBuilder::create(schema);
        let size_limit = stage_info.copy_options.size_limit;

        // Size limit.
        {
            if size_limit > 0 {
                builder.size_limit(size_limit);
            }
        }

        // Block size.
        {
            let max_block_size = ctx.get_settings().get_max_block_size()? as usize;
            builder.block_size(max_block_size);
        }

        // Skip header.
        {
            builder.skip_header(stage_info.file_format_options.skip_header);
        }

        // Field delimiter, default ','.
        {
            let field_delimiter = &stage_info.file_format_options.field_delimiter;
            builder.field_delimiter(field_delimiter);
        }

        // Record delimiter, default '\n'.
        {
            let record_delimiter = &stage_info.file_format_options.record_delimiter;
            builder.record_delimiter(record_delimiter);
        }

        let source = builder.build(reader)?;
        Ok(SourceStream::new(Box::new(source)))
    }

    async fn initialize(&mut self) -> Result<()> {
        let ctx = self.ctx.clone();
        let stage = &self.plan.stage_info;
        let file_format = stage.file_format_options.format.clone();
        let file_name = self.file_name.clone();

        // Get the dal file reader.
        let file_reader = match &stage.stage_params.storage {
            StageStorage::S3(s3) => {
                let endpoint = &ctx.get_config().storage.s3.endpoint_url;
                let bucket = &s3.bucket;
                let path = &s3.path;

                let key_id = &s3.credentials_aws_key_id;
                let secret_key = &s3.credentials_aws_secret_key;
                S3FileReader::create(file_name, endpoint, bucket, path, key_id, secret_key).await
            }
        }?;

        // Get the format(CSV) stream.
        let format_stream = match &file_format {
            StageFileFormatType::Csv => {
                Ok(
                    Self::csv_stream(ctx.clone(), self.plan.schema.clone(), stage, file_reader)
                        .await?
                        .execute()
                        .await?,
                )
            }
            // Unsupported.
            format => Err(ErrorCode::LogicalError(format!(
                "Unsupported file format: {:?}",
                format
            ))),
        }?;

        self.wrap_stream = Some(format_stream);

        Ok(())
    }
}

impl AsyncSource for ExternalSource {
    const NAME: &'static str = "ExternalSource";

    type BlockFuture<'a>
    where Self: 'a
    = impl Future<Output = Result<Option<DataBlock>>>;

    fn generate(&mut self) -> Self::BlockFuture<'_> {
        async move {
            if !self.initialized {
                self.initialized = true;
                self.initialize().await?;
            }

            match &mut self.wrap_stream {
                None => Err(ErrorCode::LogicalError("")),
                Some(stream) => match stream.next().await {
                    None => Ok(None),
                    Some(Err(cause)) => Err(cause),
                    Some(Ok(data)) => Ok(Some(data)),
                },
            }
        }
    }
}
