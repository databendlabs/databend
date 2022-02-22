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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::StageFileFormatType;
use common_meta_types::StageStorage;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_planners::CopyPlan;
use common_streams::CsvSourceBuilder;
use common_streams::DataBlockStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use common_streams::SourceStream;
use common_tracing::tracing;
use futures::io::BufReader;
use futures::TryStreamExt;
use opendal::credential::Credential;
use opendal::readers::SeekableReader;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

#[allow(dead_code)]
pub struct CopyInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyPlan,
}

impl CopyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CopyInterpreter { ctx, plan }))
    }

    async fn get_dal_operator(
        &self,
        stage_info: &UserStageInfo,
    ) -> Result<(opendal::Operator, String)> {
        match &stage_info.stage_params.storage {
            StageStorage::S3(s3) => {
                let mut builder = opendal::services::s3::Backend::build();

                // Region.
                {
                    // TODO(bohu): opendal to check the region.
                    let region = "us-east-2";
                    let bucket = &s3.bucket;
                    builder.region(region).bucket(bucket);
                }

                // Credentials.
                if !s3.credentials_aws_key_id.is_empty() {
                    let key_id = &s3.credentials_aws_key_id;
                    let secret_key = &s3.credentials_aws_secret_key;
                    let credential = Credential::hmac(key_id, secret_key);
                    builder.credential(credential);
                }

                let operator = builder
                    .finish()
                    .await
                    .map_err(|e| ErrorCode::DalS3Error(format!("s3 dal build error:{:?}", e)))?;

                Ok((opendal::Operator::new(operator), s3.path.to_string()))
            }
        }
    }

    async fn get_csv_stream(&self, stage_info: &UserStageInfo) -> Result<SourceStream> {
        let schema = self.plan.schema.clone();
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
            let max_block_size = self.ctx.get_settings().get_max_block_size()? as usize;
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

        let read_buffer_size = self.ctx.get_settings().get_storage_read_buffer_size()?;
        let (dal_operator, path) = self.get_dal_operator(stage_info).await?;
        let size = dal_operator
            .stat(&path)
            .run()
            .await
            .map_err(|e| ErrorCode::DalStatError(format!("dal stat {:} error:{:?}", path, e)))?
            .size;
        tracing::info!("Get {} object size:{}", path, size);

        let reader = SeekableReader::new(dal_operator, &path, size);
        let reader = BufReader::with_capacity(read_buffer_size as usize, reader);
        let source = builder.build(reader)?;

        Ok(SourceStream::new(Box::new(source)))
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyInterpreter {
    fn name(&self) -> &str {
        "CopyInterpreter"
    }

    async fn execute(
        &self,
        mut _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        tracing::info!("Plan:{:?}", self.plan);

        let stage_info = self.plan.stage_plan.stage_info.clone();
        let source_stream = match stage_info.stage_type {
            StageType::External => {
                match stage_info.file_format_options.format {
                    // CSV.
                    StageFileFormatType::Csv => self.get_csv_stream(&stage_info).await,
                    // Unsupported.
                    format => Err(ErrorCode::LogicalError(format!(
                        "Unsupported file format: {:?}",
                        format
                    ))),
                }
            }

            StageType::Internal => Err(ErrorCode::LogicalError(
                "Unsupported copy from internal stage",
            )),
        }?;

        let input_stream = source_stream.execute().await?;
        let progress_stream = Box::pin(ProgressStream::try_create(
            input_stream,
            self.ctx.get_scan_progress(),
        )?);

        let table = self
            .ctx
            .get_table(&self.plan.db_name, &self.plan.tbl_name)
            .await?;
        let r = table
            .append_data(self.ctx.clone(), progress_stream)
            .await?
            .try_collect()
            .await?;
        table.commit_insertion(self.ctx.clone(), r, false).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
