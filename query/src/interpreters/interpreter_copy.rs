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
use common_streams::CsvSource;
use common_streams::DataBlockStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use common_streams::SourceStream;
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

    #[allow(dead_code)]
    async fn get_dal(&self, stage: UserStageInfo) -> Result<opendal::Operator> {
        match stage.stage_params.storage {
            StageStorage::S3(s3) => {
                let key_id = s3.credentials_aws_key_id;
                let secret_key = s3.credentials_aws_secret_key;
                let credential = Credential::hmac(&key_id, &secret_key);
                let bucket = s3.bucket;

                let mut builder = opendal::services::s3::Backend::build();
                Ok(opendal::Operator::new(
                    builder
                        .region("us-east-2")
                        .bucket(&bucket)
                        .credential(credential)
                        .finish()
                        .await
                        .unwrap(),
                ))
            }
        }
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
        let max_block_size = self.ctx.get_settings().get_max_block_size()? as usize;
        let read_buffer_size = self.ctx.get_settings().get_storage_read_buffer_size()?;
        let schema = self.plan.schema.clone();

        let stage_info = self.plan.stage_plan.stage_info.clone();

        let source_stream = match stage_info.stage_type {
            StageType::Internal => Err(ErrorCode::LogicalError(
                "Unsupported copy from internal stage",
            )),
            StageType::External => {
                let file_location = stage_info.stage_name.clone();
                let dal = self.get_dal(stage_info.clone()).await?;
                let object = dal.stat(&file_location).run().await.unwrap();
                let reader = SeekableReader::new(dal, &file_location, object.size);
                let reader = BufReader::with_capacity(read_buffer_size as usize, reader);

                match stage_info.file_format_option.format {
                    // CSV.
                    StageFileFormatType::Csv => {
                        // Field delimiter.
                        let field_delimiter_str = stage_info.file_format_option.field_delimiter;
                        let field_delimiter = match field_delimiter_str.len() {
                            n if n >= 1 => field_delimiter_str.as_bytes()[0],
                            _ => b',',
                        };

                        // Record delimiter.
                        let record_delimiter_str = stage_info.file_format_option.record_delimiter;
                        let record_delimiter = match record_delimiter_str.len() {
                            n if n >= 1 => record_delimiter_str.as_bytes()[0],
                            _ => b'\n',
                        };

                        let csv = CsvSource::try_create(
                            reader,
                            schema,
                            true,
                            field_delimiter,
                            record_delimiter,
                            max_block_size,
                        )?;
                        Ok(SourceStream::new(Box::new(csv)))
                    }
                    // Unsupported.
                    format => Err(ErrorCode::LogicalError(format!(
                        "Unsupported file format: {:?}",
                        format
                    ))),
                }
            }
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
