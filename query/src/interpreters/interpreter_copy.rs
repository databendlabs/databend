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

    async fn get_dal_operator(&self, stage_info: &UserStageInfo) -> Result<opendal::Operator> {
        match &stage_info.stage_params.storage {
            StageStorage::S3(s3) => {
                let key_id = &s3.credentials_aws_key_id;
                let secret_key = &s3.credentials_aws_secret_key;
                let credential = Credential::hmac(key_id, secret_key);
                let bucket = &s3.bucket;

                let mut builder = opendal::services::s3::Backend::build();
                Ok(opendal::Operator::new(
                    builder
                        .region("us-east-2")
                        .bucket(bucket)
                        .credential(credential)
                        .finish()
                        .await
                        .map_err(|e| {
                            ErrorCode::DalS3Error(format!("s3 dal build error:{:?}", e))
                        })?,
                ))
            }
        }
    }

    async fn get_csv_stream(&self, stage_info: &UserStageInfo) -> Result<SourceStream> {
        let schema = self.plan.schema.clone();
        let max_block_size = self.ctx.get_settings().get_max_block_size()? as usize;
        let read_buffer_size = self.ctx.get_settings().get_storage_read_buffer_size()?;

        // External stage, stage_name is the file location.
        let file_location = stage_info.stage_name.clone();
        let dal_operator = self.get_dal_operator(stage_info).await?;
        let object = dal_operator
            .stat(&file_location)
            .run()
            .await
            .map_err(|e| ErrorCode::DalStatError(format!("dal stat error:{:?}", e)))?;
        let reader = SeekableReader::new(dal_operator, &file_location, object.size);
        let reader = BufReader::with_capacity(read_buffer_size as usize, reader);

        // Skip header.
        let header = stage_info.file_format_option.skip_header > 0;

        // Field delimiter, default ','.
        let field_delimiter_str = &stage_info.file_format_option.field_delimiter;
        let field_delimiter = match field_delimiter_str.len() {
            n if n >= 1 => field_delimiter_str.as_bytes()[0],
            _ => b',',
        };

        // Record delimiter, default '\n'.
        let record_delimiter_str = &stage_info.file_format_option.record_delimiter;
        let record_delimiter = match record_delimiter_str.len() {
            n if n >= 1 => record_delimiter_str.as_bytes()[0],
            _ => b'\n',
        };

        Ok(SourceStream::new(Box::new(CsvSource::try_create(
            reader,
            schema,
            header,
            field_delimiter,
            record_delimiter,
            max_block_size,
        )?)))
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
        let stage_info = self.plan.stage_plan.stage_info.clone();
        let source_stream = match stage_info.stage_type {
            StageType::External => {
                match stage_info.file_format_option.format {
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
