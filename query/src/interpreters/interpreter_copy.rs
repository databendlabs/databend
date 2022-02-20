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

use common_exception::Result;
use common_meta_types::StageStorage;
use common_meta_types::UserStageInfo;
use common_planners::CopyPlan;
use common_streams::DataBlockStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use common_streams::SourceFactory;
use common_streams::SourceParams;
use common_streams::SourceStream;
use futures::io::BufReader;
use futures::TryStreamExt;
use opendal::credential::Credential;
use opendal::readers::SeekableReader;

use crate::common::HashMap;
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
        let dal = self
            .get_dal(self.plan.stage_plan.stage_info.clone())
            .await?;

        let max_block_size = self.ctx.get_settings().get_max_block_size()? as usize;
        let path = self
            .plan
            .stage_plan
            .stage_info
            .stage_params
            .location
            .clone()
            .as_str();
        let o = dal.stat(path).run().await.unwrap();
        let reader = SeekableReader::new(dal, path, o.size);
        let read_buffer_size = self.ctx.get_settings().get_storage_read_buffer_size()?;
        let reader = BufReader::with_capacity(read_buffer_size as usize, reader);

        let source_params = SourceParams {
            reader,
            path,
            format: "csv",
            schema: self.plan.schema.clone(),
            max_block_size,
            projection: (0..self.plan.schema().fields().len()).collect(),
            options: &HashMap::create(),
        };
        let source_stream = SourceStream::new(SourceFactory::try_get(source_params)?);
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
