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
use common_planners::CopyPlan;
use common_streams::DataBlockStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use common_streams::SourceFactory;
use common_streams::SourceParams;
use common_streams::SourceStream;
use futures::io::BufReader;
use futures::TryStreamExt;
use nom::bytes::complete::tag;
use nom::bytes::complete::take_until;
use nom::IResult;
use opendal::credential::Credential;
use opendal::readers::SeekableReader;
use opendal::services::s3;
use opendal::Operator as DalOperator;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct CopyInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyPlan,
}

impl CopyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CopyInterpreter { ctx, plan }))
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
        let table = self
            .ctx
            .get_table(&self.plan.db_name, &self.plan.tbl_name)
            .await?;

        let location = self.plan.location.clone();
        let c = extract_stage_location(location.as_str());
        if c.is_err() {
            return Err(ErrorCode::BadOption(
                "Cannot convert value to stage and path",
            ));
        }
        let (stage, path) = c.unwrap();

        let acc = get_dal_by_stage(self.ctx.clone(), stage).await?;
        let max_block_size = self.ctx.get_settings().get_max_block_size()? as usize;
        let o = acc.stat(path).run().await.unwrap();
        let reader = SeekableReader::new(acc, path, o.size);
        let read_buffer_size = self.ctx.get_settings().get_storage_read_buffer_size()?;
        let reader = BufReader::with_capacity(read_buffer_size as usize, reader);
        let source_params = SourceParams {
            reader,
            path,
            format: self.plan.format.as_str(),
            schema: self.plan.schema.clone(),
            max_block_size,
            projection: (0..self.plan.schema().fields().len()).collect(),
            options: &self.plan.options,
        };
        let source_stream = SourceStream::new(SourceFactory::try_get(source_params)?);
        let input_stream = source_stream.execute().await?;
        let progress_stream = Box::pin(ProgressStream::try_create(
            input_stream,
            self.ctx.get_scan_progress(),
        )?);

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

/// @my_ext_stage/tutorials/sample.csv -> stage: my_ext_stage,  location: /tutorials/sample.csv
fn extract_stage_location(path: &str) -> IResult<&str, &str> {
    let (path, _) = tag("@")(path)?;
    let (path, stage) = take_until("/")(path)?;
    Ok((stage, path))
}

//  this is mock implementation from env
//  todo: support get the stage config from metadata
async fn get_dal_by_stage(ctx: Arc<QueryContext>, _stage_name: &str) -> Result<DalOperator> {
    // TODO: we need to check the storage type and get the right dal.
    let conf = ctx.get_config().storage.s3;

    let cred = Credential::hmac(&conf.access_key_id, &conf.secret_access_key);

    let mut builder = s3::Backend::build();

    Ok(DalOperator::new(
        builder
            .region(&conf.region)
            .endpoint(&conf.endpoint_url)
            .bucket(&conf.bucket)
            .credential(cred)
            .finish()
            .await
            .unwrap(),
    ))
}
