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

use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::Pipe;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::processors::sources::StreamSource;
use common_planners::validate_function_arg;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[async_trait::async_trait]
pub trait Procedure: Sync + Send {
    fn procedure_name(&self) -> &str;

    fn procedure_features(&self) -> ProcedureFeatures;

    fn validate(&self, ctx: Arc<QueryContext>, args: &[String]) -> Result<()> {
        let features = self.procedure_features();
        validate_function_arg(
            self.procedure_name(),
            args.len(),
            features.variadic_arguments,
            features.num_arguments,
        )?;
        if features.management_mode_required && !ctx.get_config().query.management_mode {
            return Err(ErrorCode::ManagementModePermissionDenied(format!(
                "Access denied: '{}' only used in management-mode",
                self.procedure_name()
            )));
        }
        if let Some(user_option_flag) = features.user_option_flag {
            let user_info = ctx.get_current_user()?;
            if !user_info.has_option_flag(user_option_flag) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Access denied: '{}' requires user {} option flag",
                    self.procedure_name(),
                    user_option_flag
                )));
            }
        }
        Ok(())
    }

    async fn eval(
        &self,
        ctx: Arc<QueryContext>,
        args: Vec<String>,
        pipeline: &mut Pipeline,
    ) -> Result<()>;

    fn result_schema(&self) -> Arc<DataSchema>;
}

#[async_trait::async_trait]
pub trait OneBlockProcedure {
    fn name(&self) -> &str;

    fn features(&self) -> ProcedureFeatures;

    async fn all_data(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock>;

    fn schema(&self) -> Arc<DataSchema>;
}

#[async_trait::async_trait]
pub trait BlockStreamProcedure
where Self: Sized
{
    fn wrap(self) -> BlockStreamWrapper<Self> {
        BlockStreamWrapper(self)
    }

    fn name(&self) -> &str;

    fn features(&self) -> ProcedureFeatures;

    async fn data_stream(
        &self,
        ctx: Arc<QueryContext>,
        args: Vec<String>,
    ) -> Result<SendableDataBlockStream>;

    fn schema(&self) -> Arc<DataSchema>;
}

pub struct OneBlockWrapper<T>(pub T);

#[async_trait::async_trait]
impl<T> Procedure for OneBlockWrapper<T>
where T: OneBlockProcedure + Sync + Send
{
    fn procedure_name(&self) -> &str {
        self.0.name()
    }

    fn procedure_features(&self) -> ProcedureFeatures {
        self.0.features()
    }

    async fn eval(
        &self,
        ctx: Arc<QueryContext>,
        args: Vec<String>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.validate(ctx.clone(), &args)?;
        let block = self.0.all_data(ctx.clone(), args).await?;
        let output = OutputPort::create();
        let source = StreamSource::create(
            ctx,
            Some(DataBlockStream::create(self.0.schema(), None, vec![block]).boxed()),
            output.clone(),
        )?;

        pipeline.add_pipe(Pipe::SimplePipe {
            inputs_port: vec![],
            outputs_port: vec![output],
            processors: vec![source],
        });

        Ok(())
    }

    fn result_schema(&self) -> Arc<DataSchema> {
        self.0.schema()
    }
}

pub struct BlockStreamWrapper<T>(pub T);

#[async_trait::async_trait]
impl<T> Procedure for BlockStreamWrapper<T>
where T: BlockStreamProcedure + Sync + Send
{
    fn procedure_name(&self) -> &str {
        self.0.name()
    }

    fn procedure_features(&self) -> ProcedureFeatures {
        self.0.features()
    }

    async fn eval(
        &self,
        ctx: Arc<QueryContext>,
        args: Vec<String>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.validate(ctx.clone(), &args)?;
        let block_stream = self.0.data_stream(ctx.clone(), args).await?;
        let output = OutputPort::create();
        let source = StreamSource::create(ctx, Some(block_stream), output.clone())?;

        pipeline.add_pipe(Pipe::SimplePipe {
            inputs_port: vec![],
            outputs_port: vec![output],
            processors: vec![source],
        });

        Ok(())
    }

    fn result_schema(&self) -> Arc<DataSchema> {
        self.0.schema()
    }
}
