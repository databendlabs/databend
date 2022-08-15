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
    fn name(&self) -> &str;

    fn features(&self) -> ProcedureFeatures;

    fn validate(&self, ctx: Arc<QueryContext>, args: &[String]) -> Result<()> {
        let features = self.features();
        validate_function_arg(
            self.name(),
            args.len(),
            features.variadic_arguments,
            features.num_arguments,
        )?;
        if features.management_mode_required && !ctx.get_config().query.management_mode {
            return Err(ErrorCode::ManagementModePermissionDenied(format!(
                "Access denied: '{}' only used in management-mode",
                self.name()
            )));
        }
        if let Some(user_option_flag) = features.user_option_flag {
            let user_info = ctx.get_current_user()?;
            if !user_info.has_option_flag(user_option_flag) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Access denied: '{}' requires user {} option flag",
                    self.name(),
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

    fn schema(&self) -> Arc<DataSchema>;
}

/// Procedure that returns all the data in one DataBlock
/// For procedures that returns a small amount of data only.
/// If procedure may return a large amount of data, please use [StreamProcedure]
///
/// Technically, it is not [Procedure] but a builder of procedure.
/// The method `into_procedure` is be used while registering to [ProcedureFactory],
#[async_trait::async_trait]
pub trait OneBlockProcedure {
    fn into_procedure(self) -> Box<dyn Procedure>
    where
        Self: Send + Sync,
        Self: Sized + 'static,
    {
        Box::new(impls::OneBlockProcedureWrapper(self))
    }

    fn name(&self) -> &str;

    fn features(&self) -> ProcedureFeatures;

    async fn all_data(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock>;

    fn schema(&self) -> Arc<DataSchema>;
}

/// Procedure that returns data as [SendableBlockStream]
///
/// Technically, it is not [Procedure] but a builder of procedure.
/// The method `into_procedure` is be used while registering to [ProcedureFactory],
#[async_trait::async_trait]
pub trait StreamProcedure
where Self: Sized
{
    fn into_procedure(self) -> Box<dyn Procedure>
    where
        Self: Send + Sync,
        Self: Sized + 'static,
    {
        Box::new(impls::StreamProcedureWrapper(self))
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

mod impls {

    use super::*;

    // To avoid implementation conflicts, introduce a new type
    pub(in self::super) struct OneBlockProcedureWrapper<T>(pub T);

    #[async_trait::async_trait]
    impl<T> Procedure for OneBlockProcedureWrapper<T>
    where T: OneBlockProcedure + Sync + Send
    {
        fn name(&self) -> &str {
            self.0.name()
        }

        fn features(&self) -> ProcedureFeatures {
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

        fn schema(&self) -> Arc<DataSchema> {
            self.0.schema()
        }
    }

    // To avoid implementation conflicts, introduce a new type
    pub(in self::super) struct StreamProcedureWrapper<T>(pub T);

    #[async_trait::async_trait]
    impl<T> Procedure for StreamProcedureWrapper<T>
    where T: StreamProcedure + Sync + Send
    {
        fn name(&self) -> &str {
            self.0.name()
        }

        fn features(&self) -> ProcedureFeatures {
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

        fn schema(&self) -> Arc<DataSchema> {
            self.0.schema()
        }
    }
}
