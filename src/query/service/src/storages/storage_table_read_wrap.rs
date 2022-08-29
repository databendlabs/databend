//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_base::base::GlobalIORuntime;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;
use common_streams::SendableDataBlockStream;

use crate::interpreters::ProcessorExecutorStream;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::Pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::storages::Table;

#[async_trait::async_trait]
pub trait TableStreamReadWrap: Send + Sync {
    async fn read(
        &self,
        _ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream>;
}

#[async_trait::async_trait]
impl<T: Table> TableStreamReadWrap for T {
    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let mut pipeline = Pipeline::create();
        self.read2(ctx.clone(), plan, &mut pipeline)?;

        let settings = ctx.get_settings();
        let async_runtime = GlobalIORuntime::instance();
        let query_need_abort = ctx.query_need_abort();
        pipeline.set_max_threads(settings.get_max_threads()? as usize);
        let executor_settings = ExecutorSettings::try_create(&settings)?;

        let executor = PipelinePullingExecutor::try_create(
            async_runtime,
            query_need_abort,
            pipeline,
            executor_settings,
        )?;

        Ok(Box::pin(ProcessorExecutorStream::create(executor)?))
    }
}

#[async_trait::async_trait]
impl TableStreamReadWrap for dyn Table {
    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let mut pipeline = Pipeline::create();
        self.read2(ctx.clone(), plan, &mut pipeline)?;

        let settings = ctx.get_settings();
        let async_runtime = GlobalIORuntime::instance();
        let query_need_abort = ctx.query_need_abort();
        pipeline.set_max_threads(settings.get_max_threads()? as usize);
        let executor_settings = ExecutorSettings::try_create(&settings)?;
        let executor = PipelinePullingExecutor::try_create(
            async_runtime,
            query_need_abort,
            pipeline,
            executor_settings,
        )?;

        Ok(Box::pin(ProcessorExecutorStream::create(executor)?))
    }
}
