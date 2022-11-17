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

use std::any::Any;
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfo;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipe;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::processors::sources::AsyncSource;
use common_pipeline_sources::processors::sources::AsyncSourcer;
use common_pipeline_sources::processors::sources::EmptySource;
use common_pipeline_sources::processors::sources::SyncSource;
use common_pipeline_sources::processors::sources::SyncSourcer;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SystemTablePart;

#[typetag::serde(name = "system")]
impl PartInfo for SystemTablePart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<SystemTablePart>() {
            None => false,
            Some(other) => self == other,
        }
    }

    fn hash(&self) -> u64 {
        0
    }
}

pub trait SyncSystemTable: Send + Sync {
    const NAME: &'static str;

    fn get_table_info(&self) -> &TableInfo;
    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock>;

    fn get_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((
            PartStatistics::default(),
            Partitions::create(PartitionsShuffleKind::Seq, vec![Arc::new(Box::new(
                SystemTablePart,
            ))]),
        ))
    }
}

pub struct SyncOneBlockSystemTable<TTable: SyncSystemTable> {
    inner_table: Arc<TTable>,
}

impl<TTable: 'static + SyncSystemTable> SyncOneBlockSystemTable<TTable>
where Self: Table
{
    pub fn create(inner: TTable) -> Arc<dyn Table> {
        Arc::new(SyncOneBlockSystemTable::<TTable> {
            inner_table: Arc::new(inner),
        })
    }
}

#[async_trait::async_trait]
impl<TTable: 'static + SyncSystemTable> Table for SyncOneBlockSystemTable<TTable> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        self.inner_table.get_table_info()
    }

    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        self.inner_table.get_partitions(ctx, push_downs)
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        // avoid duplicate read in cluster mode.
        if plan.parts.partitions.is_empty() {
            let output = OutputPort::create();
            pipeline.add_pipe(Pipe::SimplePipe {
                inputs_port: vec![],
                outputs_port: vec![output.clone()],
                processors: vec![EmptySource::create(output)?],
            });

            return Ok(());
        }

        let output = OutputPort::create();
        let inner_table = self.inner_table.clone();
        pipeline.add_pipe(Pipe::SimplePipe {
            processors: vec![SystemTableSyncSource::create(
                ctx,
                output.clone(),
                inner_table,
            )?],
            inputs_port: vec![],
            outputs_port: vec![output],
        });

        Ok(())
    }
}

struct SystemTableSyncSource<TTable: 'static + SyncSystemTable> {
    finished: bool,
    inner: Arc<TTable>,
    context: Arc<dyn TableContext>,
}

impl<TTable: 'static + SyncSystemTable> SystemTableSyncSource<TTable>
where Self: SyncSource
{
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        inner: Arc<TTable>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx.clone(), output, SystemTableSyncSource::<TTable> {
            inner,
            context: ctx,
            finished: false,
        })
    }
}

impl<TTable: 'static + SyncSystemTable> SyncSource for SystemTableSyncSource<TTable> {
    const NAME: &'static str = TTable::NAME;

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }

        self.finished = true;
        Ok(Some(self.inner.get_full_data(self.context.clone())?))
    }
}

#[async_trait::async_trait]
pub trait AsyncSystemTable: Send + Sync {
    const NAME: &'static str;

    fn get_table_info(&self) -> &TableInfo;
    async fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock>;

    async fn get_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((
            PartStatistics::default(),
            Partitions::create(PartitionsShuffleKind::Seq, vec![Arc::new(Box::new(
                SystemTablePart,
            ))]),
        ))
    }
}

pub struct AsyncOneBlockSystemTable<TTable: AsyncSystemTable> {
    inner_table: Arc<TTable>,
}

impl<TTable: 'static + AsyncSystemTable> AsyncOneBlockSystemTable<TTable>
where Self: Table
{
    pub fn create(inner: TTable) -> Arc<dyn Table> {
        Arc::new(AsyncOneBlockSystemTable::<TTable> {
            inner_table: Arc::new(inner),
        })
    }
}

#[async_trait::async_trait]
impl<TTable: 'static + AsyncSystemTable> Table for AsyncOneBlockSystemTable<TTable> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        self.inner_table.get_table_info()
    }

    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        self.inner_table.get_partitions(ctx, push_downs).await
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let output = OutputPort::create();
        let inner_table = self.inner_table.clone();
        pipeline.add_pipe(Pipe::SimplePipe {
            processors: vec![SystemTableAsyncSource::create(
                output.clone(),
                inner_table,
                ctx,
            )?],
            inputs_port: vec![],
            outputs_port: vec![output],
        });

        Ok(())
    }
}

struct SystemTableAsyncSource<TTable: 'static + AsyncSystemTable> {
    finished: bool,
    inner: Arc<TTable>,
    context: Arc<dyn TableContext>,
}

impl<TTable: 'static + AsyncSystemTable> SystemTableAsyncSource<TTable>
where Self: AsyncSource
{
    pub fn create(
        output: Arc<OutputPort>,
        inner: Arc<TTable>,
        context: Arc<dyn TableContext>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(context.clone(), output, SystemTableAsyncSource::<TTable> {
            inner,
            context,
            finished: false,
        })
    }
}

#[async_trait::async_trait]
impl<TTable: 'static + AsyncSystemTable> AsyncSource for SystemTableAsyncSource<TTable> {
    const NAME: &'static str = TTable::NAME;

    #[async_trait::unboxed_simple]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }

        self.finished = true;
        Ok(Some(self.inner.get_full_data(self.context.clone()).await?))
    }
}
