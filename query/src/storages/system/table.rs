use std::collections::HashMap;
use std::sync::Arc;
use common_arrow::arrow_format::ipc::flatbuffers::bitflags::_core::any::Any;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_meta_types::{MetaId, TableInfo};
use common_planners::{Expression, Extras, Partitions, ReadDataSourcePlan, Statistics, TruncateTablePlan};
use common_streams::{DataBlockStream, SendableDataBlockStream};
use crate::sessions::QueryContext;
use crate::storages::Table;
use common_exception::Result;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::processors::SyncSource;

trait SyncSystemTable: Send + Sync {
    const NAME: &'static str;

    fn get_table_info(&self) -> &TableInfo;
    fn get_full_data(&self, ctx: Arc<QueryContext>) -> Result<DataBlock>;
}

#[async_trait::async_trait]
trait ASyncSystemTable: Send + Sync {
    const NAME: &'static str;

    fn get_table_info(&self) -> &TableInfo;
    async fn get_full_data(&self, ctx: Arc<QueryContext>) -> Result<DataBlock>;
}

struct SyncOneBlockSystemTable<TTable: SyncSystemTable> {
    inner_table: Arc<TTable>,
}

#[async_trait::async_trait]
impl<TTable: 'static + SyncSystemTable> Table for SyncOneBlockSystemTable<TTable> {
    fn as_any(&self) -> &dyn Any { self }

    fn get_table_info(&self) -> &TableInfo {
        self.inner_table.get_table_info()
    }

    async fn read(&self, ctx: Arc<QueryContext>, _: &ReadDataSourcePlan) -> Result<SendableDataBlockStream> {
        let block = self.inner_table.get_full_data(ctx)?;
        Ok(Box::pin(DataBlockStream::create(block.schema().clone(), None, vec![block])))
    }

    fn read2(&self, ctx: Arc<QueryContext>, _: &ReadDataSourcePlan, pipeline: &mut NewPipeline) -> Result<()> {
        // let schema = self.table_info.schema();
        // let output = OutputPort::create();
        // pipeline.add_pipe(NewPipe::SimplePipe {
        //     processors: vec![OneSource::create(output.clone(), schema)?],
        //     inputs_port: vec![],
        //     outputs_port: vec![output],
        // });

        Ok(())
    }
}

struct SystemTableSyncSource<TTable: SyncSystemTable> {
    inner: TTable,
    context: Arc<QueryContext>,
}

impl<TTable: SyncSystemTable> SyncSource for SystemTableSyncSource<TTable> {
    const NAME: &'static str = TTable::NAME;

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        Ok(Some(self.inner.get_full_data(self.context.clone())?))
    }
}

