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
use std::collections::VecDeque;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;
use parking_lot::RwLock;

use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::SyncSource;
use crate::pipelines::processors::SyncSourcer;
use crate::pipelines::Pipeline;
use crate::pipelines::SourcePipeBuilder;
use crate::sessions::TableContext;
use crate::storages::Table;

pub struct ClusteringHistoryTable {
    table_info: TableInfo,
    max_rows: i32,
    data: Arc<RwLock<VecDeque<DataBlock>>>,
}

impl ClusteringHistoryTable {
    pub fn create(table_id: u64, max_rows: i32) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("start_time", TimestampType::new_impl(3)),
            DataField::new("end_time", TimestampType::new_impl(3)),
            DataField::new("database", Vu8::to_data_type()),
            DataField::new("table", Vu8::to_data_type()),
            DataField::new("reclustered_bytes", u64::to_data_type()),
            DataField::new("reclustered_rows", u64::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'clustering_history'".to_string(),
            name: "clustering_history".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemClusteringHistory".to_string(),
                ..Default::default()
            },
        };

        Self {
            table_info,
            max_rows,
            data: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    pub async fn append_data(
        &self,
        _ctx: Arc<dyn TableContext>,
        mut stream: SendableDataBlockStream,
    ) -> Result<()> {
        while let Some(block) = stream.next().await {
            let block = block?;
            self.data.write().push_back(block);
        }

        // Check overflow.
        let over = self.data.read().len() as i32 - self.max_rows;
        if over > 0 {
            for _x in 0..over {
                self.data.write().pop_front();
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Table for ClusteringHistoryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        Ok((Statistics::default(), vec![]))
    }

    fn read2(
        &self,
        ctx: Arc<dyn TableContext>,
        _: &ReadDataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        // TODO: split data for multiple threads
        let output = OutputPort::create();
        let mut source_builder = SourcePipeBuilder::create();

        source_builder.add_source(
            output.clone(),
            ClusteringHistorySource::create(ctx, output, &self.data.read())?,
        );

        pipeline.add_pipe(source_builder.finalize());
        Ok(())
    }

    async fn truncate(
        &self,
        _ctx: Arc<dyn TableContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        let mut data = self.data.write();
        *data = VecDeque::new();
        Ok(())
    }
}

struct ClusteringHistorySource {
    data: VecDeque<DataBlock>,
}

impl ClusteringHistorySource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        data: &VecDeque<DataBlock>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, ClusteringHistorySource { data: data.clone() })
    }
}

impl SyncSource for ClusteringHistorySource {
    const NAME: &'static str = "system.clustering_history";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        Ok(self.data.pop_front())
    }
}
