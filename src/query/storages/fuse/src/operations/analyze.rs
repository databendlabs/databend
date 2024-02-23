// Copyright 2021 Datafuse Labs
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

use async_trait::async_trait;
use async_trait::unboxed_simple;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_io::prelude::borsh_deserialize_from_slice;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_storages_common_table_meta::meta::MetaHLL;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;

use crate::FuseTable;

impl FuseTable {
    #[allow(clippy::too_many_arguments)]
    pub fn do_analyze(
        ctx: Arc<dyn TableContext>,
        output_schema: Arc<DataSchema>,
        catalog: &str,
        database: &str,
        table: &str,
        snapshot_id: SnapshotId,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_sink(|input| {
            SinkAnalyzeState::create(
                ctx.clone(),
                output_schema.clone(),
                catalog,
                database,
                table,
                snapshot_id,
                input,
            )
        })?;
        Ok(())
    }
}

struct SinkAnalyzeState {
    ctx: Arc<dyn TableContext>,
    output_schema: Arc<DataSchema>,

    catalog: String,
    database: String,
    table: String,
    snapshot_id: SnapshotId,
}

impl SinkAnalyzeState {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output_schema: Arc<DataSchema>,
        catalog: &str,
        database: &str,
        table: &str,
        snapshot_id: SnapshotId,
        input: Arc<InputPort>,
    ) -> Result<ProcessorPtr> {
        let sinker = AsyncSinker::create(input, ctx.clone(), SinkAnalyzeState {
            ctx,
            output_schema,
            catalog: catalog.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            snapshot_id,
        });
        Ok(ProcessorPtr::create(sinker))
    }
}

#[async_trait]
impl AsyncSink for SinkAnalyzeState {
    const NAME: &'static str = "SinkAnalyzeState";

    #[unboxed_simple]
    #[async_backtrace::framed]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        if data_block.num_rows() == 0 {
            return Ok(false);
        }
        // always use the latest table
        let table = self
            .ctx
            .get_table(&self.catalog, &self.database, &self.table)
            .await?;

        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = table.read_table_snapshot().await?;
        if snapshot.is_none() {
            return Ok(true);
        }
        let table_statistics = table
            .read_table_snapshot_statistics(snapshot.as_ref())
            .await?;

        let is_full = data_block.columns()[self.output_schema.num_fields() - 1]
            .value
            .index(0)
            .unwrap();

        let is_full = is_full.as_boolean().unwrap();

        let mut ndv_states = table_statistics.map(|s| s.hll.clone()).unwrap_or_default();

        let index_num = self.output_schema.num_fields() - 1;

        for (f, col) in self
            .output_schema
            .fields()
            .iter()
            .take(index_num)
            .zip(data_block.columns())
        {
            let name = f.name();
            let index: u32 = name.strip_prefix("ndv_").unwrap().parse().unwrap();

            let col = col.value.index(0).unwrap();
            let col = col.as_binary().unwrap();
            let hll: MetaHLL = borsh_deserialize_from_slice(*col)?;

            if !is_full {
                ndv_states
                    .entry(index)
                    .and_modify(|c| c.merge(&hll))
                    .or_insert(hll);
            } else {
                ndv_states.insert(index, hll);
            }
        }

        let snapshot = snapshot.unwrap();
        // 3. Generate new table statistics
        let table_statistics = TableSnapshotStatistics::new(ndv_states, self.snapshot_id.clone());
        let table_statistics_location = table
            .meta_location_generator
            .snapshot_statistics_location_from_uuid(
                &table_statistics.snapshot_id,
                table_statistics.format_version(),
            )?;

        println!(
            "write to {:?} {:?}",
            table_statistics_location,
            table_statistics.column_distinct_values()
        );

        // 4. Save table statistics
        let mut new_snapshot = TableSnapshot::from_previous(&snapshot);
        new_snapshot.table_statistics_location = Some(table_statistics_location);

        FuseTable::commit_to_meta_server(
            self.ctx.as_ref(),
            &table.table_info,
            &table.meta_location_generator,
            new_snapshot,
            Some(table_statistics),
            &None,
            &table.operator,
        )
        .await?;

        Ok(true)
    }
}
