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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use async_trait::unboxed_simple;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::BlockRowIndex;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use opendal::Operator;

use crate::io;
use crate::io::TableMetaLocationGenerator;
use crate::io::WriteSettings;

pub struct AggIndexSink {
    data_accessor: Operator,
    index_id: u64,
    write_settings: WriteSettings,
    sink_schema: TableSchemaRef,
    block_name_offset: usize,
    keep_block_name_col: bool,
    location_data: HashMap<String, Vec<BlockRowIndex>>,
    blocks: Vec<DataBlock>,
}

impl AggIndexSink {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        input: Arc<InputPort>,
        data_accessor: Operator,
        index_id: u64,
        write_settings: WriteSettings,
        sink_schema: TableSchemaRef,
        block_name_offset: usize,
        keep_block_name_col: bool,
    ) -> Result<ProcessorPtr> {
        let sinker = AsyncSinker::create(input, AggIndexSink {
            data_accessor,
            index_id,
            write_settings,
            sink_schema,
            block_name_offset,
            keep_block_name_col,
            location_data: HashMap::new(),
            blocks: vec![],
        });

        Ok(ProcessorPtr::create(sinker))
    }

    fn process_block(&mut self, block: &mut DataBlock) {
        let col = block.get_by_offset(self.block_name_offset);
        let block_name_col = col.value.try_downcast::<StringType>().unwrap();
        let block_id = self.blocks.len();
        for i in 0..block.num_rows() {
            let location = block_name_col.index(i).unwrap().to_string();

            self.location_data
                .entry(location)
                .and_modify(|idx_vec| idx_vec.push((block_id as u32, i as u32, 1)))
                .or_insert(vec![(block_id as u32, i as u32, 1)]);
        }
        let mut result = DataBlock::new(vec![], block.num_rows());

        for (idx, col) in block.columns().iter().enumerate() {
            if !self.keep_block_name_col && idx == self.block_name_offset {
                continue;
            }
            result.add_column(col.clone());
        }

        self.blocks.push(result);
    }
}

#[async_trait]
impl AsyncSink for AggIndexSink {
    const NAME: &'static str = "AggIndexSink";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        for (loc, indexes) in &self.location_data {
            let start = Instant::now();
            let block = DataBlock::take_blocks(&self.blocks, indexes, indexes.len());
            let loc = TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                loc,
                self.index_id,
            );
            let mut data = vec![];
            io::serialize_block(
                &self.write_settings,
                &self.sink_schema,
                block,
                &mut data,
                &Default::default(),
            )?;

            {
                metrics_inc_agg_index_write_nums(1);
                metrics_inc_agg_index_write_bytes(data.len() as u64);
                metrics_inc_agg_index_write_milliseconds(start.elapsed().as_millis() as u64);
            }

            self.data_accessor.write(&loc, data).await?;
        }
        Ok(())
    }

    #[unboxed_simple]
    #[async_backtrace::framed]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        let mut block = data_block;
        self.process_block(&mut block);

        Ok(false)
    }
}
