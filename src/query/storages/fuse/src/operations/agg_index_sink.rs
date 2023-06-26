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

use async_trait::async_trait;
use async_trait::unboxed_simple;
use common_exception::Result;
use common_expression::types::StringType;
use common_expression::types::ValueType;
use common_expression::BlockRowIndex;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FieldIndex;
use common_expression::TableSchemaRef;
use common_expression::BLOCK_NAME_COL_NAME;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_sinks::AsyncSink;
use common_pipeline_sinks::AsyncSinker;
use common_sql::ColumnBinding;
use opendal::Operator;

use crate::io;
use crate::io::TableMetaLocationGenerator;
use crate::io::WriteSettings;

pub struct AggIndexSink {
    data_accessor: Operator,
    index_id: u64,
    write_settings: WriteSettings,
    source_schema: TableSchemaRef,
    projections: Vec<FieldIndex>,
    block_location: Option<FieldIndex>,
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
        source_schema: TableSchemaRef,
        input_schema: DataSchemaRef,
        result_columns: &[ColumnBinding],
        user_defined_block_name: bool,
    ) -> Result<ProcessorPtr> {
        let mut projections = Vec::new();
        let mut block_location: Option<FieldIndex> = None;

        for column_binding in result_columns {
            let index = column_binding.index;
            if column_binding
                .column_name
                .eq_ignore_ascii_case(BLOCK_NAME_COL_NAME)
            {
                if user_defined_block_name {
                    projections.push(input_schema.index_of(index.to_string().as_str())?);
                }
                block_location = Some(input_schema.index_of(index.to_string().as_str())?);
            } else {
                projections.push(input_schema.index_of(index.to_string().as_str())?);
            }
        }

        let mut new_source_schema = source_schema.as_ref().clone();

        if !user_defined_block_name {
            new_source_schema.drop_column(BLOCK_NAME_COL_NAME)?;
        }

        let sinker = AsyncSinker::create(input, AggIndexSink {
            data_accessor,
            index_id,
            write_settings,
            source_schema: Arc::new(new_source_schema),
            projections,
            block_location,
            location_data: HashMap::new(),
            blocks: vec![],
        });

        Ok(ProcessorPtr::create(sinker))
    }

    fn process_block(&mut self, block: &mut DataBlock) {
        let col = block.get_by_offset(self.block_location.unwrap());
        let block_name_col = col.value.try_downcast::<StringType>().unwrap();
        let block_id = self.blocks.len();
        for i in 0..block.num_rows() {
            let location = unsafe {
                String::from_utf8_unchecked(StringType::to_owned_scalar(
                    block_name_col.index(i).unwrap(),
                ))
            };

            self.location_data
                .entry(location)
                .and_modify(|idx_vec| idx_vec.push((block_id as u32, i as u32, 1)))
                .or_insert(vec![(block_id as u32, i as u32, 1)]);
        }
        let mut result = DataBlock::new(vec![], block.num_rows());
        for index in self.projections.iter() {
            result.add_column(block.get_by_offset(*index).clone());
        }
        self.blocks.push(result);
    }
}

#[async_trait]
impl AsyncSink for AggIndexSink {
    const NAME: &'static str = "AggIndexSink";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        let blocks = self.blocks.iter().collect::<Vec<_>>();
        for (loc, indexes) in &self.location_data {
            let block = DataBlock::take_blocks(&blocks, indexes, indexes.len());
            let loc = TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                loc,
                self.index_id,
            );
            let mut data = vec![];
            io::serialize_block(&self.write_settings, &self.source_schema, block, &mut data)?;
            self.data_accessor.write(&loc, data).await?;
        }
        Ok(())
    }

    #[unboxed_simple]
    #[async_backtrace::framed]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        let mut block = data_block;
        self.process_block(&mut block);

        Ok(true)
    }
}
