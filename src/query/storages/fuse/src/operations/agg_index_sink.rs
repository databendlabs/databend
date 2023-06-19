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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockRowIndex;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FieldIndex;
use common_expression::ScalarRef;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;
use common_expression::BLOCK_NAME_COL_NAME;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_sql::ColumnBinding;
use opendal::Operator;

use crate::io;
use crate::io::TableMetaLocationGenerator;
use crate::io::WriteSettings;

enum State {
    None,
    PreProcess(DataBlock),
    PostProcess,
    Finished,
}

pub struct AggIndexSink {
    state: State,
    input: Arc<InputPort>,
    data_accessor: Operator,
    index_id: u64,
    write_settings: WriteSettings,
    source_schema: TableSchemaRef,
    projections: Vec<FieldIndex>,
    block_location: Option<FieldIndex>,
    location_data: HashMap<String, Vec<BlockRowIndex>>,
    blocks: Vec<DataBlock>,
    finished: bool,
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

        let mut new_source_schema = TableSchema::from(source_schema.as_ref().clone());

        if !user_defined_block_name {
            new_source_schema.drop_column(BLOCK_NAME_COL_NAME)?;
        }

        dbg!(&projections);
        dbg!(&block_location);
        dbg!(&new_source_schema);
        Ok(ProcessorPtr::create(Box::new(AggIndexSink {
            state: State::None,
            input,
            data_accessor,
            index_id,
            write_settings,
            source_schema: Arc::new(new_source_schema),
            projections,
            block_location,
            location_data: HashMap::new(),
            blocks: vec![],
            finished: false,
        })))
    }

    fn process_block(&mut self, block: &mut DataBlock) {
        dbg!(&block.columns());
        let col = block.get_by_offset(self.block_location.unwrap());
        let block_id = self.blocks.len();
        for i in 0..block.num_rows() {
            let location = unsafe {
                match col.value.index_unchecked(i) {
                    ScalarRef::String(loc) => String::from_utf8_unchecked(loc.to_vec()),
                    _ => unreachable!(),
                }
            };

            self.location_data
                .entry(location)
                .and_modify(|idx_vec| idx_vec.push((block_id, i, 1)))
                .or_insert(vec![(block_id, i, 1)]);
        }
        dbg!(&self.location_data);
        let mut result = DataBlock::new(vec![], block.num_rows());
        for index in self.projections.iter() {
            result.add_column(block.get_by_offset(*index).clone());
        }
        self.blocks.push(result);
    }
}

#[async_trait]
impl Processor for AggIndexSink {
    fn name(&self) -> String {
        "AggIndexSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.finished {
            self.state = State::Finished;
            return Ok(Event::Finished);
        }

        if self.input.is_finished() {
            self.state = State::PostProcess;
            return Ok(Event::Async);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        let data_block = self.input.pull_data().unwrap()?;
        if data_block.is_empty() {
            // data source like
            //  `select number from numbers(3000000) where number >=2000000 and number < 3000000`
            // may generate empty data blocks
            Ok(Event::NeedData)
        } else {
            self.state = State::PreProcess(data_block);
            Ok(Event::Sync)
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::PreProcess(mut block) => {
                let block = self.process_block(&mut block);
                dbg!(&block);
            }
            _state => {
                return Err(ErrorCode::Internal("Unknown state for fuse table sink"));
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::PostProcess => {
                let blocks = self.blocks.iter().map(|b| b).collect::<Vec<_>>();
                for (loc, indexes) in &self.location_data {
                    let block = DataBlock::take_blocks(&blocks, indexes, indexes.len());
                    let loc =
                        TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                            loc,
                            self.index_id,
                        );
                    dbg!(&loc);
                    dbg!(&block);
                    let mut data = vec![];
                    io::serialize_block(
                        &self.write_settings,
                        &self.source_schema,
                        block,
                        &mut data,
                    )?;
                    self.data_accessor.write(&loc, data).await?;
                }
                self.finished = true;
            }
            _state => {
                return Err(ErrorCode::Internal("Unknown state for fuse table sink."));
            }
        }

        Ok(())
    }
}
