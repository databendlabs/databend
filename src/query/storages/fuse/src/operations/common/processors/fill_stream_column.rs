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
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;

use common_catalog::plan::StreamColumn;
use common_catalog::plan::StreamColumnMeta;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_expression::FieldIndex;
use common_expression::ScalarRef;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;

pub struct FillStreamColumnProcessor {
    stream_columns: BTreeMap<FieldIndex, StreamColumn>,
    data_blocks: VecDeque<(StreamColumnMeta, DataBlock)>,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
}

impl FillStreamColumnProcessor {
    pub fn create(
        stream_columns: BTreeMap<FieldIndex, StreamColumn>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Self {
        Self {
            stream_columns,
            data_blocks: VecDeque::new(),
            input,
            output,
            output_data: None,
        }
    }
}

#[async_trait::async_trait]
impl Processor for FillStreamColumnProcessor {
    fn name(&self) -> String {
        "FillStreamColumnProcessor".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(source_meta) = data_block.take_meta() {
                if let Some(stream_column_meta) = StreamColumnMeta::downcast_from(source_meta) {
                    self.data_blocks.push_back((stream_column_meta, data_block));
                    return Ok(Event::Sync);
                }
            }

            unreachable!();
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some((stream_column_meta, data_block)) = self.data_blocks.pop_front() {
            let mut columns = data_block.columns().to_vec();
            let num_columns = data_block.num_columns();
            let num_rows = data_block.num_rows();
            for (index, stream_column) in self.stream_columns.iter() {
                if *index < num_columns {
                    let val = columns[*index].value.index(0).unwrap();
                    if matches!(val, ScalarRef::Null) {
                        columns[*index] =
                            stream_column.generate_column_values(&stream_column_meta, num_rows);
                    }
                } else {
                    columns
                        .push(stream_column.generate_column_values(&stream_column_meta, num_rows));
                }
            }
            self.output_data = Some(DataBlock::new(
                data_block.columns().to_vec(),
                data_block.num_rows(),
            ));
        }
        Ok(())
    }
}
