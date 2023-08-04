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
use std::time::Instant;

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::DataBlock;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::TableSchema;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_sql::executor::OnConflictField;
use storages_common_table_meta::meta::ColumnStatistics;

use crate::metrics::metrics_inc_replace_process_input_block_time_ms;
use crate::operations::replace_into::mutator::mutator_replace_into::ReplaceIntoMutator;

pub struct ReplaceIntoProcessor {
    replace_into_mutator: ReplaceIntoMutator,

    // stage data blocks
    input_port: Arc<InputPort>,
    output_port_merge_into_action: Arc<OutputPort>,
    output_port_append_data: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data_merge_into_action: Option<DataBlock>,
    output_data_append: Option<DataBlock>,

    target_table_empty: bool,
}

impl ReplaceIntoProcessor {
    pub fn create(
        ctx: &dyn TableContext,
        on_conflict_fields: Vec<OnConflictField>,
        cluster_keys: Vec<RemoteExpr<String>>,
        most_significant_on_conflict_field_index: Option<FieldIndex>,
        table_schema: &TableSchema,
        target_table_empty: bool,
        table_range_idx: HashMap<ColumnId, ColumnStatistics>,
    ) -> Result<Self> {
        let replace_into_mutator = ReplaceIntoMutator::try_create(
            ctx,
            on_conflict_fields,
            cluster_keys,
            most_significant_on_conflict_field_index,
            table_schema,
            table_range_idx,
        )?;
        let input_port = InputPort::create();
        let output_port_merge_into_action = OutputPort::create();
        let output_port_append_data = OutputPort::create();

        Ok(Self {
            replace_into_mutator,
            input_port,
            output_port_merge_into_action,
            output_port_append_data,
            input_data: None,
            output_data_merge_into_action: None,
            output_data_append: None,
            target_table_empty,
        })
    }

    pub fn into_pipe(self) -> Pipe {
        let pipe_item = self.into_pipe_item();
        Pipe::create(1, 2, vec![pipe_item])
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input_port.clone();
        let output_port_merge_into_action = self.output_port_merge_into_action.clone();
        let output_port_append_data = self.output_port_append_data.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![
            output_port_append_data,
            output_port_merge_into_action,
        ])
    }
}

#[async_trait::async_trait]
impl Processor for ReplaceIntoProcessor {
    fn name(&self) -> String {
        "ReplaceIntoTransform".to_owned()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
    fn event(&mut self) -> Result<Event> {
        let finished = self.input_port.is_finished()
            && self.output_data_append.is_none()
            && self.output_data_merge_into_action.is_none();

        if finished {
            self.output_port_merge_into_action.finish();
            self.output_port_append_data.finish();
            return Ok(Event::Finished);
        }

        let mut pushed_something = false;
        if self.output_port_append_data.can_push() {
            if let Some(data) = self.output_data_append.take() {
                self.output_port_append_data.push_data(Ok(data));
                pushed_something = true;
            }
        }

        if self.output_port_merge_into_action.can_push() {
            if let Some(data) = self.output_data_merge_into_action.take() {
                self.output_port_merge_into_action.push_data(Ok(data));
                pushed_something = true;
            }
        }

        if pushed_something {
            Ok(Event::NeedConsume)
        } else {
            if self.input_data.is_some() {
                return Ok(Event::Sync);
            }

            if self.input_port.has_data() {
                if self.output_data_append.is_none() && self.output_data_merge_into_action.is_none()
                {
                    // no pending data (being sent to down streams)
                    self.input_data = Some(self.input_port.pull_data().unwrap()?);
                    Ok(Event::Sync)
                } else {
                    // data pending
                    Ok(Event::NeedConsume)
                }
            } else {
                self.input_port.set_need_data();
                Ok(Event::NeedData)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            let start = Instant::now();
            let merge_into_action = self.replace_into_mutator.process_input_block(&data_block)?;
            metrics_inc_replace_process_input_block_time_ms(start.elapsed().as_millis() as u64);
            if !self.target_table_empty {
                self.output_data_merge_into_action =
                    Some(DataBlock::empty_with_meta(Box::new(merge_into_action)));
            }
            self.output_data_append = Some(data_block);
            return Ok(());
        }

        Ok(())
    }
}
