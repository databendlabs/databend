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
use std::collections::HashSet;
use std::ops::Not;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchema;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_sql::executor::physical_plans::OnConflictField;
use databend_storages_common_table_meta::meta::ColumnStatistics;

use crate::operations::replace_into::mutator::ReplaceIntoMutator;

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
    delete_when: Option<(Expr, usize)>,
    ctx: Arc<dyn TableContext>,
}

impl ReplaceIntoProcessor {
    #[allow(dead_code)]
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        on_conflict_fields: Vec<OnConflictField>,
        cluster_keys: Vec<RemoteExpr<String>>,
        bloom_filter_column_indexes: Vec<FieldIndex>,
        table_schema: &TableSchema,
        target_table_empty: bool,
        table_range_idx: HashMap<ColumnId, ColumnStatistics>,
        delete_when: Option<(Expr, usize)>,
    ) -> Result<Self> {
        let replace_into_mutator = ReplaceIntoMutator::try_create(
            ctx.as_ref(),
            on_conflict_fields,
            cluster_keys,
            bloom_filter_column_indexes,
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
            delete_when,
            ctx,
        })
    }

    #[allow(dead_code)]
    pub fn into_pipe(self) -> Pipe {
        let pipe_item = self.into_pipe_item();
        Pipe::create(1, 2, vec![pipe_item])
    }

    #[allow(dead_code)]
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
        if let Some(mut data_block) = self.input_data.take() {
            let start = Instant::now();
            let mut filter = None;
            let mut all_delete = false;
            if let Some((expr, delete_column)) = &self.delete_when {
                let expr = expr.project_column_ref(|_| *delete_column);
                let func_ctx = self.ctx.get_function_context()?;
                let evaluator = Evaluator::new(&data_block, &func_ctx, &BUILTIN_FUNCTIONS);
                let predicates = evaluator
                    .run(&expr)
                    .map_err(|e| e.add_message("eval filter failed:"))?
                    .try_downcast::<BooleanType>()
                    .unwrap();
                match predicates {
                    Value::Scalar(scalar) => {
                        all_delete = scalar;
                    }
                    Value::Column(column) => {
                        filter = Some(column.not());
                    }
                }

                let column_num = data_block.num_columns();
                let projections = (0..column_num)
                    .filter(|i| i != delete_column)
                    .collect::<HashSet<_>>();
                data_block = data_block.project(&projections);
            };
            let merge_into_action = self.replace_into_mutator.process_input_block(&data_block)?;
            metrics_inc_replace_process_input_block_time_ms(start.elapsed().as_millis() as u64);
            metrics_inc_replace_block_number_input(1);
            if !self.target_table_empty {
                self.output_data_merge_into_action =
                    Some(DataBlock::empty_with_meta(Box::new(merge_into_action)));
            }

            if all_delete {
                return Ok(());
            }

            if let Some(filter) = filter {
                data_block = data_block.filter_with_bitmap(&filter)?;
            }

            metrics_inc_replace_append_blocks_rows(data_block.num_rows() as u64);

            if data_block.num_rows() > 0 {
                self.output_data_append = Some(data_block);
            }
            return Ok(());
        }

        Ok(())
    }
}
