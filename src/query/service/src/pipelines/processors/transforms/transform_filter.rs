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
use std::sync::Arc;

use common_catalog::plan::AggIndexMeta;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::optimizer::ColumnSet;

use crate::pipelines::processors::Event;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;

/// Filter the input [`DataBlock`] with the predicate `expr`.
pub struct FilterState {
    expr: Expr,
    projections: ColumnSet,
    func_ctx: FunctionContext,
}

impl FilterState {
    fn new(expr: Expr, projections: ColumnSet, func_ctx: FunctionContext) -> Self {
        assert_eq!(expr.data_type(), &DataType::Boolean);
        Self {
            expr,
            projections,
            func_ctx,
        }
    }

    fn filter(&self, data_block: DataBlock) -> Result<DataBlock> {
        let num_evals = data_block
            .get_meta()
            .and_then(AggIndexMeta::downcast_ref_from)
            .map(|a| a.num_evals);

        if let Some(num_evals) = num_evals {
            // It's from aggregating index.
            Ok(data_block.project_with_agg_index(&self.projections, num_evals))
        } else {
            let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
            let filter = evaluator
                .run(&self.expr)?
                .try_downcast::<BooleanType>()
                .unwrap();
            let data_block = data_block.project(&self.projections);
            data_block.filter_boolean_value(&filter)
        }
    }
}

pub struct TransformFilter {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,

    filter_state: FilterState,
}

impl TransformFilter {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        expr: Expr,
        projections: ColumnSet,
        func_ctx: FunctionContext,
    ) -> Result<Box<dyn Processor>> {
        let filter_state = FilterState::new(expr, projections, func_ctx);
        Ok(Box::new(TransformFilter {
            input_port,
            output_port,
            input_data: None,
            output_data: None,
            filter_state,
        }))
    }
}

#[async_trait::async_trait]
impl Processor for TransformFilter {
    fn name(&self) -> String {
        "Filter".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output_port.is_finished() {
            self.input_port.finish();
            return Ok(Event::Finished);
        }

        if !self.output_port.can_push() {
            self.input_port.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_data.take() {
            self.output_port.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input_port.has_data() {
            let data = self.input_port.pull_data().unwrap()?;
            self.input_data = Some(data);
            return Ok(Event::Sync);
        }

        if self.input_port.is_finished() {
            self.output_port.finish();
            return Ok(Event::Finished);
        }

        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            let data_block = self.filter_state.filter(data_block)?;
            if data_block.num_rows() > 0 {
                self.output_data = Some(data_block)
            }
        }
        Ok(())
    }
}
