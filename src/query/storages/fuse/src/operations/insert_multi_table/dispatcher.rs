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
use std::ops::BitAnd;
use std::ops::BitOr;
use std::ops::Not;
use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
pub struct Dispatcher {
    filters: Vec<Expr>,
    keep_remain: bool,
    ctx: Arc<dyn TableContext>,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data: Vec<DataBlock>,
}

impl Dispatcher {
    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        filters: Vec<Expr>,
        keep_remain: bool,
        ctx: Arc<dyn TableContext>,
    ) -> Box<dyn Processor> {
        Box::new(Dispatcher {
            filters,
            keep_remain,
            ctx,
            input: input_port,
            output: output_port,
            input_data: None,
            output_data: vec![],
        })
    }
}

#[async_trait::async_trait]
impl Processor for Dispatcher {
    fn name(&self) -> String {
        "Dispatcher".to_string()
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

        if let Some(data) = self.output_data.pop() {
            self.output.push_data(Ok(data));
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(input_data) = self.input_data.take() {
            let output_data = dispatch(
                input_data,
                &self.filters,
                self.keep_remain,
                self.ctx.clone(),
            )?;
            self.output_data.extend(output_data);
        }
        Ok(())
    }
}

/// Dispatch the data block to different branches according to the filters.
pub fn dispatch(
    data_block: DataBlock,
    filters: &[Expr],
    keep_remain: bool,
    ctx: Arc<dyn TableContext>,
) -> Result<Vec<DataBlock>> {
    // TODO: A naive impl, need to be optimized.
    let mut result = vec![];
    let mut filtered = MutableBitmap::from_len_zeroed(data_block.num_rows());
    let func_ctx = ctx.get_function_context()?;
    for filter in filters {
        let evaluator = Evaluator::new(&data_block, &func_ctx, &BUILTIN_FUNCTIONS);
        let values = evaluator
            .run(filter)
            .map_err(|e| e.add_message("eval filter failed:"))?
            .try_downcast::<BooleanType>()
            .unwrap();
        let bitmap = values.into_column().unwrap();
        let filter = bitmap.bitand(&Bitmap::from(filtered.clone().not()));
        result.push(data_block.clone().filter_with_bitmap(&filter)?);
        filtered = filtered.bitor(&filter);
    }
    if keep_remain {
        result.push(data_block.filter_with_bitmap(&Bitmap::from(filtered.clone().not()))?);
    }
    Ok(result)
}
