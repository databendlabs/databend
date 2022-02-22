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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_planners::Expression;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::new::processors::transforms::transform::Transformer;
use crate::pipelines::transforms::ExpressionExecutor;

pub type TransformHaving = TransformFilterImpl<true>;
pub type TransformFilter = TransformFilterImpl<false>;

pub struct TransformFilterImpl<const HAVING: bool> {
    executor: Arc<ExpressionExecutor>,
}

impl<const HAVING: bool> TransformFilterImpl<HAVING>
where Self: Transform
{
    pub fn try_create(
        schema: DataSchemaRef,
        predicate: Expression,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let predicate_executor = Self::expr_executor(&schema, &predicate)?;
        predicate_executor.validate()?;
        Ok(Transformer::create(input, output, TransformFilterImpl {
            executor: Arc::new(predicate_executor),
        }))
    }

    fn expr_executor(schema: &DataSchemaRef, expr: &Expression) -> Result<ExpressionExecutor> {
        let expr_field = expr.to_data_field(schema)?;
        let expr_schema = DataSchemaRefExt::create(vec![expr_field]);

        ExpressionExecutor::try_create(
            "filter expression executor",
            schema.clone(),
            expr_schema,
            vec![expr.clone()],
            false,
        )
    }
}

impl Transform for TransformFilterImpl<true> {
    const NAME: &'static str = "HavingTransform";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let filter_block = self.executor.execute(&data)?;
        DataBlock::filter_block(&data, filter_block.column(0))
    }
}

impl Transform for TransformFilterImpl<false> {
    const NAME: &'static str = "FilterTransform";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let filter_block = self.executor.execute(&data)?;
        DataBlock::filter_block(&data, filter_block.column(0))
    }
}
