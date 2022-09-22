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

use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_legacy_planners::LegacyExpression;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;

use crate::processors::transforms::transform::Transform;
use crate::processors::transforms::transform::Transformer;
use crate::processors::transforms::ExpressionExecutor;

pub type ProjectionTransform = ExpressionTransformImpl<true>;
pub type ExpressionTransform = ExpressionTransformImpl<false>;

pub struct ExpressionTransformImpl<const ALIAS_PROJECT: bool> {
    executor: ExpressionExecutor,
}

impl<const ALIAS_PROJECT: bool> ExpressionTransformImpl<ALIAS_PROJECT>
where Self: Transform
{
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        exprs: Vec<LegacyExpression>,
        ctx: Arc<dyn TableContext>,
    ) -> Result<ProcessorPtr> {
        let executor = ExpressionExecutor::try_create(
            ctx,
            "expression executor",
            input_schema,
            output_schema,
            exprs,
            ALIAS_PROJECT,
        )?;
        executor.validate()?;

        Ok(Transformer::create(input, output, Self { executor }))
    }
}

impl Transform for ExpressionTransformImpl<true> {
    const NAME: &'static str = "ProjectionTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        self.executor.execute(&data)
    }
}

impl Transform for ExpressionTransformImpl<false> {
    const NAME: &'static str = "ExpressionTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        self.executor.execute(&data)
    }
}
