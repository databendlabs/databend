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
use common_datavalues::DataType;
use common_exception::Result;
use common_planners::Expression;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::new::processors::transforms::transform::Transformer;
use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;

pub type TransformHaving = TransformFilterImpl<true>;
pub type TransformFilter = TransformFilterImpl<false>;

pub struct TransformFilterImpl<const HAVING: bool> {
    schema: DataSchemaRef,
    executor: ExpressionExecutor,
}

impl<const HAVING: bool> TransformFilterImpl<HAVING>
where Self: Transform
{
    pub fn try_create(
        schema: DataSchemaRef,
        predicate: Expression,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        ctx: Arc<QueryContext>,
    ) -> Result<ProcessorPtr> {
        let executor = Self::expr_executor(&schema, &predicate, ctx)?;
        executor.validate()?;
        Ok(Transformer::create(input, output, TransformFilterImpl {
            schema,
            executor,
        }))
    }

    fn expr_executor(
        schema: &DataSchemaRef,
        expr: &Expression,
        ctx: Arc<QueryContext>,
    ) -> Result<ExpressionExecutor> {
        let expr_field = expr.to_data_field(schema)?;
        let expr_schema = DataSchemaRefExt::create(vec![expr_field]);

        ExpressionExecutor::try_create(
            ctx,
            "filter expression executor",
            schema.clone(),
            expr_schema,
            vec![expr.clone()],
            false,
        )
    }

    fn correct_with_schema(&self, data_block: DataBlock) -> Result<DataBlock> {
        if !self.schema.eq(data_block.schema()) {
            let schema_fields = self.schema.fields();
            let mut new_columns = Vec::with_capacity(schema_fields.len());

            for schema_field in schema_fields {
                let column = data_block.try_column_by_name(schema_field.name())?;
                let physical_type = column.data_type_id().to_physical_type();
                let physical_type_expect =
                    schema_field.data_type().data_type_id().to_physical_type();

                if physical_type == physical_type_expect {
                    new_columns.push(column.clone())
                }
            }

            return Ok(DataBlock::create(self.schema.clone(), new_columns));
        }

        Ok(data_block)
    }
}

impl Transform for TransformFilterImpl<true> {
    const NAME: &'static str = "HavingTransform";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let filter_block = self.executor.execute(&data)?;
        self.correct_with_schema(DataBlock::filter_block(&data, filter_block.column(0))?)
    }
}

impl Transform for TransformFilterImpl<false> {
    const NAME: &'static str = "FilterTransform";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let filter_block = self.executor.execute(&data)?;
        self.correct_with_schema(DataBlock::filter_block(&data, filter_block.column(0))?)
    }
}
