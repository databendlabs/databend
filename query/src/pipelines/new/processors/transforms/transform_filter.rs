use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues2::DataSchemaRef;
use common_datavalues2::DataSchemaRefExt;
use common_exception::Result;
use common_planners::Expression;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::new::processors::transforms::transform::Transformer;
use crate::pipelines::transforms::ExpressionExecutor;

pub struct TransformFilter {
    executor: Arc<ExpressionExecutor>,
}

impl TransformFilter {
    pub fn try_create(
        schema: DataSchemaRef,
        predicate: Expression,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let predicate_executor = Self::expr_executor(&schema, &predicate)?;
        predicate_executor.validate()?;
        Ok(Transformer::create(input, output, TransformFilter {
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

impl Transform for TransformFilter {
    const NAME: &'static str = "FilterTransform";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let filter_block = self.executor.execute(&data)?;
        DataBlock::filter_block(&data, filter_block.column(0))
    }
}
