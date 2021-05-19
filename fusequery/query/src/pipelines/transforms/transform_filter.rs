// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::convert::TryInto;
use std::sync::Arc;

use common_arrow::arrow;
use common_datablocks::DataBlock;
use common_datavalues as datavalues;
use common_datavalues::BooleanArray;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_functions::IFunction;
use common_planners::ExpressionAction;
use common_streams::SendableDataBlockStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;

pub struct FilterTransform {
    func: Box<dyn IFunction>,
    input: Arc<dyn IProcessor>
}

impl FilterTransform {
    pub fn try_create(predicate: ExpressionAction, having: bool) -> Result<Self> {
        let func = predicate.to_function()?;
        if !having && func.is_aggregator() {
            return Result::Err(ErrorCodes::SyntexException(format!(
                "Aggregate function {:?} is found in WHERE in query",
                predicate
            )));
        }

        Ok(FilterTransform {
            func,
            input: Arc::new(EmptyProcessor::create())
        })
    }
}

#[async_trait::async_trait]
impl IProcessor for FilterTransform {
    fn name(&self) -> &str {
        "FilterTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let func = self.func.clone();
        let input_stream = self.input.execute().await?;

        let executor = |func: Box<dyn IFunction>, block: Result<DataBlock>| -> Result<DataBlock> {
            let block = block?;
            let rows = block.num_rows();
            let filter_fn = func.clone();

            // Filter function eval result to array
            let filter_array = filter_fn.eval(&block)?.to_array(rows)?;
            // Downcast to boolean array
            let filter_array = datavalues::downcast_array!(filter_array, BooleanArray)?;
            // Convert to arrow record_batch
            let batch = block.try_into()?;
            let batch = arrow::compute::filter_record_batch(&batch, filter_array)?;
            batch.try_into()
        };

        let stream =
            input_stream.filter_map(move |v| executor(func.clone(), v).map(Some).transpose());
        Ok(Box::pin(stream))
    }
}
