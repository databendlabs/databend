// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use common_aggregate_functions::AggregateFunction;
use common_datablocks::DataBlock;
use common_datavalues::DataArrayRef;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_datavalues::StringArray;
use common_exception::Result;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::stream::StreamExt;
use log::info;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

pub struct AggregatorPartialTransform {
    funcs: Vec<Box<dyn AggregateFunction>>,
    arg_names: Vec<Vec<String>>,

    schema: DataSchemaRef,
    input: Arc<dyn Processor>,
}

impl AggregatorPartialTransform {
    pub fn try_create(
        schema: DataSchemaRef,
        schema_before_groupby: DataSchemaRef,
        exprs: Vec<Expression>,
    ) -> Result<Self> {
        let funcs = exprs
            .iter()
            .map(|expr| expr.to_aggregate_function(&schema_before_groupby))
            .collect::<Result<Vec<_>>>()?;

        let arg_names = exprs
            .iter()
            .map(|expr| expr.to_aggregate_function_names())
            .collect::<Result<Vec<_>>>()?;

        Ok(AggregatorPartialTransform {
            funcs,
            arg_names,
            schema,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait::async_trait]
impl Processor for AggregatorPartialTransform {
    fn name(&self) -> &str {
        "AggregatorPartialTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let mut funcs = self.funcs.clone();
        let mut stream = self.input.execute().await?;
        let arg_names = self.arg_names.clone();

        let start = Instant::now();
        while let Some(block) = stream.next().await {
            let block = block?;
            let rows = block.num_rows();

            for (idx, func) in funcs.iter_mut().enumerate() {
                let mut arg_columns = vec![];
                for name in arg_names[idx].iter() {
                    arg_columns.push(block.try_column_by_name(name)?.clone());
                }
                func.accumulate(&arg_columns, rows)?;
            }
        }
        let delta = start.elapsed();
        info!("Aggregator partial cost: {:?}", delta);

        let mut columns: Vec<DataArrayRef> = vec![];
        for func in funcs.iter() {
            // Column.
            let states = DataValue::Struct(func.accumulate_result()?);
            let ser = serde_json::to_string(&states)?;
            let col = Arc::new(StringArray::from(vec![ser.as_str()]));
            columns.push(col);
        }

        let block = DataBlock::create_by_array(self.schema.clone(), columns);

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
