// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use common_datablocks::DataBlock;
use common_datavalues::DataArrayRef;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::StringArray;
use common_exception::Result;
use common_functions::IFunction;
use common_planners::ExpressionAction;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::stream::StreamExt;
use log::info;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;
use common_aggregate_functions::IAggreagteFunction;

pub struct AggregatorPartialTransform {
    funcs: Vec<Box<dyn IAggreagteFunction>>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>
}

impl AggregatorPartialTransform {
    pub fn try_create(schema: DataSchemaRef, exprs: Vec<ExpressionAction>) -> Result<Self> {
        let mut funcs = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            funcs.push(expr.to_function()?);
        }

        Ok(AggregatorPartialTransform {
            funcs,
            schema,
            input: Arc::new(EmptyProcessor::create())
        })
    }
}

#[async_trait::async_trait]
impl IProcessor for AggregatorPartialTransform {
    fn name(&self) -> &str {
        "AggregatorPartialTransform"
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
        let mut funcs = self.funcs.clone();
        let mut stream = self.input.execute().await?;

        let start = Instant::now();
        while let Some(block) = stream.next().await {
            let block = block?;

            for func in funcs.iter_mut() {
                func.accumulate(&block)?;
            }
        }
        let delta = start.elapsed();
        info!("Aggregator partial cost: {:?}", delta);

        let mut fields = Vec::with_capacity(funcs.len());
        let mut columns: Vec<DataArrayRef> = Vec::with_capacity(funcs.len());
        for func in &funcs {
            // Field.
            let field = DataField::new(format!("{}", func).as_str(), DataType::Utf8, false);
            fields.push(field);

            // Column.
            let states = DataValue::Struct(func.accumulate_result()?);
            let ser = serde_json::to_string(&states)?;
            let col = Arc::new(StringArray::from(vec![ser.as_str()]));
            columns.push(col);
        }

        let schema = DataSchemaRefExt::create(fields);
        let block = DataBlock::create(schema, columns);
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block]
        )))
    }
}
