// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::try_into_data_array;
use common_datavalues::DataArrayRef;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_functions::IFunction;
use common_infallible::RwLock;
use common_planners::ExpressionPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::stream::StreamExt;
use hashbrown::HashMap;
use log::info;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;

// Table for <group_key, indices>
type GroupFuncTable = RwLock<HashMap<Vec<u8>, Vec<Box<dyn IFunction>>, ahash::RandomState>>;

pub struct GroupByFinalTransform {
    aggr_exprs: Vec<ExpressionPlan>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
    groups: GroupFuncTable,
}

impl GroupByFinalTransform {
    pub fn create(
        schema: DataSchemaRef,
        aggr_exprs: Vec<ExpressionPlan>,
        _group_exprs: Vec<ExpressionPlan>,
    ) -> Self {
        Self {
            aggr_exprs,
            schema,
            input: Arc::new(EmptyProcessor::create()),
            groups: RwLock::new(HashMap::default()),
        }
    }
}

#[async_trait::async_trait]
impl IProcessor for GroupByFinalTransform {
    fn name(&self) -> &str {
        "GroupByFinalTransform"
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
        let aggr_funcs = self
            .aggr_exprs
            .iter()
            .map(|x| x.to_function())
            .collect::<Result<Vec<_>>>()?;
        let aggr_funcs_length = aggr_funcs.len();

        let start = Instant::now();
        let mut stream = self.input.execute().await?;
        while let Some(block) = stream.next().await {
            let mut groups = self.groups.write();
            let block = block?;
            for row in 0..block.num_rows() {
                if let DataValue::Binary(Some(group_key)) =
                    DataValue::try_from_array(block.column(aggr_funcs_length), row)?
                {
                    match groups.get_mut(&group_key) {
                        None => {
                            let mut funcs = aggr_funcs.clone();
                            for (i, func) in funcs.iter_mut().enumerate() {
                                if let DataValue::Utf8(Some(col)) =
                                    DataValue::try_from_array(block.column(i), row)?
                                {
                                    let val: DataValue = serde_json::from_str(&col)?;
                                    if let DataValue::Struct(states) = val {
                                        func.merge(&states)?;
                                    }
                                }
                            }
                            groups.insert(group_key.clone(), funcs);
                        }
                        Some(funcs) => {
                            for (i, func) in funcs.iter_mut().enumerate() {
                                if let DataValue::Utf8(Some(col)) =
                                    DataValue::try_from_array(block.column(i), row)?
                                {
                                    let val: DataValue = serde_json::from_str(&col)?;
                                    if let DataValue::Struct(states) = val {
                                        func.merge(&states)?;
                                    }
                                }
                            }
                        }
                    };
                }
            }
        }
        let delta = start.elapsed();
        info!("Group by final cost: {:?}", delta);

        // Collect the merge states.
        let groups = self.groups.read();
        let mut aggr_values: Vec<Vec<DataValue>> = {
            let mut values = vec![];
            for _i in 0..aggr_funcs_length {
                values.push(vec![])
            }
            values
        };
        for (_, aggr_funcs) in groups.iter() {
            for (i, func) in aggr_funcs.iter().enumerate() {
                let merge = func.merge_result()?;
                aggr_values[i].push(merge);
            }
        }

        // Build final state block.
        let mut columns: Vec<DataArrayRef> = Vec::with_capacity(self.aggr_exprs.len());
        for value in &aggr_values {
            columns.push(try_into_data_array(value.as_slice())?);
        }
        let block = DataBlock::create(self.schema.clone(), columns);

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
