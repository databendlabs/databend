// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common_aggregate_functions::IAggregateFunction;
use common_datablocks::DataBlock;
use common_datavalues::DataArrayRef;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::ExpressionAction;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::stream::StreamExt;
use log::info;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;

// Table for <group_key, indices>
type GroupFuncTable =
    RwLock<HashMap<Vec<u8>, Vec<Box<dyn IAggregateFunction>>, ahash::RandomState>>;

// Group Key ==> Group by values
type GroupKeyTable = RwLock<HashMap<Vec<u8>, Vec<DataValue>>>;

pub struct GroupByFinalTransform {
    aggr_exprs: Vec<ExpressionAction>,
    group_exprs: Vec<ExpressionAction>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
    groups: GroupFuncTable,
    keys: GroupKeyTable
}

impl GroupByFinalTransform {
    pub fn create(
        schema: DataSchemaRef,
        aggr_exprs: Vec<ExpressionAction>,
        group_exprs: Vec<ExpressionAction>
    ) -> Self {
        Self {
            aggr_exprs,
            group_exprs,
            schema,
            input: Arc::new(EmptyProcessor::create()),
            groups: RwLock::new(HashMap::default()),
            keys: RwLock::new(HashMap::default())
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
            .map(|x| x.to_aggregate_function())
            .collect::<Result<Vec<_>>>()?;

        let aggr_funcs_len = aggr_funcs.len();
        let group_expr_len = self.group_exprs.len();

        let start = Instant::now();
        let mut stream = self.input.execute().await?;
        while let Some(block) = stream.next().await {
            let mut groups = self.groups.write();
            let mut keys = self.keys.write();
            let block = block?;
            for row in 0..block.num_rows() {
                if let DataValue::Binary(Some(group_key)) =
                    DataValue::try_from_array(block.column(1 + aggr_funcs_len), row)?
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

                            if let DataValue::Utf8(Some(col)) =
                                DataValue::try_from_array(block.column(aggr_funcs_len), row)?
                            {
                                let val: DataValue = serde_json::from_str(&col)?;
                                if let DataValue::Struct(states) = val {
                                    keys.insert(group_key.clone(), states);
                                }
                            }
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
        let keys = self.keys.read();

        let mut group_values: Vec<Vec<DataValue>> = {
            let mut values = vec![];
            for _i in 0..group_expr_len {
                values.push(vec![])
            }
            values
        };

        let mut aggr_values: Vec<Vec<DataValue>> = {
            let mut values = vec![];
            for _i in 0..aggr_funcs_len {
                values.push(vec![])
            }
            values
        };
        for (key, aggr_funcs) in groups.iter() {
            if let Some(values) = keys.get(key) {
                for (i, value) in values.iter().enumerate() {
                    group_values[i].push(value.clone());
                }
            }

            for (i, func) in aggr_funcs.iter().enumerate() {
                let merge = func.merge_result()?;
                aggr_values[i].push(merge);
            }
        }

        // Build final state block.
        let mut columns: Vec<DataArrayRef> = Vec::with_capacity(aggr_funcs_len + group_expr_len);

        for value in &aggr_values {
            if !value.is_empty() {
                columns.push(DataValue::try_into_data_array(value.as_slice())?);
            }
        }

        for value in &group_values {
            if !value.is_empty() {
                columns.push(DataValue::try_into_data_array(value.as_slice())?);
            }
        }

        let mut blocks = vec![];
        if !columns.is_empty() {
            let block = DataBlock::create_by_array(self.schema.clone(), columns);
            blocks.push(block);
        }

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            blocks
        )))
    }
}
