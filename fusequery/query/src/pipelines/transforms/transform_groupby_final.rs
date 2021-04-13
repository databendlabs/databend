// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
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
                                if let DataValue::String(Some(col)) =
                                    DataValue::try_from_array(block.column(i), 0)?
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
                                if let DataValue::String(Some(col)) =
                                    DataValue::try_from_array(block.column(i), 0)?
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

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![],
        )))
    }
}
