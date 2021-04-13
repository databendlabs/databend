// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_planners::ExpressionPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::stream::StreamExt;
use hashbrown::HashMap;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;

// Table for <group_key, indices>
type GroupTable = HashMap<Vec<u8>, Vec<u32>, ahash::RandomState>;

pub struct GroupByPartialTransform {
    group_exprs: Vec<ExpressionPlan>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
}

impl GroupByPartialTransform {
    pub fn create(
        schema: DataSchemaRef,
        _aggr_exprs: Vec<ExpressionPlan>,
        group_exprs: Vec<ExpressionPlan>,
    ) -> Self {
        Self {
            group_exprs,
            schema,
            input: Arc::new(EmptyProcessor::create()),
        }
    }
}

#[async_trait::async_trait]
impl IProcessor for GroupByPartialTransform {
    fn name(&self) -> &str {
        "GroupByPartialTransform"
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
        let group_fields = self
            .group_exprs
            .iter()
            .map(|x| x.to_data_field(&self.schema))
            .collect::<Result<Vec<_>>>()?;
        let group_schema = Arc::new(DataSchema::new(group_fields));

        let group_funcs = self
            .group_exprs
            .iter()
            .map(|x| x.to_function())
            .collect::<Result<Vec<_>>>()?;
        let group_funcs_length = group_funcs.len();

        let mut blocks = vec![];
        let mut stream = self.input.execute().await?;
        while let Some(block) = stream.next().await {
            let block = block?;
            let mut group_indices = GroupTable::default();
            let mut group_columns = Vec::with_capacity(group_funcs_length);

            // 1.1 Eval the group expr columns.
            {
                for func in &group_funcs {
                    group_columns.push(func.eval(&block)?.to_array(block.num_rows())?);
                }
            }

            // 1.2 Make group with indices.
            {
                for row in 0..block.num_rows() {
                    let mut group_key = vec![];
                    for col in &group_columns {
                        common_datavalues::concat_row_to_one_key(col, row, &mut group_key)?;
                    }
                    group_indices
                        .raw_entry_mut()
                        .from_key(&group_key)
                        .and_modify(|_k, v| v.push(row as u32))
                        .or_insert_with(|| (group_key.clone(), vec![row as u32]));
                }
            }

            // 1.3 Get all sub blocks group by group_key.
            {}

            let group_block = DataBlock::create(group_schema.clone(), group_columns);
            blocks.push(group_block);
        }

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            blocks,
        )))
    }
}
