// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use bumpalo::Bump;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DFBinaryArray;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

type GroupFuncTable = RwLock<HashMap<Vec<u8>, (Vec<usize>, Vec<DataValue>), ahash::RandomState>>;

pub struct GroupByFinalTransform {
    aggr_exprs: Vec<Expression>,
    group_exprs: Vec<Expression>,
    schema: DataSchemaRef,
    schema_before_group_by: DataSchemaRef,
    input: Arc<dyn Processor>,
    groups: GroupFuncTable,
}

impl GroupByFinalTransform {
    pub fn create(
        schema: DataSchemaRef,
        schema_before_group_by: DataSchemaRef,
        aggr_exprs: Vec<Expression>,
        group_exprs: Vec<Expression>,
    ) -> Self {
        Self {
            aggr_exprs,
            group_exprs,
            schema,
            schema_before_group_by,
            input: Arc::new(EmptyProcessor::create()),
            groups: RwLock::new(HashMap::default()),
        }
    }
}

#[async_trait::async_trait]
impl Processor for GroupByFinalTransform {
    fn name(&self) -> &str {
        "GroupByFinalTransform"
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
        tracing::debug!("execute...");
        let aggr_funcs = self
            .aggr_exprs
            .iter()
            .map(|x| x.to_aggregate_function(&self.schema_before_group_by))
            .collect::<Result<Vec<_>>>()?;

        let aggr_funcs_len = aggr_funcs.len();
        let group_expr_len = self.group_exprs.len();

        let start = Instant::now();
        let arena = Bump::new();
        let mut stream = self.input.execute().await?;
        while let Some(block) = stream.next().await {
            let mut groups = self.groups.write();
            let block = block?;

            let key_array = block.column(aggr_funcs_len + group_expr_len).to_array()?;
            let key_array: &DFBinaryArray = key_array.binary()?;
            let array = key_array.downcast_ref();

            let states_series = (0..aggr_funcs_len)
                .map(|i| block.column(i).to_array())
                .collect::<Result<Vec<_>>>()?;
            let mut states_binary_arrays = Vec::with_capacity(states_series.len());

            for agg in states_series.iter().take(aggr_funcs_len) {
                let aggr_array: &DFBinaryArray = agg.binary()?;
                let aggr_array = aggr_array.downcast_ref();
                states_binary_arrays.push(aggr_array);
            }

            for row in 0..block.num_rows() {
                let group_key = array.value(row);
                match groups.get_mut(group_key) {
                    None => {
                        let mut places = Vec::with_capacity(aggr_funcs_len);
                        for (i, func) in aggr_funcs.iter().enumerate() {
                            let data = states_binary_arrays[i].value(row);
                            let place = func.allocate_state(&arena);
                            func.deserialize(place, data)?;
                            places.push(place);
                        }
                        let mut values = Vec::with_capacity(group_expr_len);
                        for i in 0..group_expr_len {
                            values.push(block.column(i + aggr_funcs_len).try_get(row)?);
                        }

                        groups.insert(group_key.to_owned(), (places, values));
                    }
                    Some((places, _)) => {
                        for (i, func) in aggr_funcs.iter().enumerate() {
                            let data = states_binary_arrays[i].value(row);
                            let place = func.allocate_state(&arena);
                            func.deserialize(place, data)?;
                            func.merge(places[i], place)?;
                        }
                    }
                };
            }
        }
        let delta = start.elapsed();
        tracing::debug!("Group by final cost: {:?}", delta);

        // Collect the merge states.
        let groups = self.groups.read();

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
        for (_key, (places, values)) in groups.iter() {
            for (i, value) in values.iter().enumerate() {
                group_values[i].push(value.clone());
            }

            for (i, func) in aggr_funcs.iter().enumerate() {
                let merge = func.merge_result(places[i])?;
                aggr_values[i].push(merge);
            }
        }

        // Build final state block.
        let mut columns: Vec<Series> = Vec::with_capacity(aggr_funcs_len + group_expr_len);

        for (i, value) in aggr_values.iter().enumerate() {
            columns.push(DataValue::try_into_data_array(
                value.as_slice(),
                &self.aggr_exprs[i].to_data_type(&self.schema_before_group_by)?,
            )?);
        }

        for (i, value) in group_values.iter().enumerate() {
            columns.push(DataValue::try_into_data_array(
                value.as_slice(),
                &self.group_exprs[i].to_data_type(&self.schema_before_group_by)?,
            )?);
        }

        let mut blocks = vec![];
        if !columns.is_empty() {
            let block = DataBlock::create_by_array(self.schema.clone(), columns);
            blocks.push(block);
        }

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            blocks,
        )))
    }
}
