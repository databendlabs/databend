// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use bumpalo::Bump;
use common_datablocks::DataBlock;
use common_datavalues::arrays::BinaryArrayBuilder;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

// Table for <group_key, (place, keys) >
type GroupFuncTable = RwLock<HashMap<Vec<u8>, (Vec<usize>, Vec<DataValue>), ahash::RandomState>>;

pub struct GroupByPartialTransform {
    aggr_exprs: Vec<Expression>,
    group_exprs: Vec<Expression>,

    schema: DataSchemaRef,
    schema_before_group_by: DataSchemaRef,
    input: Arc<dyn Processor>,
    groups: GroupFuncTable,
}

impl GroupByPartialTransform {
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
impl Processor for GroupByPartialTransform {
    fn name(&self) -> &str {
        "GroupByPartialTransform"
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

    /// Create hash group based on row index and apply the function with vector.
    /// For example:
    /// row_idx, A
    /// 0, 1
    /// 1, 2
    /// 2, 3
    /// 3, 4
    /// 4, 5
    ///
    /// grouping by [A%3]
    /// 1.1)
    /// row_idx, group_key, A
    /// 0, 1, 1
    /// 1, 2, 2
    /// 2, 0, 3
    /// 3, 1, 4
    /// 4, 2, 5
    ///
    /// 1.2) make indices group(for vector compute)
    /// group_key, indices
    /// 0, [2]
    /// 1, [0, 3]
    /// 2, [1, 4]
    ///
    /// 1.3) apply aggregate function(SUM(A)) to the take block
    /// group_key, SUM(A)
    /// <0, 3>
    /// <1, 1+4>
    /// <2, 2+5>
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("execute...");

        let aggr_len = self.aggr_exprs.len();
        let start = Instant::now();
        let schema_before_group_by = self.schema_before_group_by.clone();
        let mut funcs = Vec::with_capacity(self.aggr_exprs.len());
        let mut arg_names = Vec::with_capacity(self.aggr_exprs.len());
        let mut aggr_cols = Vec::with_capacity(self.aggr_exprs.len());

        for expr in self.aggr_exprs.iter() {
            funcs.push(expr.to_aggregate_function(&schema_before_group_by)?);
            arg_names.push(expr.to_aggregate_function_names()?);
            aggr_cols.push(expr.column_name());
        }
        let group_cols = self
            .group_exprs
            .iter()
            .map(|x| x.column_name())
            .collect::<Vec<_>>();

        let mut stream = self.input.execute().await?;
        let arena = Bump::new();
        while let Some(block) = stream.next().await {
            let block = block?;
            // 1.1 and 1.2.
            let group_blocks = DataBlock::group_by(&block, &group_cols)?;
            // 1.3 Apply take blocks to aggregate function by group_key.
            {
                for (group_key, group_keys, take_block) in group_blocks {
                    let rows = take_block.num_rows();

                    let mut groups = self.groups.write();
                    match groups.get_mut(&group_key) {
                        // New group.
                        None => {
                            let mut places = Vec::with_capacity(aggr_cols.len());
                            for (idx, _aggr_col) in aggr_cols.iter().enumerate() {
                                let func = funcs[idx].clone();
                                let place = funcs[idx].allocate_state(&arena);

                                let arg_columns = arg_names[idx]
                                    .iter()
                                    .map(|arg| {
                                        take_block.try_column_by_name(arg).map(|c| c.clone())
                                    })
                                    .collect::<Result<Vec<DataColumn>>>()?;
                                func.accumulate(place, &arg_columns, rows)?;

                                places.push(place);
                            }

                            groups.insert(group_key.clone(), (places, group_keys));
                        }
                        // Accumulate result against the take block by indices.
                        Some((places, _)) => {
                            for (idx, _aggr_col) in aggr_cols.iter().enumerate() {
                                let arg_columns = arg_names[idx]
                                    .iter()
                                    .map(|arg| {
                                        take_block.try_column_by_name(arg).map(|c| c.clone())
                                    })
                                    .collect::<Result<Vec<DataColumn>>>()?;

                                funcs[idx].accumulate(places[idx], &arg_columns, rows)?
                            }
                        }
                    }
                }
            }
        }

        let delta = start.elapsed();
        tracing::debug!("Group by partial cost: {:?}", delta);

        let groups = self.groups.read();
        if groups.is_empty() {
            return Ok(Box::pin(DataBlockStream::create(
                DataSchemaRefExt::create(vec![]),
                None,
                vec![],
            )));
        }

        let mut group_arrays = Vec::with_capacity(group_cols.len());
        for _i in 0..group_cols.len() {
            group_arrays.push(Vec::with_capacity(groups.len()));
        }

        // Builders.
        let mut state_builders: Vec<BinaryArrayBuilder> = (0..aggr_len)
            .map(|_| BinaryArrayBuilder::new(groups.len() * 4))
            .collect();

        let mut group_key_builder = BinaryArrayBuilder::new(groups.len());
        for (key, (places, values)) in groups.iter() {
            for (idx, func) in funcs.iter().enumerate() {
                let mut writer = vec![];
                func.serialize(places[idx], &mut writer)?;

                state_builders[idx].append_value(&writer);
            }

            for (i, value) in values.iter().enumerate() {
                group_arrays[i].push(value.clone());
            }
            // Keys
            group_key_builder.append_value(key);
        }

        let mut columns: Vec<Series> = Vec::with_capacity(self.schema.fields().len());
        for mut builder in state_builders {
            columns.push(builder.finish().into_series());
        }
        for (i, values) in group_arrays.iter().enumerate() {
            columns.push(DataValue::try_into_data_array(
                values,
                &self.group_exprs[i].to_data_type(&self.schema_before_group_by)?,
            )?)
        }
        let array = group_key_builder.finish();
        columns.push(array.into_series());

        let block = DataBlock::create_by_array(self.schema.clone(), columns);
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
