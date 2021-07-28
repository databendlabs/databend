// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use bumpalo::Bump;
use common_datablocks::DataBlock;
use common_datablocks::HashMethodKind;
use common_datavalues::prelude::*;
use common_datavalues::DFBinaryArray;
use common_datavalues::DFUInt16Array;
use common_datavalues::DFUInt32Array;
use common_datavalues::DFUInt64Array;
use common_datavalues::DFUInt8Array;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

pub struct GroupByFinalTransform {
    max_block_size: usize,
    aggr_exprs: Vec<Expression>,
    group_exprs: Vec<Expression>,
    schema: DataSchemaRef,
    schema_before_group_by: DataSchemaRef,
    input: Arc<dyn Processor>,
}

impl GroupByFinalTransform {
    pub fn create(
        schema: DataSchemaRef,
        max_block_size: usize,
        schema_before_group_by: DataSchemaRef,
        aggr_exprs: Vec<Expression>,
        group_exprs: Vec<Expression>,
    ) -> Self {
        Self {
            max_block_size,
            aggr_exprs,
            group_exprs,
            schema,
            schema_before_group_by,
            input: Arc::new(EmptyProcessor::create()),
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

        let group_cols = self
            .group_exprs
            .iter()
            .map(|x| x.column_name())
            .collect::<Vec<_>>();

        let start = Instant::now();
        let arena = Bump::new();

        let mut stream = self.input.execute().await?;
        let sample_block = DataBlock::empty_with_schema(self.schema.clone());
        let method = DataBlock::choose_hash_method(&sample_block, &group_cols)?;

        macro_rules! apply {
            ($hash_method: ident, $key_array_type: ty, $downcast_fn: ident, $group_func_table: ty) => {{
                type GroupFuncTable = $group_func_table;
                let groups_locker = GroupFuncTable::default();

                while let Some(block) = stream.next().await {
                    let mut groups = groups_locker.write();
                    let block = block?;

                    let key_array = block.column(aggr_funcs_len + group_expr_len).to_array()?;
                    let key_array: $key_array_type = key_array.$downcast_fn()?;

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
                        let group_key = $hash_method.get_key(&key_array, row);
                        match groups.get_mut(&group_key) {
                            None => {
                                let mut places = Vec::with_capacity(aggr_funcs_len);
                                for (i, func) in aggr_funcs.iter().enumerate() {
                                    let mut data = states_binary_arrays[i].value(row);
                                    let place = func.allocate_state(&arena);
                                    func.deserialize(place, &mut data)?;
                                    places.push(place);
                                }
                                let mut values = Vec::with_capacity(group_expr_len);
                                for i in 0..group_expr_len {
                                    values.push(block.column(i + aggr_funcs_len).try_get(row)?);
                                }

                                groups.insert(group_key, (places, values));
                            }
                            Some((places, _)) => {
                                for (i, func) in aggr_funcs.iter().enumerate() {
                                    let mut data = states_binary_arrays[i].value(row);
                                    let place = func.allocate_state(&arena);
                                    func.deserialize(place, &mut data)?;
                                    func.merge(places[i], place)?;
                                }
                            }
                        };
                    }
                }
                let delta = start.elapsed();
                tracing::debug!("Group by final cost: {:?}", delta);

                // Collect the merge states.
                let groups = groups_locker.read();

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
                    blocks = DataBlock::split_block_by_size(&block, self.max_block_size)?;
                }

                Ok(Box::pin(DataBlockStream::create(
                    self.schema.clone(),
                    None,
                    blocks,
                )))
            }};
        }

        macro_rules! match_hash_method_and_apply {
            ($method: ident, $apply: ident) => {{
                match $method {
                    HashMethodKind::Serializer(hash_method) => {
                        apply! { hash_method,  &DFBinaryArray, binary,   RwLock<HashMap<Vec<u8>, (Vec<usize>, Vec<DataValue>), ahash::RandomState>>}
                    }
                    HashMethodKind::KeysU8(hash_method) => {
                        apply! { hash_method , &DFUInt8Array, u8,  RwLock<HashMap<u8, (Vec<usize>, Vec<DataValue>), ahash::RandomState>> }
                    }
                    HashMethodKind::KeysU16(hash_method) => {
                        apply! { hash_method , &DFUInt16Array, u16,  RwLock<HashMap<u16, (Vec<usize>, Vec<DataValue>), ahash::RandomState>> }
                    }
                    HashMethodKind::KeysU32(hash_method) => {
                        apply! { hash_method , &DFUInt32Array, u32,  RwLock<HashMap<u32, (Vec<usize>, Vec<DataValue>), ahash::RandomState>> }
                    }
                    HashMethodKind::KeysU64(hash_method) => {
                        apply! { hash_method , &DFUInt64Array, u64,  RwLock<HashMap<u64, (Vec<usize>, Vec<DataValue>), ahash::RandomState>> }
                    }
                }
            }};
        }

        match_hash_method_and_apply! {method, apply}
    }
}
