// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;
use std::time::Instant;

use bumpalo::Bump;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use common_datablocks::{DataBlock, HashMethodKeysU8};
use common_datablocks::HashMethod;
use common_datablocks::HashMethodKind;
use common_datavalues::arrays::BinaryArrayBuilder;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::aggregates::get_layout_offsets;
use common_functions::aggregates::StateAddr;
use common_infallible::RwLock;
use common_io::prelude::*;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::pipelines::transforms::group_by::{Aggregator, PolymorphicKeysHelper};
use crate::common::HashTableKeyable;

pub struct GroupByPartialTransform {
    aggr_exprs: Vec<Expression>,
    group_exprs: Vec<Expression>,

    schema: DataSchemaRef,
    schema_before_group_by: DataSchemaRef,
    input: Arc<dyn Processor>,
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
    /// For each row, allocate the state if key not exists in the map, and apply accumulate_row row by row
    ///  row_idx, A % 3 -> state place
    ///  0, 1 -> state1
    ///  1, 2 -> state2
    ///  2, 3 -> state3
    ///  3, 1 -> state1
    ///  4, 2 -> state2
    /// 1.2)  serialize the state to the output block
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("execute...");
        let group_cols = self
            .group_exprs
            .iter()
            .map(|x| x.column_name())
            .collect::<Vec<_>>();
        let sample_block = DataBlock::empty_with_schema(self.schema_before_group_by.clone());
        let method = DataBlock::choose_hash_method(&sample_block, &group_cols)?;

        macro_rules! apply {
            ($hash_method: ident, $key_array_builder: ty, $group_func_table: ty) => {{
                // Table for <group_key, (place, keys) >
                let start = Instant::now();
                let mut stream = self.input.execute().await?;

                let aggr_len = self.aggr_exprs.len();
                let schema_before_group_by = self.schema_before_group_by.clone();
                let mut funcs = Vec::with_capacity(self.aggr_exprs.len());
                let mut arg_names = Vec::with_capacity(self.aggr_exprs.len());
                let mut aggr_cols = Vec::with_capacity(self.aggr_exprs.len());
                let aggr_funcs_len = self.aggr_exprs.len();

                for expr in self.aggr_exprs.iter() {
                    funcs.push(expr.to_aggregate_function(&schema_before_group_by)?);
                    arg_names.push(expr.to_aggregate_function_names()?);
                    aggr_cols.push(expr.column_name());
                }

                let arena = Bump::new();
                let (layout, offsets_aggregate_states) = unsafe { get_layout_offsets(&funcs) };

                type GroupFuncTable = $group_func_table;
                let groups_locker = GroupFuncTable::default();
                while let Some(block) = stream.next().await {
                    let block = block?;
                    // 1.1 and 1.2.
                    let mut group_columns = Vec::with_capacity(group_cols.len());
                    {
                        for col in group_cols.iter() {
                            group_columns.push(block.try_column_by_name(col)?);
                        }
                    }

                    let mut aggr_arg_columns = Vec::with_capacity(aggr_cols.len());
                    for (idx, _aggr_col) in aggr_cols.iter().enumerate() {
                        let arg_columns = arg_names[idx]
                            .iter()
                            .map(|arg| block.try_column_by_name(arg).and_then(|c| c.to_array()))
                            .collect::<Result<Vec<Series>>>()?;
                        aggr_arg_columns.push(arg_columns);
                    }

                    // this can benificial for the case of dereferencing
                    let aggr_arg_columns_slice = &aggr_arg_columns;

                    let mut places = Vec::with_capacity(block.num_rows());
                    let group_keys = $hash_method.build_keys(&group_columns, block.num_rows())?;
                    let mut groups = groups_locker.write();
                    {
                        for group_key in group_keys.iter() {
                            match groups.get(group_key) {
                                // New group.
                                None => {
                                    if aggr_funcs_len == 0 {
                                        groups.insert(group_key.clone(), 0);
                                    } else {
                                        let place: StateAddr = arena.alloc_layout(layout).into();
                                        for idx in 0..aggr_len {
                                            let arg_place =
                                                place.next(offsets_aggregate_states[idx]);
                                            funcs[idx].init_state(arg_place);
                                        }
                                        places.push(place);
                                        groups.insert(group_key.clone(), place.addr());
                                    }
                                }
                                // Accumulate result against the take block by indices.
                                Some(place) => {
                                    let place: StateAddr = (*place).into();
                                    places.push(place);
                                }
                            }
                        }

                        for ((idx, func), args) in
                            funcs.iter().enumerate().zip(aggr_arg_columns_slice.iter())
                        {
                            func.accumulate_keys(
                                &places,
                                offsets_aggregate_states[idx],
                                args,
                                block.num_rows(),
                            )?;
                        }
                    }
                }

                let delta = start.elapsed();
                tracing::debug!("Group by partial cost: {:?}", delta);

                let groups = groups_locker.read();
                if groups.is_empty() {
                    return Ok(Box::pin(DataBlockStream::create(
                        DataSchemaRefExt::create(vec![]),
                        None,
                        vec![],
                    )));
                }

                // Builders.
                let mut state_builders: Vec<BinaryArrayBuilder> = (0..aggr_len)
                    .map(|_| BinaryArrayBuilder::with_capacity(groups.len() * 4))
                    .collect();

                type KeyBuilder = $key_array_builder;
                let mut group_key_builder = KeyBuilder::with_capacity(groups.len());

                let mut bytes = BytesMut::new();
                for (key, place) in groups.iter() {
                    let place: StateAddr = (*place).into();

                    for (idx, func) in funcs.iter().enumerate() {
                        let arg_place = place.next(offsets_aggregate_states[idx]);
                        func.serialize(arg_place, &mut bytes)?;
                        state_builders[idx].append_value(&bytes[..]);
                        bytes.clear();
                    }

                    group_key_builder.append_value((*key).clone());
                }

                let mut columns: Vec<Series> = Vec::with_capacity(self.schema.fields().len());
                for mut builder in state_builders {
                    columns.push(builder.finish().into_series());
                }
                let array = group_key_builder.finish();
                columns.push(array.into_series());

                let block = DataBlock::create_by_array(self.schema.clone(), columns);
                Ok(Box::pin(DataBlockStream::create(
                    self.schema.clone(),
                    None,
                    vec![block],
                )))
            }};
        }

        match method {
            HashMethodKind::Serializer(hash_method) => {
                apply! { hash_method, BinaryArrayBuilder , RwLock<HashMap<Vec<u8>, usize, ahash::RandomState>>}
            }
            HashMethodKind::KeysU8(hash_method) => {
                // TODO: use fixed array.
                self.execute_impl(group_cols, hash_method).await
            }
            HashMethodKind::KeysU16(hash_method) => {
                self.execute_impl(group_cols, hash_method).await

            }
            HashMethodKind::KeysU32(hash_method) => {
                self.execute_impl(group_cols, hash_method).await
            }
            HashMethodKind::KeysU64(hash_method) => {
                self.execute_impl(group_cols, hash_method).await
            }
        }
    }
}

impl GroupByPartialTransform {
    async fn execute_impl<Method: HashMethod + PolymorphicKeysHelper<Method>>(&self, group_cols: Vec<String>, hash_method: Method) -> common_exception::Result<SendableDataBlockStream>
        where Method::HashKey: HashTableKeyable
    {
        let start = Instant::now();

        let mut stream = self.input.execute().await?;
        let aggr_exprs = &self.aggr_exprs;
        let schema = self.schema_before_group_by.clone();
        let aggregator = Aggregator::create(hash_method, aggr_exprs, schema)?;
        let groups_locker = aggregator.aggregate(group_cols, stream).await?;

        let delta = start.elapsed();
        tracing::debug!("Group by partial cost: {:?}", delta);

        let groups = groups_locker.read();
        let finalized_schema = self.schema.clone();
        aggregator.aggregate_finalized(&groups.0, finalized_schema)
    }
}
