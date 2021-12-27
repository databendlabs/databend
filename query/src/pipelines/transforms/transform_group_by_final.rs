// Copyright 2021 Datafuse Labs.
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
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use bumpalo::Bump;
use common_arrow::arrow::array::MutableArray;
use common_arrow::arrow::array::MutableBinaryArray;
use common_arrow::arrow::array::MutableBooleanArray;
use common_arrow::arrow::array::MutablePrimitiveArray;
use common_arrow::arrow::datatypes::PhysicalType;
use common_datablocks::DataBlock;
use common_datablocks::HashMethodKind;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::get_layout_offsets;
use common_functions::aggregates::StateAddr;
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

macro_rules! with_match_primitive_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use common_arrow::arrow::datatypes::PrimitiveType::*;
    // use common_arrow::arrow::types::{days_ms, months_days_ns};
    match $key_type {
        Int8 => __with_ty__! { i8 },
        Int16 => __with_ty__! { i16 },
        Int32 => __with_ty__! { i32 },
        Int64 => __with_ty__! { i64 },
        UInt8 => __with_ty__! { u8 },
        UInt16 => __with_ty__! { u16 },
        UInt32 => __with_ty__! { u32 },
        UInt64 => __with_ty__! { u64 },
        Float32 => __with_ty__! { f32 },
        Float64 => __with_ty__! { f64 },
        _ => {}
    }
})}

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
        let funcs = self
            .aggr_exprs
            .iter()
            .map(|x| x.to_aggregate_function(&self.schema_before_group_by))
            .collect::<Result<Vec<_>>>()?;
        let aggr_funcs_len = funcs.len();
        let group_expr_len = self.group_exprs.len();

        let group_cols = self
            .group_exprs
            .iter()
            .map(|x| x.column_name())
            .collect::<Vec<_>>();

        let group_fields = self
            .group_exprs
            .iter()
            .map(|c| c.to_data_field(&self.schema_before_group_by))
            .collect::<Result<Vec<_>>>()?;

        let start = Instant::now();
        let arena = Bump::new();

        let mut stream = self.input.execute().await?;
        let sample_block = DataBlock::empty_with_schema(self.schema_before_group_by.clone());
        let method = DataBlock::choose_hash_method(&sample_block, &group_cols)?;

        let (layout, offsets_aggregate_states) = unsafe { get_layout_offsets(&funcs) };

        macro_rules! apply {
            ($hash_method: ident, $key_array_type: ty, $downcast_fn: ident, $group_func_table: ty) => {{
                type GroupFuncTable = $group_func_table;
                let groups_locker = GroupFuncTable::default();

                while let Some(block) = stream.next().await {
                    let mut groups = groups_locker.write();
                    let block = block?;

                    let key_array = block.column(aggr_funcs_len).to_array()?;
                    let key_array: $key_array_type = key_array.$downcast_fn()?;

                    let states_series = (0..aggr_funcs_len)
                        .map(|i| block.column(i).to_array())
                        .collect::<Result<Vec<_>>>()?;
                    let mut states_binary_arrays = Vec::with_capacity(states_series.len());

                    for agg in states_series.iter().take(aggr_funcs_len) {
                        let aggr_array: &DFStringArray = agg.string()?;
                        let aggr_array = aggr_array.inner();
                        states_binary_arrays.push(aggr_array);
                    }

                    for row in 0..block.num_rows() {
                        let group_key = $hash_method.get_key(&key_array, row);
                        match groups.get(&group_key) {
                            None => {
                                if aggr_funcs_len == 0 {
                                    groups.insert(group_key, 0usize);
                                } else {
                                    let place: StateAddr = arena.alloc_layout(layout).into();
                                    for (idx, func) in funcs.iter().enumerate() {
                                        let arg_place = place.next(offsets_aggregate_states[idx]);

                                        let mut data = states_binary_arrays[idx].value(row);
                                        func.init_state(arg_place);
                                        func.deserialize(arg_place, &mut data)?;
                                    }
                                    groups.insert(group_key, place.addr());
                                }
                            }
                            Some(place) => {
                                let place: StateAddr = (*place).into();

                                for (idx, func) in funcs.iter().enumerate() {
                                    let arg_place = place.next(offsets_aggregate_states[idx]);

                                    let mut data = states_binary_arrays[idx].value(row);
                                    let temp = arena.alloc_layout(funcs[idx].state_layout());
                                    let temp_addr = temp.into();

                                    funcs[idx].init_state(temp_addr);
                                    func.deserialize(temp_addr, &mut data)?;
                                    func.merge(arg_place, temp_addr)?;
                                }
                            }
                        };
                    }
                }
                let delta = start.elapsed();
                tracing::debug!("Group by final cost: {:?}", delta);

                // Collect the merge states.
                let groups = groups_locker.read();

                let mut aggr_values: Vec<Box<dyn MutableArray>> = {
                    let mut values = vec![];
                    for func in &funcs {
                        let array: Box<dyn MutableArray> = match func.return_type()? {
                            DataType::Int8 => Ok(Box::new(MutablePrimitiveArray::<i8>::new())
                                as Box<dyn MutableArray>),
                            DataType::Int16 => Ok(Box::new(MutablePrimitiveArray::<i16>::new())
                                as Box<dyn MutableArray>),
                            DataType::Int32 => Ok(Box::new(MutablePrimitiveArray::<i32>::new())
                                as Box<dyn MutableArray>),
                            DataType::Int64 => Ok(Box::new(MutablePrimitiveArray::<i64>::new())
                                as Box<dyn MutableArray>),
                            DataType::UInt8 => Ok(Box::new(MutablePrimitiveArray::<u8>::new())
                                as Box<dyn MutableArray>),
                            DataType::UInt16 => Ok(Box::new(MutablePrimitiveArray::<u16>::new())
                                as Box<dyn MutableArray>),
                            DataType::UInt32 => Ok(Box::new(MutablePrimitiveArray::<u32>::new())
                                as Box<dyn MutableArray>),
                            DataType::UInt64 => Ok(Box::new(MutablePrimitiveArray::<u64>::new())
                                as Box<dyn MutableArray>),
                            DataType::Float32 => Ok(Box::new(MutablePrimitiveArray::<f32>::new())
                                as Box<dyn MutableArray>),
                            DataType::Float64 => Ok(Box::new(MutablePrimitiveArray::<f64>::new())
                                as Box<dyn MutableArray>),
                            DataType::Boolean => {
                                Ok(Box::new(MutableBooleanArray::new()) as Box<dyn MutableArray>)
                            }
                            DataType::String => {
                                Ok(Box::new(MutableBinaryArray::<i64>::new())
                                    as Box<dyn MutableArray>)
                            }
                            DataType::Date16 => Ok(Box::new(MutablePrimitiveArray::<u16>::new())
                                as Box<dyn MutableArray>),
                            DataType::Date32 => Ok(Box::new(MutablePrimitiveArray::<u32>::new())
                                as Box<dyn MutableArray>),
                            DataType::DateTime32(_) => {
                                Ok(Box::new(MutablePrimitiveArray::<u32>::new())
                                    as Box<dyn MutableArray>)
                            }
                            other => Err(ErrorCode::BadDataValueType(format!(
                                "Unexpected type:{} for DataValue List",
                                other
                            ))),
                        }?;
                        values.push(array)
                    }
                    values
                };
                let mut keys = Vec::with_capacity(groups.len());
                for (key, place) in groups.iter() {
                    keys.push(key.clone());

                    let place: StateAddr = (*place).into();
                    for (idx, func) in funcs.iter().enumerate() {
                        let arg_place = place.next(offsets_aggregate_states[idx]);
                        let array: &mut dyn MutableArray = aggr_values[idx].borrow_mut();
                        func.merge_result(arg_place, array)?;
                    }
                }

                // Build final state block.
                let mut columns: Vec<Series> = Vec::with_capacity(aggr_funcs_len + group_expr_len);
                for mut array in aggr_values {
                    match array.data_type().to_physical_type() {
                        PhysicalType::Boolean => {
                            let array = DFBooleanArray::from_arrow_array(array.as_arc().as_ref());
                            columns.push(array.into_series());
                        }
                        PhysicalType::Primitive(primitive) => {
                            with_match_primitive_type!(primitive, |$T| {
                                let array = DFPrimitiveArray::<$T>::from_arrow_array(
                                    array.as_arc().as_ref(),
                                );
                                columns.push(array.into_series());
                            })
                        }
                        PhysicalType::Binary => {
                            let array = DFStringArray::from_arrow_array(array.as_arc().as_ref());
                            columns.push(array.into_series());
                            tracing::error!("try into string array");
                        }
                        _ => {
                            tracing::error!("should not be here");
                        }
                    };
                }

                {
                    let group_columns = $hash_method.de_group_columns(keys, &group_fields)?;
                    columns.extend_from_slice(&group_columns);
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
                        apply! { hash_method,  &DFStringArray, string, RwLock<HashMap<Vec<u8>, usize, ahash::RandomState>>}
                    }
                    HashMethodKind::KeysU8(hash_method) => {
                        apply! { hash_method , &DFUInt8Array, u8, RwLock<HashMap<u8, usize, ahash::RandomState>> }
                    }
                    HashMethodKind::KeysU16(hash_method) => {
                        apply! { hash_method , &DFUInt16Array, u16, RwLock<HashMap<u16, usize, ahash::RandomState>> }
                    }
                    HashMethodKind::KeysU32(hash_method) => {
                        apply! { hash_method , &DFUInt32Array, u32, RwLock<HashMap<u32, usize, ahash::RandomState>> }
                    }
                    HashMethodKind::KeysU64(hash_method) => {
                        apply! { hash_method , &DFUInt64Array, u64, RwLock<HashMap<u64, usize, ahash::RandomState>> }
                    }
                }
            }};
        }

        match_hash_method_and_apply! {method, apply}
    }
}
