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
use std::sync::Arc;
use std::time::Instant;

use common_arrow::arrow::array::MutableArray;
use common_arrow::arrow::array::MutableBinaryArray;
use common_arrow::arrow::array::MutableBooleanArray;
use common_arrow::arrow::array::MutablePrimitiveArray;
use common_arrow::arrow::datatypes::PhysicalType;
use common_datablocks::DataBlock;
use common_datavalues::prelude::DFBooleanArray;
use common_datavalues::prelude::DFPrimitiveArray;
use common_datavalues::prelude::DFStringArray;
use common_datavalues::prelude::IntoSeries;
use common_datavalues::series::Series;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::get_layout_offsets;
use common_functions::aggregates::AggregateFunctionRef;
use common_functions::aggregates::StateAddr;
use common_planners::Expression;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::stream::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

pub struct AggregatorFinalTransform {
    funcs: Vec<AggregateFunctionRef>,
    schema: DataSchemaRef,
    input: Arc<dyn Processor>,
}

impl AggregatorFinalTransform {
    pub fn try_create(
        schema: DataSchemaRef,
        schema_before_group_by: DataSchemaRef,
        exprs: Vec<Expression>,
    ) -> Result<Self> {
        let funcs = exprs
            .iter()
            .map(|expr| expr.to_aggregate_function(&schema_before_group_by))
            .collect::<Result<Vec<_>>>()?;
        Ok(AggregatorFinalTransform {
            funcs,
            schema,
            input: Arc::new(EmptyProcessor::create()),
        })
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
impl Processor for AggregatorFinalTransform {
    fn name(&self) -> &str {
        "AggregatorFinalTransform"
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

        let funcs = self.funcs.clone();
        let mut stream = self.input.execute().await?;

        let start = Instant::now();
        let arena = bumpalo::Bump::new();

        let (layout, offsets_aggregate_states) = unsafe { get_layout_offsets(&funcs) };
        let places: Vec<usize> = {
            let place: StateAddr = arena.alloc_layout(layout).into();
            funcs
                .iter()
                .enumerate()
                .map(|(idx, func)| {
                    let arg_place = place.next(offsets_aggregate_states[idx]);
                    func.init_state(arg_place);
                    arg_place.addr()
                })
                .collect()
        };

        while let Some(block) = stream.next().await {
            let block = block?;
            for (idx, func) in funcs.iter().enumerate() {
                let place = places[idx].into();

                let binary_array = block.column(idx).to_array()?;
                let binary_array: &DFStringArray = binary_array.string()?;
                let array = binary_array.inner();

                let mut data = array.value(0);
                let s = funcs[idx].state_layout();
                let temp = arena.alloc_layout(s);
                let temp_addr = temp.into();
                funcs[idx].init_state(temp_addr);

                func.deserialize(temp_addr, &mut data)?;
                func.merge(place, temp_addr)?;
            }
        }
        let delta = start.elapsed();
        tracing::debug!("Aggregator final cost: {:?}", delta);

        let funcs_len = funcs.len();
        let mut aggr_values: Vec<Box<dyn MutableArray>> =
            {
                let mut values = vec![];
                for i in 0..funcs_len {
                    let array: Box<dyn MutableArray> =
                        match funcs[i].return_type()? {
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
        for (idx, func) in funcs.iter().enumerate() {
            let place = places[idx].into();
            let array: &mut dyn MutableArray = aggr_values[idx].borrow_mut();
            let _ = func.merge_result(place, array)?;
        }

        let mut columns: Vec<Series> = Vec::with_capacity(funcs_len);
        tracing::error!("group by final");
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
                }
                _ => {
                    tracing::debug!("should not be here");
                }
            };
        }
        let mut blocks = vec![];
        if !columns.is_empty() {
            blocks.push(DataBlock::create_by_array(self.schema.clone(), columns));
        }

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            blocks,
        )))
    }
}
