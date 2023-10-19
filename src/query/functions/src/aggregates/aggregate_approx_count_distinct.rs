// Copyright 2021 Datafuse Labs
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

use std::alloc::Layout;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::types::DateType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::types::ValueType;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use streaming_algorithms::HyperLogLog;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::deserialize_state;
use super::serialize_state;
use super::AggregateFunctionRef;
use super::StateAddr;
use crate::aggregates::aggregator_common::assert_unary_arguments;

/// Use Hyperloglog to estimate distinct of values
pub struct AggregateApproxCountDistinctState<S> {
    hll: HyperLogLog<S>,
}

/// S: ScalarType
#[derive(Clone)]
pub struct AggregateApproxCountDistinctFunction<T> {
    display_name: String,
    _t: PhantomData<T>,
}

impl<T: ValueType + Send + Sync> AggregateApproxCountDistinctFunction<T>
where for<'a> T::ScalarRef<'a>: Hash
{
    pub fn try_create(
        display_name: &str,
        _arguments: Vec<DataType>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_string(),
            _t: PhantomData,
        }))
    }
}

impl<T: ValueType + Send + Sync> AggregateFunction for AggregateApproxCountDistinctFunction<T>
where for<'a> T::ScalarRef<'a>: Hash
{
    fn name(&self) -> &str {
        "AggregateApproxCountDistinctFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Number(NumberDataType::UInt64))
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateApproxCountDistinctState {
            hll: HyperLogLog::<T::ScalarRef<'_>>::new(0.04),
        });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateApproxCountDistinctState<T::ScalarRef<'_>>>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<AggregateApproxCountDistinctState<T::ScalarRef<'_>>>();
        let column = T::try_downcast_column(&columns[0]).unwrap();

        if let Some(validity) = validity {
            T::iter_column(&column)
                .zip(validity.iter())
                .for_each(|(t, b)| {
                    if b {
                        state.hll.push(&t);
                    }
                });
        } else {
            T::iter_column(&column).for_each(|t| {
                state.hll.push(&t);
            });
        }
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let state = place.get::<AggregateApproxCountDistinctState<T::ScalarRef<'_>>>();
        let column = T::try_downcast_column(&columns[0]).unwrap();
        state.hll.push(&T::index_column(&column, row).unwrap());
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<AggregateApproxCountDistinctState<T::ScalarRef<'_>>>();
        serialize_state(writer, &state.hll)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateApproxCountDistinctState<T::ScalarRef<'_>>>();
        let hll: HyperLogLog<T::ScalarRef<'_>> = deserialize_state(reader)?;
        state.hll.union(&hll);

        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateApproxCountDistinctState<T::ScalarRef<'_>>>();
        let other = rhs.get::<AggregateApproxCountDistinctState<T::ScalarRef<'_>>>();
        state.hll.union(&other.hll);
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<AggregateApproxCountDistinctState<T::ScalarRef<'_>>>();
        let builder = NumberType::<u64>::try_downcast_builder(builder).unwrap();
        builder.push(state.hll.len() as u64);
        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<AggregateApproxCountDistinctState<T::ScalarRef<'_>>>();
        std::ptr::drop_in_place(state);
    }
}

pub fn try_create_aggregate_approx_count_distinct_function(
    display_name: &str,
    _params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;

    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            AggregateApproxCountDistinctFunction::<NumberType<NUM_TYPE>>::try_create(
                display_name,
                arguments,
            )
        }
        DataType::String =>
            AggregateApproxCountDistinctFunction::<StringType>::try_create(display_name, arguments,),
        DataType::Date =>
            AggregateApproxCountDistinctFunction::<DateType>::try_create(display_name, arguments,),
        DataType::Timestamp => AggregateApproxCountDistinctFunction::<TimestampType>::try_create(
            display_name,
            arguments,
        ),
        _ => AggregateApproxCountDistinctFunction::<AnyType>::try_create(display_name, arguments,),
    })
}

pub fn aggregate_approx_count_distinct_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        returns_default_when_only_null: true,
        ..Default::default()
    };

    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_approx_count_distinct_function),
        features,
    )
}

impl<T> fmt::Display for AggregateApproxCountDistinctFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
