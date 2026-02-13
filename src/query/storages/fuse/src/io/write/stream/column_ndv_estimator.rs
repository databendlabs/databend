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

use std::hash::Hash;
use std::marker::PhantomData;

use databend_common_expression::Column;
use databend_common_expression::SELECTIVITY_THRESHOLD;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::Decimal256Type;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int8Type;
use databend_common_expression::types::Int16Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::types::UInt16Type;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::boolean::TrueIdxIter;
use databend_common_expression::types::timestamp_tz::TimestampTzType;
use databend_common_expression::with_number_type;
use databend_common_storage::MetaHLL;
use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait ColumnNDVEstimatorOps: Send + Sync {
    fn update_column(&mut self, column: &Column);
    fn update_scalar(&mut self, scalar: &ScalarRef);

    fn count(&self) -> usize;
    fn hll(self) -> MetaHLL;
}

#[enum_dispatch(ColumnNDVEstimatorOps)]
pub enum ColumnNDVEstimator {
    Int8(ColumnNDVEstimatorImpl<Int8Type>),
    Int16(ColumnNDVEstimatorImpl<Int16Type>),
    Int32(ColumnNDVEstimatorImpl<Int32Type>),
    Int64(ColumnNDVEstimatorImpl<Int64Type>),
    UInt8(ColumnNDVEstimatorImpl<UInt8Type>),
    UInt16(ColumnNDVEstimatorImpl<UInt16Type>),
    UInt32(ColumnNDVEstimatorImpl<UInt32Type>),
    UInt64(ColumnNDVEstimatorImpl<UInt64Type>),
    Float32(ColumnNDVEstimatorImpl<Float32Type>),
    Float64(ColumnNDVEstimatorImpl<Float64Type>),
    String(ColumnNDVEstimatorImpl<StringType>),
    Date(ColumnNDVEstimatorImpl<DateType>),
    Timestamp(ColumnNDVEstimatorImpl<TimestampType>),
    TimestampTz(ColumnNDVEstimatorImpl<TimestampTzType>),
    Decimal64(ColumnNDVEstimatorImpl<Decimal64Type>),
    Decimal128(ColumnNDVEstimatorImpl<Decimal128Type>),
    Decimal256(ColumnNDVEstimatorImpl<Decimal256Type>),
}

pub fn create_column_ndv_estimator(data_type: &DataType) -> ColumnNDVEstimator {
    macro_rules! match_number_type_create {
        ($inner_type:expr) => {{
            with_number_type!(|NUM_TYPE| match $inner_type {
                NumberDataType::NUM_TYPE => {
                    paste::paste! {
                        ColumnNDVEstimator::NUM_TYPE(ColumnNDVEstimatorImpl::<[<NUM_TYPE Type>]>::new())
                    }
                }
            })
        }};
    }

    let inner_type = data_type.remove_nullable();
    match inner_type {
        DataType::Number(num_type) => {
            match_number_type_create!(num_type)
        }
        DataType::String => ColumnNDVEstimator::String(ColumnNDVEstimatorImpl::<StringType>::new()),
        DataType::Date => ColumnNDVEstimator::Date(ColumnNDVEstimatorImpl::<DateType>::new()),
        DataType::Timestamp => {
            ColumnNDVEstimator::Timestamp(ColumnNDVEstimatorImpl::<TimestampType>::new())
        }
        DataType::TimestampTz => {
            ColumnNDVEstimator::TimestampTz(ColumnNDVEstimatorImpl::<TimestampTzType>::new())
        }
        DataType::Decimal(size) => {
            if size.can_carried_by_64() {
                ColumnNDVEstimator::Decimal64(ColumnNDVEstimatorImpl::<Decimal64Type>::new())
            } else if size.can_carried_by_128() {
                ColumnNDVEstimator::Decimal128(ColumnNDVEstimatorImpl::<Decimal128Type>::new())
            } else {
                ColumnNDVEstimator::Decimal256(ColumnNDVEstimatorImpl::<Decimal256Type>::new())
            }
        }
        _ => unreachable!("Unsupported data type: {:?}", data_type),
    }
}

pub struct ColumnNDVEstimatorImpl<T>
where
    T: ValueType + Send + Sync,
    for<'a> T::ScalarRef<'a>: Hash,
{
    hll: MetaHLL,
    _phantom: PhantomData<T>,
}

impl<T> ColumnNDVEstimatorImpl<T>
where
    T: ValueType + Send + Sync,
    for<'a> T::ScalarRef<'a>: Hash,
{
    pub fn new() -> Self {
        Self {
            hll: MetaHLL::new(),
            _phantom: Default::default(),
        }
    }
}

impl<T> ColumnNDVEstimatorOps for ColumnNDVEstimatorImpl<T>
where
    T: ValueType + Send + Sync,
    for<'a> T::ScalarRef<'a>: Hash,
{
    fn update_column(&mut self, column: &Column) {
        let (column, validity) = match column {
            Column::Nullable(box inner) => {
                let validity = if inner.validity.null_count() == 0 {
                    None
                } else {
                    Some(&inner.validity)
                };
                (&inner.column, validity)
            }
            Column::Null { .. } => return,
            column => (column, None),
        };

        let column = T::try_downcast_column(column).unwrap();
        if let Some(v) = validity {
            if v.true_count() as f64 / v.len() as f64 >= SELECTIVITY_THRESHOLD {
                for (data, valid) in T::iter_column(&column).zip(v.iter()) {
                    if valid {
                        self.hll.add_object(&data);
                    }
                }
            } else {
                TrueIdxIter::new(v.len(), Some(v)).for_each(|idx| {
                    let val = unsafe { T::index_column_unchecked(&column, idx) };
                    self.hll.add_object(&val);
                })
            }
        } else {
            for value in T::iter_column(&column) {
                self.hll.add_object(&value);
            }
        }
    }

    fn update_scalar(&mut self, scalar: &ScalarRef) {
        if matches!(scalar, ScalarRef::Null) {
            return;
        }

        let val = T::try_downcast_scalar(scalar).unwrap();
        self.hll.add_object(&val);
    }

    fn count(&self) -> usize {
        self.hll.count()
    }

    fn hll(self) -> MetaHLL {
        self.hll
    }
}
