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

use std::cmp::Ordering;
use std::marker::PhantomData;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::SELECTIVITY_THRESHOLD;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Decimal;
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
use databend_storages_common_table_meta::meta::ColumnStatistics;
use enum_dispatch::enum_dispatch;

use crate::statistics::Trim;

pub type CommonBuilder<T> = GenericColumnStatisticsBuilder<T, CommonAdapter>;
pub type DecimalBuilder<T> = GenericColumnStatisticsBuilder<T, DecimalAdapter>;

#[enum_dispatch(ColumnStatsOps)]
pub enum ColumnStatisticsBuilder {
    Int8(CommonBuilder<Int8Type>),
    Int16(CommonBuilder<Int16Type>),
    Int32(CommonBuilder<Int32Type>),
    Int64(CommonBuilder<Int64Type>),
    UInt8(CommonBuilder<UInt8Type>),
    UInt16(CommonBuilder<UInt16Type>),
    UInt32(CommonBuilder<UInt32Type>),
    UInt64(CommonBuilder<UInt64Type>),
    Float32(CommonBuilder<Float32Type>),
    Float64(CommonBuilder<Float64Type>),
    String(CommonBuilder<StringType>),
    Date(CommonBuilder<DateType>),
    Timestamp(CommonBuilder<TimestampType>),
    TimestampTz(CommonBuilder<TimestampTzType>),
    Decimal64(DecimalBuilder<Decimal64Type>),
    Decimal128(DecimalBuilder<Decimal128Type>),
    Decimal256(DecimalBuilder<Decimal256Type>),
}

#[enum_dispatch]
pub trait ColumnStatsOps {
    fn update_column(&mut self, column: &Column);
    fn update_scalar(&mut self, scalar: &ScalarRef, num_rows: usize, data_type: &DataType);
    fn finalize(self) -> Result<ColumnStatistics>;
}

pub trait ColumnStatsPeek {
    fn peek(&self) -> Result<Option<ColumnStatistics>>;
}

impl<T, A> ColumnStatsOps for GenericColumnStatisticsBuilder<T, A>
where
    T: ValueType + Send + Sync,
    T::Scalar: Send + Sync,
    A: ColumnStatisticsAdapter<T> + 'static,
    for<'a, 'b> T::ScalarRef<'a>: PartialOrd<T::ScalarRef<'b>>,
{
    fn update_column(&mut self, column: &Column) {
        GenericColumnStatisticsBuilder::update_column(self, column);
    }

    fn update_scalar(&mut self, scalar: &ScalarRef, num_rows: usize, data_type: &DataType) {
        GenericColumnStatisticsBuilder::update_scalar(self, scalar, num_rows, data_type);
    }

    fn finalize(self) -> Result<ColumnStatistics> {
        GenericColumnStatisticsBuilder::finalize(self)
    }
}

pub fn create_column_stats_builder(data_type: &DataType) -> ColumnStatisticsBuilder {
    let inner_type = data_type.remove_nullable();
    macro_rules! match_number_type_create {
        ($inner_type:expr) => {{
            with_number_type!(|NUM_TYPE| match $inner_type {
                NumberDataType::NUM_TYPE => {
                    paste::paste! {
                        ColumnStatisticsBuilder::NUM_TYPE(CommonBuilder::<[<NUM_TYPE Type>]>::create(inner_type))
                    }
                }
            })
        }};
    }

    match inner_type {
        DataType::Number(num_type) => {
            match_number_type_create!(num_type)
        }
        DataType::String => {
            ColumnStatisticsBuilder::String(CommonBuilder::<StringType>::create(inner_type))
        }
        DataType::Date => {
            ColumnStatisticsBuilder::Date(CommonBuilder::<DateType>::create(inner_type))
        }
        DataType::Timestamp => {
            ColumnStatisticsBuilder::Timestamp(CommonBuilder::<TimestampType>::create(inner_type))
        }
        DataType::TimestampTz => ColumnStatisticsBuilder::TimestampTz(CommonBuilder::<
            TimestampTzType,
        >::create(inner_type)),
        DataType::Decimal(size) => {
            if size.can_carried_by_64() {
                ColumnStatisticsBuilder::Decimal64(DecimalBuilder::<Decimal64Type>::create(
                    inner_type,
                ))
            } else if size.can_carried_by_128() {
                ColumnStatisticsBuilder::Decimal128(DecimalBuilder::<Decimal128Type>::create(
                    inner_type,
                ))
            } else {
                ColumnStatisticsBuilder::Decimal256(DecimalBuilder::<Decimal256Type>::create(
                    inner_type,
                ))
            }
        }
        _ => unreachable!("Unsupported data type: {:?}", data_type),
    }
}

pub trait ColumnStatisticsAdapter<T: ValueType>: Send + Sync {
    type Value: Clone + Send + Sync;

    fn scalar_to_value(val: T::ScalarRef<'_>) -> Self::Value;

    fn value_to_scalar(val: Self::Value) -> T::Scalar;

    fn update_value(value: &mut Self::Value, scalar: T::ScalarRef<'_>, ordering: Ordering);
}

pub struct CommonAdapter;

impl<T> ColumnStatisticsAdapter<T> for CommonAdapter
where
    T: ValueType,
    T::Scalar: Send + Sync,
    for<'a, 'b> T::ScalarRef<'a>: PartialOrd<T::ScalarRef<'b>>,
{
    type Value = T::Scalar;

    fn scalar_to_value(val: T::ScalarRef<'_>) -> Self::Value {
        T::to_owned_scalar(val)
    }

    fn value_to_scalar(val: Self::Value) -> T::Scalar {
        val
    }

    fn update_value(value: &mut Self::Value, scalar: T::ScalarRef<'_>, ordering: Ordering) {
        if scalar.partial_cmp(&T::to_scalar_ref(value)) == Some(ordering) {
            *value = T::to_owned_scalar(scalar);
        }
    }
}

pub struct DecimalAdapter;

impl<T> ColumnStatisticsAdapter<T> for DecimalAdapter
where
    T: ValueType,
    T::Scalar: Decimal + Send + Sync,
    for<'a, 'b> T::ScalarRef<'a>: PartialOrd<T::ScalarRef<'b>>,
{
    type Value = <T::Scalar as Decimal>::U64Array;

    fn scalar_to_value(val: T::ScalarRef<'_>) -> Self::Value {
        T::Scalar::to_u64_array(T::to_owned_scalar(val))
    }

    fn value_to_scalar(val: Self::Value) -> T::Scalar {
        T::Scalar::from_u64_array(val)
    }

    fn update_value(value: &mut Self::Value, scalar: T::ScalarRef<'_>, ordering: Ordering) {
        let val = T::Scalar::from_u64_array(*value);
        if scalar.partial_cmp(&T::to_scalar_ref(&val)) == Some(ordering) {
            *value = T::Scalar::to_u64_array(T::to_owned_scalar(scalar));
        }
    }
}

pub struct GenericColumnStatisticsBuilder<T, A>
where
    T: ValueType,
    A: ColumnStatisticsAdapter<T>,
{
    min: Option<A::Value>,
    max: Option<A::Value>,
    null_count: usize,
    in_memory_size: usize,
    data_type: DataType,

    _phantom: PhantomData<(T, A)>,
}

impl<T, A> GenericColumnStatisticsBuilder<T, A>
where
    T: ValueType + Send + Sync,
    T::Scalar: Send + Sync,
    A: ColumnStatisticsAdapter<T> + 'static,
    for<'a, 'b> T::ScalarRef<'a>: PartialOrd<T::ScalarRef<'b>>,
{
    fn create(data_type: DataType) -> Self {
        Self {
            min: None,
            max: None,
            null_count: 0,
            in_memory_size: 0,
            data_type,
            _phantom: PhantomData,
        }
    }

    fn add_batch<'a, I>(&mut self, mut iter: I)
    where I: Iterator<Item = T::ScalarRef<'a>> {
        let first = iter.next().unwrap();
        let mut min = first.clone();
        let mut max = first;
        for v in iter {
            if matches!(min.partial_cmp(&v), Some(Ordering::Greater)) {
                min = v;
                continue;
            }

            if matches!(max.partial_cmp(&v), Some(Ordering::Less)) {
                max = v;
            }
        }

        self.add(min, max);
    }

    fn add(&mut self, min: T::ScalarRef<'_>, max: T::ScalarRef<'_>) {
        if let Some(val) = self.min.as_mut() {
            A::update_value(val, min, Ordering::Less);
        } else {
            self.min = Some(A::scalar_to_value(min));
        }

        if let Some(val) = self.max.as_mut() {
            A::update_value(val, max, Ordering::Greater);
        } else {
            self.max = Some(A::scalar_to_value(max));
        }
    }

    fn update_column(&mut self, column: &Column) {
        self.in_memory_size += column.memory_size(false);
        if column.len() == 0 {
            return;
        }
        let (column, validity) = match column {
            Column::Nullable(box inner) => {
                let validity = if inner.validity.null_count() == 0 {
                    None
                } else {
                    Some(&inner.validity)
                };
                (&inner.column, validity)
            }
            Column::Null { len } => {
                self.null_count += *len;
                return;
            }
            col => (col, None),
        };
        self.null_count += validity.map_or(0, |v| v.null_count());

        let column = T::try_downcast_column(column).unwrap();
        if let Some(v) = validity {
            if v.true_count() as f64 / v.len() as f64 >= SELECTIVITY_THRESHOLD {
                let column_iter = T::iter_column(&column);
                let value_iter = column_iter
                    .zip(v.iter())
                    .filter(|(_, v)| *v)
                    .map(|(v, _)| v);
                self.add_batch(value_iter);
            } else {
                for idx in TrueIdxIter::new(v.len(), Some(v)) {
                    let v = unsafe { T::index_column_unchecked(&column, idx) };
                    self.add(v.clone(), v);
                }
            }
        } else {
            let column_iter = T::iter_column(&column);
            self.add_batch(column_iter);
        }
    }

    fn update_scalar(&mut self, scalar: &ScalarRef, num_rows: usize, data_type: &DataType) {
        // when we read it back from parquet, it is a Column instead of Scalar
        self.in_memory_size += scalar.estimated_scalar_repeat_size(num_rows, data_type);
        if scalar.is_null() {
            self.null_count += num_rows;
            return;
        }

        let val = T::try_downcast_scalar(scalar).unwrap();
        self.add(val.clone(), val);
    }

    fn finalize(self) -> Result<ColumnStatistics> {
        Self::build_statistics(
            self.min,
            self.max,
            self.null_count,
            self.in_memory_size,
            &self.data_type,
        )
    }

    fn peek(&self) -> Result<Option<ColumnStatistics>> {
        if self.min.is_none() && self.max.is_none() && self.null_count == 0 {
            return Ok(None);
        }
        let stats = Self::build_statistics(
            self.min.clone(),
            self.max.clone(),
            self.null_count,
            self.in_memory_size,
            &self.data_type,
        )?;
        Ok(Some(stats))
    }

    fn build_statistics(
        min: Option<A::Value>,
        max: Option<A::Value>,
        null_count: usize,
        in_memory_size: usize,
        data_type: &DataType,
    ) -> Result<ColumnStatistics> {
        let min = if let Some(v) = min {
            let v = A::value_to_scalar(v);
            // Safe unwrap: `Trim for Scalar` always returns Some (strings truncate, numerics passthrough).
            T::upcast_scalar_with_type(v, data_type).trim_min().unwrap()
        } else {
            Scalar::Null
        };
        let max = if let Some(v) = max {
            let v = A::value_to_scalar(v);
            if let Some(v) = T::upcast_scalar_with_type(v, data_type).trim_max() {
                v
            } else {
                return Err(ErrorCode::Internal(
                    "Unable to trim string: first 16 chars are all replacement_point".to_string(),
                ));
            }
        } else {
            Scalar::Null
        };

        Ok(ColumnStatistics::new(
            min,
            max,
            null_count as u64,
            in_memory_size as u64,
            None,
        ))
    }
}

impl ColumnStatsPeek for ColumnStatisticsBuilder {
    fn peek(&self) -> Result<Option<ColumnStatistics>> {
        match self {
            ColumnStatisticsBuilder::Int8(inner) => inner.peek(),
            ColumnStatisticsBuilder::Int16(inner) => inner.peek(),
            ColumnStatisticsBuilder::Int32(inner) => inner.peek(),
            ColumnStatisticsBuilder::Int64(inner) => inner.peek(),
            ColumnStatisticsBuilder::UInt8(inner) => inner.peek(),
            ColumnStatisticsBuilder::UInt16(inner) => inner.peek(),
            ColumnStatisticsBuilder::UInt32(inner) => inner.peek(),
            ColumnStatisticsBuilder::UInt64(inner) => inner.peek(),
            ColumnStatisticsBuilder::Float32(inner) => inner.peek(),
            ColumnStatisticsBuilder::Float64(inner) => inner.peek(),
            ColumnStatisticsBuilder::String(inner) => inner.peek(),
            ColumnStatisticsBuilder::Date(inner) => inner.peek(),
            ColumnStatisticsBuilder::Timestamp(inner) => inner.peek(),
            ColumnStatisticsBuilder::TimestampTz(inner) => inner.peek(),
            ColumnStatisticsBuilder::Decimal64(inner) => inner.peek(),
            ColumnStatisticsBuilder::Decimal128(inner) => inner.peek(),
            ColumnStatisticsBuilder::Decimal256(inner) => inner.peek(),
        }
    }
}
