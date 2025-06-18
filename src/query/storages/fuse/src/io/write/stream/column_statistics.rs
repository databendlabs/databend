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
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::boolean::TrueIdxIter;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::Decimal256Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_expression::SELECTIVITY_THRESHOLD;
use databend_storages_common_table_meta::meta::ColumnDistinctHLL;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

use crate::statistics::traverse_values_dfs;
use crate::statistics::Trim;

pub struct ColumnStatisticsState {
    col_stats: HashMap<ColumnId, Box<dyn ColumnMinMaxState>>,
    distinct_columns: HashMap<ColumnId, Box<dyn ColumnNDVEstimator>>,
}

impl ColumnStatisticsState {
    pub fn new(
        stats_columns: &[(ColumnId, DataType)],
        distinct_columns: &[(ColumnId, DataType)],
    ) -> Self {
        let col_stats = stats_columns
            .iter()
            .map(|(col_id, data_type)| (*col_id, create_column_minmax_state(data_type)))
            .collect();

        let distinct_columns = distinct_columns
            .iter()
            .map(|(col_id, data_type)| (*col_id, create_estimator(data_type)))
            .collect();

        Self {
            col_stats,
            distinct_columns,
        }
    }

    pub fn add_block(&mut self, schema: &TableSchemaRef, data_block: &DataBlock) -> Result<()> {
        let rows = data_block.num_rows();
        let leaves = traverse_values_dfs(data_block.columns(), schema.fields())?;
        for (column_id, col, data_type) in leaves {
            match col {
                Value::Scalar(s) => {
                    self.col_stats.get_mut(&column_id).unwrap().update_scalar(
                        &s.as_ref(),
                        rows,
                        &data_type,
                    );
                    if let Some(estimator) = self.distinct_columns.get_mut(&column_id) {
                        estimator.update_scalar(&s.as_ref());
                    }
                }
                Value::Column(col) => {
                    self.col_stats
                        .get_mut(&column_id)
                        .unwrap()
                        .update_column(&col);
                    // use distinct count calculated by the xor hash function to avoid repetitive operation.
                    if let Some(estimator) = self.distinct_columns.get_mut(&column_id) {
                        estimator.update_column(&col);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn finalize(
        self,
        column_distinct_count: HashMap<ColumnId, usize>,
    ) -> Result<StatisticsOfColumns> {
        let mut statistics = StatisticsOfColumns::with_capacity(self.col_stats.len());
        for (id, stats) in self.col_stats {
            let mut col_stats = stats.finalize()?;
            if let Some(count) = column_distinct_count.get(&id) {
                col_stats.distinct_of_values = Some(*count as u64);
            } else if let Some(estimator) = self.distinct_columns.get(&id) {
                col_stats.distinct_of_values = Some(estimator.finalize());
            }
            statistics.insert(id, col_stats);
        }
        Ok(statistics)
    }
}

pub trait ColumnNDVEstimator: Send + Sync {
    fn update_column(&mut self, column: &Column);
    fn update_scalar(&mut self, scalar: &ScalarRef);
    fn finalize(&self) -> u64;
}

pub fn create_estimator(data_type: &DataType) -> Box<dyn ColumnNDVEstimator> {
    let inner_type = data_type.remove_nullable();
    with_number_mapped_type!(|NUM_TYPE| match inner_type {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            ColumnNDVEstimatorImpl::<NumberType<NUM_TYPE>>::create()
        }
        DataType::String => {
            ColumnNDVEstimatorImpl::<StringType>::create()
        }
        DataType::Date => {
            ColumnNDVEstimatorImpl::<DateType>::create()
        }
        DataType::Timestamp => {
            ColumnNDVEstimatorImpl::<TimestampType>::create()
        }
        DataType::Decimal(s) if s.can_carried_by_128() => {
            ColumnNDVEstimatorImpl::<Decimal128Type>::create()
        }
        DataType::Decimal(_) => {
            ColumnNDVEstimatorImpl::<Decimal256Type>::create()
        }
        _ => unreachable!("Unsupported data type: {:?}", data_type),
    })
}

pub struct ColumnNDVEstimatorImpl<T>
where
    T: ValueType + Send + Sync,
    for<'a> T::ScalarRef<'a>: Hash,
{
    hll: ColumnDistinctHLL,
    _phantom: PhantomData<T>,
}

impl<T> ColumnNDVEstimatorImpl<T>
where
    T: ValueType + Send + Sync,
    for<'a> T::ScalarRef<'a>: Hash,
{
    pub fn create() -> Box<dyn ColumnNDVEstimator> {
        Box::new(Self {
            hll: ColumnDistinctHLL::new(),
            _phantom: Default::default(),
        })
    }
}

impl<T> ColumnNDVEstimator for ColumnNDVEstimatorImpl<T>
where
    T: ValueType + Send + Sync,
    for<'a> T::ScalarRef<'a>: Hash,
{
    fn update_column(&mut self, column: &Column) {
        let (column, validity) = if let Column::Nullable(box inner) = column {
            let validity = if inner.validity.null_count() == 0 {
                None
            } else {
                Some(&inner.validity)
            };
            (&inner.column, validity)
        } else {
            (column, None)
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

    fn finalize(&self) -> u64 {
        self.hll.count() as u64
    }
}

pub trait ColumnMinMaxState: Send + Sync {
    fn update_column(&mut self, column: &Column);

    fn update_scalar(&mut self, scalar: &ScalarRef, num_rows: usize, data_type: &DataType);

    fn finalize(self: Box<Self>) -> Result<ColumnStatistics>;
}

pub trait MinMaxAdapter<T: ValueType>: Send + Sync {
    type Value: Clone + Send + Sync;

    fn scalar_to_value(val: T::ScalarRef<'_>) -> Self::Value;

    fn value_to_scalar(val: Self::Value) -> T::Scalar;

    fn update_value(value: &mut Self::Value, scalar: T::ScalarRef<'_>, ordering: Ordering);
}

pub struct CommonAdapter;

impl<T> MinMaxAdapter<T> for CommonAdapter
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

impl<T> MinMaxAdapter<T> for DecimalAdapter
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

pub fn create_column_minmax_state(data_type: &DataType) -> Box<dyn ColumnMinMaxState> {
    let inner_type = data_type.remove_nullable();
    with_number_mapped_type!(|NUM_TYPE| match inner_type {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            GenericColumnMinMaxState::<NumberType<NUM_TYPE>, CommonAdapter>::create(inner_type)
        }
        DataType::String => {
            GenericColumnMinMaxState::<StringType, CommonAdapter>::create(inner_type)
        }
        DataType::Date => {
            GenericColumnMinMaxState::<DateType, CommonAdapter>::create(inner_type)
        }
        DataType::Timestamp => {
            GenericColumnMinMaxState::<TimestampType, CommonAdapter>::create(inner_type)
        }
        DataType::Decimal(s) if s.can_carried_by_128() => {
            GenericColumnMinMaxState::<Decimal128Type, DecimalAdapter>::create(inner_type)
        }
        DataType::Decimal(_) => {
            GenericColumnMinMaxState::<Decimal256Type, DecimalAdapter>::create(inner_type)
        }
        _ => unreachable!("Unsupported data type: {:?}", data_type),
    })
}

pub struct GenericColumnMinMaxState<T, A>
where
    T: ValueType,
    A: MinMaxAdapter<T>,
{
    min: Option<A::Value>,
    max: Option<A::Value>,
    null_count: usize,
    in_memory_size: usize,
    data_type: DataType,

    _phantom: PhantomData<(T, A)>,
}

impl<T, A> GenericColumnMinMaxState<T, A>
where
    T: ValueType + Send + Sync,
    T::Scalar: Send + Sync,
    A: MinMaxAdapter<T> + 'static,
    for<'a, 'b> T::ScalarRef<'a>: PartialOrd<T::ScalarRef<'b>>,
{
    pub fn create(data_type: DataType) -> Box<dyn ColumnMinMaxState> {
        Box::new(Self {
            min: None,
            max: None,
            null_count: 0,
            in_memory_size: 0,
            data_type,
            _phantom: PhantomData,
        })
    }

    fn add_batch<'a, I>(&mut self, mut iter: I)
    where I: Iterator<Item = T::ScalarRef<'a>> {
        let first = iter.next().unwrap();
        let mut min = first.clone();
        let mut max = first;
        for v in iter {
            if matches!(min.partial_cmp(&v), Some(Ordering::Greater)) {
                min = v;
            } else if matches!(max.partial_cmp(&v), Some(Ordering::Less)) {
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
}

impl<T, A> ColumnMinMaxState for GenericColumnMinMaxState<T, A>
where
    T: ValueType + Send + Sync,
    T::Scalar: Send + Sync,
    A: MinMaxAdapter<T> + 'static,
    for<'a, 'b> T::ScalarRef<'a>: PartialOrd<T::ScalarRef<'b>>,
{
    fn update_column(&mut self, column: &Column) {
        self.in_memory_size += column.memory_size();
        let (column, validity) = if let Column::Nullable(box inner) = column {
            let validity = if inner.validity.null_count() == 0 {
                None
            } else {
                Some(&inner.validity)
            };
            (&inner.column, validity)
        } else {
            (column, None)
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

    fn finalize(self: Box<Self>) -> Result<ColumnStatistics> {
        let min = if let Some(v) = self.min {
            let v = A::value_to_scalar(v);
            // safe upwrap.
            T::upcast_scalar_with_type(v, &self.data_type)
                .trim_min()
                .unwrap()
        } else {
            Scalar::Null
        };
        let max = if let Some(v) = self.max {
            let v = A::value_to_scalar(v);
            if let Some(v) = T::upcast_scalar_with_type(v, &self.data_type).trim_max() {
                v
            } else {
                return Err(ErrorCode::Internal("Unable to trim string"));
            }
        } else {
            Scalar::Null
        };

        Ok(ColumnStatistics::new(
            min,
            max,
            self.null_count as u64,
            self.in_memory_size as u64,
            None,
        ))
    }
}
