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

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

use databend_common_exception::Result;
use databend_common_expression::types::boolean::TrueIdxIter;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
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
use databend_common_functions::aggregates::eval_aggr;
use databend_storages_common_table_meta::meta::ColumnDistinctHLL;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

use crate::statistics::reducers::reduce_column_statistics;
use crate::statistics::traverse_values_dfs;
use crate::statistics::Trim;

pub struct ColumnStatisticsState {
    col_stats: HashMap<ColumnId, Vec<ColumnStatistics>>,
    distinct_columns: HashMap<ColumnId, Box<dyn ColumnNDVEstimator>>,
}

impl ColumnStatisticsState {
    pub fn new(stats_columns: &[ColumnId], distinct_columns: &[(ColumnId, DataType)]) -> Self {
        let col_stats = stats_columns
            .iter()
            .map(|&col_id| (col_id, Vec::new()))
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
                    let unset_bits = if s == Scalar::Null { rows } else { 0 };
                    // when we read it back from parquet, it is a Column instead of Scalar
                    let in_memory_size = s.as_ref().estimated_scalar_repeat_size(rows, &data_type);
                    let col_stats = ColumnStatistics::new(
                        s.clone(),
                        s.clone(),
                        unset_bits as u64,
                        in_memory_size as u64,
                        None,
                    );
                    if let Some(estimator) = self.distinct_columns.get_mut(&column_id) {
                        estimator.update_scalar(&s.as_ref());
                    }
                    self.col_stats.get_mut(&column_id).unwrap().push(col_stats);
                }
                Value::Column(col) => {
                    // later, during the evaluation of expressions, name of field does not matter
                    let mut min = Scalar::Null;
                    let mut max = Scalar::Null;

                    let (mins, _) = eval_aggr("min", vec![], &[col.clone()], rows, vec![])?;
                    if mins.len() > 0 {
                        min = if let Some(v) = mins.index(0) {
                            // safe upwrap.
                            v.to_owned().trim_min().unwrap()
                        } else {
                            self.col_stats.remove(&column_id);
                            continue;
                        }
                    }

                    let (maxs, _) = eval_aggr("max", vec![], &[col.clone()], rows, vec![])?;
                    if maxs.len() > 0 {
                        max = if let Some(v) = maxs.index(0) {
                            if let Some(v) = v.to_owned().trim_max() {
                                v
                            } else {
                                self.col_stats.remove(&column_id);
                                continue;
                            }
                        } else {
                            self.col_stats.remove(&column_id);
                            continue;
                        }
                    }

                    let (is_all_null, bitmap) = col.validity();
                    let unset_bits = match (is_all_null, bitmap) {
                        (true, _) => rows,
                        (false, Some(bitmap)) => bitmap.null_count(),
                        (false, None) => 0,
                    };
                    let in_memory_size = col.memory_size() as u64;
                    let col_stats =
                        ColumnStatistics::new(min, max, unset_bits as u64, in_memory_size, None);
                    self.col_stats.get_mut(&column_id).unwrap().push(col_stats);

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
        for (id, stats) in &self.col_stats {
            let mut col_stats = reduce_column_statistics(stats);
            if let Some(count) = column_distinct_count.get(id) {
                col_stats.distinct_of_values = Some(*count as u64);
            } else if let Some(estimator) = self.distinct_columns.get(id) {
                col_stats.distinct_of_values = Some(estimator.finalize());
            }
            statistics.insert(*id, col_stats);
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
    T::Scalar: Hash,
{
    hll: ColumnDistinctHLL,
    _phantom: PhantomData<T>,
}

impl<T> ColumnNDVEstimatorImpl<T>
where
    T: ValueType + Send + Sync,
    T::Scalar: Hash,
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
    T::Scalar: Hash,
{
    fn update_column(&mut self, column: &Column) {
        if let Column::Nullable(box inner) = column {
            let validity_len = inner.validity.len();
            let column = T::try_downcast_column(&inner.column).unwrap();
            if inner.validity.true_count() as f64 / validity_len as f64 >= SELECTIVITY_THRESHOLD {
                for (data, valid) in T::iter_column(&column).zip(inner.validity.iter()) {
                    if valid {
                        self.hll.add_object(&T::to_owned_scalar(data));
                    }
                }
            } else {
                TrueIdxIter::new(validity_len, Some(&inner.validity)).for_each(|idx| {
                    let val = unsafe { T::index_column_unchecked(&column, idx) };
                    self.hll.add_object(&T::to_owned_scalar(val));
                })
            }
        } else {
            let column = T::try_downcast_column(column).unwrap();
            for value in T::iter_column(&column) {
                self.hll.add_object(&T::to_owned_scalar(value));
            }
        }
    }

    fn update_scalar(&mut self, scalar: &ScalarRef) {
        if matches!(scalar, ScalarRef::Null) {
            return;
        }

        let val = T::try_downcast_scalar(scalar).unwrap();
        self.hll.add_object(&T::to_owned_scalar(val));
    }

    fn finalize(&self) -> u64 {
        self.hll.count() as u64
    }
}
