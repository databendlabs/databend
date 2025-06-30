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

use databend_common_expression::types::boolean::TrueIdxIter;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::Decimal256Type;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::ScalarRef;
use databend_common_expression::SELECTIVITY_THRESHOLD;
use databend_storages_common_table_meta::meta::ColumnDistinctHLL;

pub trait ColumnNDVEstimator: Send + Sync {
    fn update_column(&mut self, column: &Column);
    fn update_scalar(&mut self, scalar: &ScalarRef);
    fn finalize(&self) -> u64;
}

pub fn create_column_ndv_estimator(data_type: &DataType) -> Box<dyn ColumnNDVEstimator> {
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
        DataType::Decimal(size) => {
            if size.can_carried_by_64() {
                ColumnNDVEstimatorImpl::<Decimal64Type>::create()
            } else if size.can_carried_by_128() {
                ColumnNDVEstimatorImpl::<Decimal128Type>::create()
            } else {
                ColumnNDVEstimatorImpl::<Decimal256Type>::create()
            }
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

    fn finalize(&self) -> u64 {
        self.hll.count() as u64
    }
}
