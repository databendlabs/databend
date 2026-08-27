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

use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use itertools::Itertools;

use super::Column;
use super::ColumnBuilder;
use super::Scalar;
use crate::ColumnMinMax;
use crate::property::Domain;
use crate::property::MinMax;
use crate::types::boolean::BooleanDomain;
use crate::types::nullable::NullableDomain;
use crate::types::string::StringDomain;
use crate::types::*;
use crate::visitor::ValueVisitor;

fn extrema_from_iter<'a, T, I>(mut values: I) -> SimpleDomain<T::Scalar>
where
    T: AccessType,
    T::ScalarRef<'a>: Copy,
    I: Iterator<Item = T::ScalarRef<'a>>,
{
    let first = values.next().unwrap();
    let mut min = first;
    let mut max = first;

    for value in values {
        if T::compare(value, min).is_lt() {
            min = value;
        }
        if T::compare(value, max).is_gt() {
            max = value;
        }
    }

    SimpleDomain {
        min: T::to_owned_scalar(min),
        max: T::to_owned_scalar(max),
    }
}

fn column_extrema<T: AccessType>(
    column: &T::Column,
    validity: Option<&Bitmap>,
) -> SimpleDomain<T::Scalar>
where
    for<'a> T::ScalarRef<'a>: Copy,
{
    match validity {
        None => extrema_from_iter::<T, _>(T::iter_column(column)),
        Some(validity) => extrema_from_iter::<T, _>(
            T::iter_column(column)
                .zip(validity.iter())
                .filter_map(|(value, valid)| valid.then_some(value)),
        ),
    }
}

fn boolean_domain(column: &Bitmap, validity: Option<&Bitmap>) -> BooleanDomain {
    match validity {
        None => {
            let true_count = column.true_count();
            BooleanDomain {
                has_false: true_count < column.len(),
                has_true: true_count > 0,
            }
        }
        Some(validity) => {
            let column_chunks = column.chunks::<u64>();
            let validity_chunks = validity.chunks::<u64>();
            let remainder = (column_chunks.remainder(), validity_chunks.remainder());
            let mut has_false = false;
            let mut has_true = false;
            for (column, validity) in column_chunks
                .zip(validity_chunks)
                .chain(std::iter::once(remainder))
            {
                has_false |= !column & validity != 0;
                has_true |= column & validity != 0;
                if has_false && has_true {
                    break;
                }
            }

            BooleanDomain {
                has_false,
                has_true,
            }
        }
    }
}

fn string_domain(column: &StringColumn, validity: Option<&Bitmap>) -> SimpleDomain<String> {
    let (min, max) = match validity {
        None => column.min_max().unwrap(),
        Some(validity) => {
            let mut indices = validity
                .iter()
                .enumerate()
                .filter_map(|(index, valid)| valid.then_some(index));
            let first = indices.next().unwrap();
            let mut min = first;
            let mut max = first;
            for index in indices {
                if StringColumn::compare(column, index, column, min).is_lt() {
                    min = index;
                    continue;
                }
                if StringColumn::compare(column, index, column, max).is_gt() {
                    max = index;
                }
            }
            (column.value(min), column.value(max))
        }
    };

    SimpleDomain {
        min: min.to_owned(),
        max: max.to_owned(),
    }
}

struct ColumnMinMaxVisitor {
    validity: Option<Bitmap>,
    has_null: bool,
}

impl ValueVisitor for ColumnMinMaxVisitor {
    type U = ColumnMinMax;

    fn visit_scalar(&mut self, _scalar: Scalar) -> Result<Self::U> {
        unreachable!("ColumnMinMaxVisitor only visits columns")
    }

    fn visit_null(&mut self, len: usize) -> Result<Self::U> {
        Ok(ColumnMinMax::AllNull)
    }

    fn visit_number<T: Number>(&mut self, column: Buffer<T>) -> Result<Self::U> {
        let domain = column_extrema::<NumberType<T>>(&column, self.validity.as_ref());
        Ok(ColumnMinMax::Values(MinMax::Number(
            T::upcast_domain(domain),
            self.has_null,
        )))
    }

    fn visit_decimal<T: Decimal>(
        &mut self,
        column: Buffer<T>,
        size: DecimalSize,
    ) -> Result<Self::U> {
        let domain = column_extrema::<DecimalType<T>>(&column, self.validity.as_ref());
        let domain = T::upcast_domain(domain, size).into_decimal().unwrap();
        Ok(ColumnMinMax::Values(MinMax::Decimal(domain, self.has_null)))
    }

    fn visit_boolean(&mut self, column: Bitmap) -> Result<Self::U> {
        Ok(ColumnMinMax::Values(MinMax::Boolean(
            boolean_domain(&column, self.validity.as_ref()),
            self.has_null,
        )))
    }

    fn visit_string(&mut self, column: StringColumn) -> Result<Self::U> {
        Ok(ColumnMinMax::Values(MinMax::String(
            string_domain(&column, self.validity.as_ref()),
            self.has_null,
        )))
    }

    fn visit_timestamp(&mut self, column: Buffer<i64>) -> Result<Self::U> {
        let domain = column_extrema::<TimestampType>(&column, self.validity.as_ref());
        Ok(ColumnMinMax::Values(MinMax::Timestamp(
            domain,
            self.has_null,
        )))
    }

    fn visit_timestamp_tz(&mut self, column: Buffer<timestamp_tz>) -> Result<Self::U> {
        let domain = column_extrema::<TimestampTzType>(&column, self.validity.as_ref());
        Ok(ColumnMinMax::Values(MinMax::TimestampTz(
            domain,
            self.has_null,
        )))
    }

    fn visit_date(&mut self, column: Buffer<i32>) -> Result<Self::U> {
        let domain = column_extrema::<DateType>(&column, self.validity.as_ref());
        Ok(ColumnMinMax::Values(MinMax::Date(domain, self.has_null)))
    }

    fn visit_interval(&mut self, column: Buffer<months_days_micros>) -> Result<Self::U> {
        let domain = column_extrema::<IntervalType>(&column, self.validity.as_ref());
        Ok(ColumnMinMax::Values(MinMax::Interval(
            domain,
            self.has_null,
        )))
    }

    fn visit_nullable(&mut self, column: Box<NullableColumn<AnyType>>) -> Result<Self::U> {
        let null_count = column.validity.null_count();
        if null_count == column.len() {
            return Ok(ColumnMinMax::AllNull);
        }

        self.has_null = null_count > 0;
        self.validity = self.has_null.then_some(column.validity);
        self.visit_column(column.column)
    }

    fn visit_typed_column<T: ValueType>(
        &mut self,
        _column: T::Column,
        data_type: &DataType,
    ) -> Result<Self::U> {
        Err(ErrorCode::UnsupportedDataType(format!(
            "min/max is not supported for column type {}",
            data_type
        )))
    }
}

impl Domain {
    pub fn from_min_max(min: Scalar, max: Scalar, data_type: &DataType) -> Self {
        let mut builder = ColumnBuilder::with_capacity(data_type, 2);
        builder.push(min.as_ref());
        builder.push(max.as_ref());
        builder.build().domain()
    }
}

impl Column {
    /// Return exact extrema for comparable scalar columns.
    ///
    /// Empty and all-NULL columns are represented separately. Types without a
    /// supported ordering return an error.
    pub fn min_max(&self) -> Result<ColumnMinMax> {
        if self.len() == 0 {
            return Ok(ColumnMinMax::Empty);
        }

        ColumnMinMaxVisitor {
            validity: None,
            has_null: false,
        }
        .visit_column(self.clone())
    }

    pub fn domain(&self) -> Domain {
        if self.len() == 0 {
            if matches!(self, Column::Array(_)) {
                return Domain::Array(None);
            }
            if matches!(self, Column::Map(_)) {
                return Domain::Map(None);
            }
            return Domain::full(&self.data_type());
        }

        match self {
            Column::Null { .. } => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            Column::EmptyArray { .. } => Domain::Array(None),
            Column::EmptyMap { .. } => Domain::Map(None),
            Column::Number(column) => crate::with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(column) => {
                    let domain = column_extrema::<NumberType<NUM_TYPE>>(column, None);
                    Domain::Number(<NUM_TYPE as Number>::upcast_domain(domain))
                }
            }),
            Column::Decimal(column) => Domain::Decimal(column.domain()),
            Column::Boolean(column) => Domain::Boolean(BooleanDomain {
                has_false: column.null_count() > 0,
                has_true: column.len() - column.null_count() > 0,
            }),
            Column::String(column) => {
                let (min, max) = StringType::iter_column(column)
                    .minmax()
                    .into_option()
                    .unwrap();
                Domain::String(StringDomain {
                    min: min.to_string(),
                    max: Some(max.to_string()),
                })
            }
            Column::Timestamp(column) => {
                let (min, max) = column.iter().minmax().into_option().unwrap();
                Domain::Timestamp(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::TimestampTz(column) => {
                let (min, max) = column.iter().minmax().into_option().unwrap();
                Domain::TimestampTz(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Date(column) => {
                let (min, max) = column.iter().minmax().into_option().unwrap();
                Domain::Date(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Interval(column) => {
                let (min, max) = column.iter().minmax().into_option().unwrap();
                Domain::Interval(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Array(column) => {
                if column.len() == 0 {
                    Domain::Array(None)
                } else {
                    Domain::Array(Some(Box::new(column.underlying_column().domain())))
                }
            }
            Column::Map(column) => {
                if column.len() == 0 {
                    Domain::Map(None)
                } else {
                    Domain::Map(Some(Box::new(column.underlying_column().domain())))
                }
            }
            Column::Nullable(column) => {
                let inner_domain = if column.validity.null_count() > 0 {
                    let inner = column.column.clone().filter(&column.validity);
                    inner.domain()
                } else {
                    column.column.domain()
                };
                Domain::Nullable(NullableDomain {
                    has_null: column.validity.null_count() > 0,
                    value: Some(Box::new(inner_domain)),
                })
            }
            Column::Tuple(fields) => {
                Domain::Tuple(fields.iter().map(|column| column.domain()).collect())
            }
            Column::Binary(_)
            | Column::Bitmap(_)
            | Column::Variant(_)
            | Column::Geometry(_)
            | Column::Geography(_)
            | Column::Vector(_)
            | Column::Opaque(_) => Domain::Undefined,
        }
    }
}
