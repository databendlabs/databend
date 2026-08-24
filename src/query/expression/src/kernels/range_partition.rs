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

use std::fmt;
use std::ops::Range;

use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::Scalar;
use crate::ScalarRef;
use crate::types::DataType;
use crate::types::DecimalColumn;
use crate::types::DecimalScalar;
use crate::types::NumberColumn;
use crate::types::NumberScalar;
use crate::types::StringColumn;
use crate::with_decimal_type;
use crate::with_number_type;

/// Sorted range boundaries with type-specialized lookup for expression columns.
///
/// `lower_bound` counts boundaries strictly smaller than a value, while
/// `upper_bound` counts boundaries smaller than or equal to it. Keeping both
/// operations explicit avoids hiding range-boundary inclusion semantics at call sites.
pub struct TypedRangeBounds {
    inner: Bounds,
}

enum Bounds {
    Empty,
    Number(NumberColumn),
    Decimal(DecimalColumn),
    Timestamp(Vec<i64>),
    Date(Vec<i32>),
    String(StringColumn),
    Scalar(Vec<Scalar>),
}

#[derive(Clone, Copy)]
enum SearchSide {
    Lower,
    Upper,
}

impl TypedRangeBounds {
    /// Convert homogeneous scalar boundaries to their physical type when supported.
    /// Other types retain the generic `Scalar` representation.
    ///
    /// Boundaries must already be sorted according to [`ScalarRef`] ordering. This constructor
    /// intentionally does not sort because callers may build offsets into the same bound array.
    pub fn from_scalars(bounds: Vec<Scalar>) -> Self {
        let Some(first) = bounds.first() else {
            return Self {
                inner: Bounds::Empty,
            };
        };

        let data_type = first.as_ref().infer_data_type();
        let homogeneous = bounds
            .iter()
            .all(|bound| bound.as_ref().infer_data_type() == data_type);
        assert!(
            homogeneous,
            "range boundaries must have one physical data type"
        );
        assert!(
            bounds.windows(2).all(|pair| {
                pair[0]
                    .as_ref()
                    .partial_cmp(&pair[1].as_ref())
                    .is_some_and(|ordering| !ordering.is_gt())
            }),
            "range boundaries must be sorted and totally ordered"
        );
        let inner = match &data_type {
            DataType::Number(_)
            | DataType::Decimal(_)
            | DataType::Timestamp
            | DataType::Date
            | DataType::String => {
                let mut builder = ColumnBuilder::with_capacity(&data_type, bounds.len());
                for bound in &bounds {
                    builder.push(bound.as_ref());
                }
                match builder.build() {
                    Column::Number(bounds) => Bounds::Number(bounds),
                    Column::Decimal(bounds) => Bounds::Decimal(bounds),
                    Column::Timestamp(bounds) => Bounds::Timestamp(bounds.to_vec()),
                    Column::Date(bounds) => Bounds::Date(bounds.to_vec()),
                    Column::String(bounds) => Bounds::String(bounds),
                    _ => unreachable!("matched data type must build the corresponding column"),
                }
            }
            _ => Bounds::Scalar(bounds),
        };
        Self { inner }
    }

    pub fn len(&self) -> usize {
        match &self.inner {
            Bounds::Empty => 0,
            Bounds::Number(bounds) => bounds.len(),
            Bounds::Decimal(bounds) => bounds.len(),
            Bounds::Timestamp(bounds) => bounds.len(),
            Bounds::Date(bounds) => bounds.len(),
            Bounds::String(bounds) => bounds.len(),
            Bounds::Scalar(bounds) => bounds.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the number of boundaries strictly smaller than `value`.
    pub fn lower_bound(&self, value: ScalarRef<'_>) -> u32 {
        self.search(value, SearchSide::Lower)
    }

    /// Return the number of boundaries smaller than or equal to `value`.
    pub fn upper_bound(&self, value: ScalarRef<'_>) -> u32 {
        self.search(value, SearchSide::Upper)
    }

    /// Search one sorted subrange and return the rank relative to its start.
    pub fn lower_bound_range(&self, value: ScalarRef<'_>, range: Range<usize>) -> u32 {
        assert!(
            range.start <= range.end && range.end <= self.len(),
            "range boundary subrange is out of bounds"
        );
        self.search_range(value, range, SearchSide::Lower)
    }

    /// Search one sorted subrange and return the rank relative to its start.
    pub fn upper_bound_range(&self, value: ScalarRef<'_>, range: Range<usize>) -> u32 {
        assert!(
            range.start <= range.end && range.end <= self.len(),
            "range boundary subrange is out of bounds"
        );
        self.search_range(value, range, SearchSide::Upper)
    }

    /// Return lower-bound ranks for all rows, assigning `null_rank` to NULL values.
    pub fn lower_bound_column(&self, entry: &BlockEntry, null_rank: u32) -> Vec<u32> {
        self.search_column(entry, null_rank, SearchSide::Lower)
    }

    /// Return upper-bound ranks for all rows, assigning `null_rank` to NULL values.
    pub fn upper_bound_column(&self, entry: &BlockEntry, null_rank: u32) -> Vec<u32> {
        self.search_column(entry, null_rank, SearchSide::Upper)
    }

    fn search(&self, value: ScalarRef<'_>, side: SearchSide) -> u32 {
        self.search_range(value, 0..self.len(), side)
    }

    fn search_range(&self, value: ScalarRef<'_>, range: Range<usize>, side: SearchSide) -> u32 {
        match &self.inner {
            Bounds::Empty => 0,
            Bounds::Number(bounds) => with_number_type!(|NUM_TYPE| match bounds {
                NumberColumn::NUM_TYPE(bounds) => match value {
                    ScalarRef::Number(NumberScalar::NUM_TYPE(value)) => {
                        search(&bounds[range], &value, side)
                    }
                    _ => type_mismatch(),
                },
            }),
            Bounds::Decimal(bounds) => with_decimal_type!(|DECIMAL_TYPE| match bounds {
                DecimalColumn::DECIMAL_TYPE(bounds, size) => match value {
                    ScalarRef::Decimal(DecimalScalar::DECIMAL_TYPE(value, other))
                        if size == &other =>
                    {
                        search(&bounds[range], &value, side)
                    }
                    _ => type_mismatch(),
                },
            }),
            Bounds::Timestamp(bounds) => match value {
                ScalarRef::Timestamp(value) => search(&bounds[range], &value, side),
                _ => type_mismatch(),
            },
            Bounds::Date(bounds) => match value {
                ScalarRef::Date(value) => search(&bounds[range], &value, side),
                _ => type_mismatch(),
            },
            Bounds::String(bounds) => match value {
                ScalarRef::String(value) => search_indices(
                    range,
                    |index| unsafe { bounds.index_unchecked(index) },
                    value,
                    side,
                ),
                _ => type_mismatch(),
            },
            Bounds::Scalar(bounds) => match side {
                SearchSide::Lower => {
                    bounds[range].partition_point(|bound| bound.as_ref() < value) as u32
                }
                SearchSide::Upper => {
                    bounds[range].partition_point(|bound| bound.as_ref() <= value) as u32
                }
            },
        }
    }

    fn search_column(&self, entry: &BlockEntry, null_rank: u32, side: SearchSide) -> Vec<u32> {
        let rows = entry.len();
        if let BlockEntry::Const(value, _, _) = entry {
            let rank = if matches!(value, Scalar::Null) {
                null_rank
            } else {
                self.search(value.as_ref(), side)
            };
            return vec![rank; rows];
        }
        if let BlockEntry::Column(Column::Nullable(column)) = entry {
            let mut ranks =
                self.search_column(&BlockEntry::Column(column.column.clone()), null_rank, side);
            for (rank, valid) in ranks.iter_mut().zip(column.validity.iter()) {
                if !valid {
                    *rank = null_rank;
                }
            }
            return ranks;
        }

        match (&self.inner, entry) {
            (Bounds::Empty, _) => vec![0; rows],
            (Bounds::Number(bounds), BlockEntry::Column(Column::Number(column))) => {
                with_number_type!(|NUM_TYPE| match bounds {
                    NumberColumn::NUM_TYPE(bounds) => match column {
                        NumberColumn::NUM_TYPE(column) =>
                            search_values(column.iter(), bounds, side),
                        _ => type_mismatch(),
                    },
                })
            }
            (Bounds::Decimal(bounds), BlockEntry::Column(Column::Decimal(column))) => {
                with_decimal_type!(|DECIMAL_TYPE| match bounds {
                    DecimalColumn::DECIMAL_TYPE(bounds, size) => match column {
                        DecimalColumn::DECIMAL_TYPE(column, other) if size == other => {
                            search_values(column.iter(), bounds, side)
                        }
                        _ => type_mismatch(),
                    },
                })
            }
            (Bounds::Timestamp(bounds), BlockEntry::Column(Column::Timestamp(column))) => {
                search_values(column.iter(), bounds, side)
            }
            (Bounds::Date(bounds), BlockEntry::Column(Column::Date(column))) => {
                search_values(column.iter(), bounds, side)
            }
            (Bounds::String(bounds), BlockEntry::Column(Column::String(column))) => column
                .iter()
                .map(|value| {
                    search_indices(
                        0..bounds.len(),
                        |index| unsafe { bounds.index_unchecked(index) },
                        value,
                        side,
                    )
                })
                .collect(),
            _ => (0..rows)
                .map(|row| {
                    // SAFETY: row is bounded by the caller-provided DataBlock row count.
                    let value = unsafe { entry.index_unchecked(row) };
                    if matches!(value, ScalarRef::Null) {
                        null_rank
                    } else {
                        self.search(value, side)
                    }
                })
                .collect(),
        }
    }
}

impl fmt::Debug for TypedRangeBounds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match &self.inner {
            Bounds::Empty => "Empty",
            Bounds::Number(_) => "Number",
            Bounds::Decimal(_) => "Decimal",
            Bounds::Timestamp(_) => "Timestamp",
            Bounds::Date(_) => "Date",
            Bounds::String(_) => "String",
            Bounds::Scalar(_) => "Scalar",
        };
        f.debug_struct("TypedRangeBounds")
            .field("kind", &kind)
            .field("len", &self.len())
            .finish()
    }
}

fn search_values<'a, T: Ord + 'a>(
    values: impl Iterator<Item = &'a T>,
    bounds: &[T],
    side: SearchSide,
) -> Vec<u32> {
    values.map(|value| search(bounds, value, side)).collect()
}

fn search<T: Ord>(bounds: &[T], value: &T, side: SearchSide) -> u32 {
    match side {
        SearchSide::Lower => bounds.partition_point(|bound| bound < value) as u32,
        SearchSide::Upper => bounds.partition_point(|bound| bound <= value) as u32,
    }
}

fn search_indices<'a, U: Ord + ?Sized + 'a>(
    range: Range<usize>,
    get: impl Fn(usize) -> &'a U,
    value: &U,
    side: SearchSide,
) -> u32 {
    let start = range.start;
    let mut left = start;
    let mut right = range.end;
    while left < right {
        let mid = left + (right - left) / 2;
        let before = match side {
            SearchSide::Lower => get(mid) < value,
            SearchSide::Upper => get(mid) <= value,
        };
        if before {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    (left - start) as u32
}

fn type_mismatch<T>() -> T {
    unreachable!("range boundary type must match the input expression type")
}

#[cfg(test)]
mod tests {
    use super::TypedRangeBounds;
    use crate::BlockEntry;
    use crate::FromData;
    use crate::Scalar;
    use crate::ScalarRef;
    use crate::types::DecimalScalar;
    use crate::types::DecimalSize;
    use crate::types::DecimalType;
    use crate::types::Int32Type;
    use crate::types::NumberScalar;
    use crate::types::StringType;

    #[test]
    fn test_lower_upper_bound_and_empty() {
        let bounds = TypedRangeBounds::from_scalars(vec![int(10), int(20), int(30)]);
        assert_eq!(bounds.lower_bound(int(20).as_ref()), 1);
        assert_eq!(bounds.upper_bound(int(20).as_ref()), 2);

        let empty = TypedRangeBounds::from_scalars(Vec::new());
        let entry: BlockEntry = Int32Type::from_data(vec![1, 2, 3]).into();
        assert!(empty.is_empty());
        assert_eq!(empty.upper_bound_column(&entry, 9), vec![0, 0, 0]);
    }

    #[test]
    fn test_typed_column_and_nulls() {
        let bounds = TypedRangeBounds::from_scalars(vec![int(10), int(20), int(30)]);
        let entry: BlockEntry =
            Int32Type::from_opt_data(vec![Some(5), Some(10), None, Some(25), Some(40)]).into();
        assert_eq!(bounds.upper_bound_column(&entry, 7), vec![0, 1, 7, 2, 3]);
    }

    #[test]
    fn test_string_column() {
        let bounds = TypedRangeBounds::from_scalars(vec![
            Scalar::String("b".to_string()),
            Scalar::String("d".to_string()),
        ]);
        let entry: BlockEntry = StringType::from_data(vec!["a", "b", "c", "z"]).into();
        assert_eq!(bounds.lower_bound_column(&entry, 3), vec![0, 0, 1, 2]);
        assert_eq!(bounds.upper_bound_column(&entry, 3), vec![0, 1, 1, 2]);
        assert_eq!(bounds.upper_bound_range(ScalarRef::String("c"), 1..2), 0);
        assert_eq!(bounds.upper_bound_range(ScalarRef::String("d"), 1..2), 1);
    }

    #[test]
    fn test_constant_column() {
        let bounds = TypedRangeBounds::from_scalars(vec![int(10), int(20)]);
        let entry = BlockEntry::new_const_column(
            crate::types::DataType::Number(crate::types::NumberDataType::Int32),
            int(20),
            3,
        );
        assert_eq!(bounds.lower_bound_column(&entry, 9), vec![1, 1, 1]);
        assert_eq!(bounds.upper_bound_column(&entry, 9), vec![2, 2, 2]);
    }

    #[test]
    fn test_decimal_column() {
        let size = DecimalSize::new_unchecked(10, 2);
        let scalar = |value| Scalar::Decimal(DecimalScalar::Decimal64(value, size));
        let bounds = TypedRangeBounds::from_scalars(vec![scalar(100), scalar(200)]);
        let entry: BlockEntry =
            DecimalType::<i64>::from_data_with_size([50, 100, 250], Some(size)).into();
        assert_eq!(bounds.upper_bound_column(&entry, 9), vec![0, 1, 2]);
    }

    #[test]
    #[should_panic(expected = "range boundaries must have one physical data type")]
    fn test_decimal_size_mismatch_is_rejected() {
        let left_size = DecimalSize::new_unchecked(10, 2);
        let right_size = DecimalSize::new_unchecked(12, 3);
        TypedRangeBounds::from_scalars(vec![
            Scalar::Decimal(DecimalScalar::Decimal64(100, left_size)),
            Scalar::Decimal(DecimalScalar::Decimal64(100, right_size)),
        ]);
    }

    #[test]
    fn test_scalar_fallback() {
        let bounds =
            TypedRangeBounds::from_scalars(vec![Scalar::Binary(vec![1]), Scalar::Binary(vec![3])]);
        assert_eq!(bounds.upper_bound(ScalarRef::Binary(&[1])), 1);
        assert_eq!(bounds.lower_bound(ScalarRef::Binary(&[3])), 1);
    }

    #[test]
    #[should_panic(expected = "range boundaries must be sorted and totally ordered")]
    fn test_unsorted_bounds_are_rejected() {
        TypedRangeBounds::from_scalars(vec![int(2), int(1)]);
    }

    fn int(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }
}
