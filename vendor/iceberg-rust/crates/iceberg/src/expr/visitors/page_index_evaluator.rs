// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Evaluates predicates against a Parquet Page Index

use std::collections::HashMap;

use fnv::FnvHashSet;
use ordered_float::OrderedFloat;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::page_index::index::Index;
use parquet::file::page_index::offset_index::OffsetIndexMetaData;

use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
use crate::expr::{BoundPredicate, BoundReference};
use crate::spec::{Datum, PrimitiveLiteral, PrimitiveType, Schema};
use crate::{Error, ErrorKind, Result};

type OffsetIndex = Vec<OffsetIndexMetaData>;

const IN_PREDICATE_LIMIT: usize = 200;

enum MissingColBehavior {
    CantMatch,
    MightMatch,
}

enum PageNullCount {
    AllNull,
    NoneNull,
    SomeNull,
    Unknown,
}

impl PageNullCount {
    fn from_row_and_null_counts(num_rows: usize, null_count: Option<i64>) -> Self {
        match (num_rows, null_count) {
            (x, Some(y)) if x == y as usize => PageNullCount::AllNull,
            (_, Some(0)) => PageNullCount::NoneNull,
            (_, Some(_)) => PageNullCount::SomeNull,
            _ => PageNullCount::Unknown,
        }
    }
}

pub(crate) struct PageIndexEvaluator<'a> {
    column_index: &'a [Index],
    offset_index: &'a OffsetIndex,
    row_group_metadata: &'a RowGroupMetaData,
    iceberg_field_id_to_parquet_column_index: &'a HashMap<i32, usize>,
    snapshot_schema: &'a Schema,
    row_count_cache: HashMap<usize, Vec<usize>>,
}

impl<'a> PageIndexEvaluator<'a> {
    pub(crate) fn new(
        column_index: &'a [Index],
        offset_index: &'a OffsetIndex,
        row_group_metadata: &'a RowGroupMetaData,
        field_id_map: &'a HashMap<i32, usize>,
        snapshot_schema: &'a Schema,
    ) -> Self {
        Self {
            column_index,
            offset_index,
            row_group_metadata,
            iceberg_field_id_to_parquet_column_index: field_id_map,
            snapshot_schema,
            row_count_cache: HashMap::new(),
        }
    }

    /// Evaluate this `PageIndexEvaluator`'s filter predicate against a
    /// specific page's column index entry in a parquet file's page index.
    /// [`ArrowReader`] uses the resulting [`RowSelection`] to reject
    /// pages within a parquet file's row group that cannot contain rows
    /// matching the filter predicate.
    pub(crate) fn eval(
        filter: &'a BoundPredicate,
        column_index: &'a [Index],
        offset_index: &'a OffsetIndex,
        row_group_metadata: &'a RowGroupMetaData,
        field_id_map: &'a HashMap<i32, usize>,
        snapshot_schema: &'a Schema,
    ) -> Result<Vec<RowSelector>> {
        if row_group_metadata.num_rows() == 0 {
            return Ok(vec![]);
        }

        let mut evaluator = Self::new(
            column_index,
            offset_index,
            row_group_metadata,
            field_id_map,
            snapshot_schema,
        );

        Ok(visit(&mut evaluator, filter)?.iter().copied().collect())
    }

    fn select_all_rows(&self) -> Result<RowSelection> {
        Ok(vec![RowSelector::select(
            self.row_group_metadata.num_rows() as usize
        )]
        .into())
    }

    fn skip_all_rows(&self) -> Result<RowSelection> {
        Ok(vec![RowSelector::skip(
            self.row_group_metadata.num_rows() as usize
        )]
        .into())
    }

    fn calc_row_selection<F>(
        &mut self,
        field_id: i32,
        predicate: F,
        missing_col_behavior: MissingColBehavior,
    ) -> Result<RowSelection>
    where
        F: Fn(Option<Datum>, Option<Datum>, PageNullCount) -> Result<bool>,
    {
        let Some(&parquet_column_index) =
            self.iceberg_field_id_to_parquet_column_index.get(&field_id)
        else {
            // if the snapshot's column is not present in the row group,
            // exit early
            return match missing_col_behavior {
                MissingColBehavior::CantMatch => self.skip_all_rows(),
                MissingColBehavior::MightMatch => self.select_all_rows(),
            };
        };

        let Some(field) = self.snapshot_schema.field_by_id(field_id) else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Field with id {field_id} missing from snapshot schema"),
            ));
        };

        let Some(field_type) = field.field_type.as_primitive_type() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Field with id {field_id} not convertible to primitive type"),
            ));
        };

        let Some(column_index) = self.column_index.get(parquet_column_index) else {
            // This should not happen, but we fail soft anyway so that the scan is still
            // successful, just a bit slower
            return self.select_all_rows();
        };

        let row_counts = {
            // Caches row count calculations for columns that appear multiple times in
            // the predicate
            match self.row_count_cache.get(&parquet_column_index) {
                Some(count) => count.clone(),
                None => {
                    let Some(offset_index) = self.offset_index.get(parquet_column_index) else {
                        // if we have a column index, we should always have an offset index.
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            format!("Missing offset index for field id {field_id}"),
                        ));
                    };

                    let count = self.calc_row_counts(offset_index);
                    self.row_count_cache
                        .insert(parquet_column_index, count.clone());

                    count
                }
            }
        };

        let Some(page_filter) = Self::apply_predicate_to_column_index(
            predicate,
            field_type,
            column_index,
            &row_counts,
        )?
        else {
            return self.select_all_rows();
        };

        let row_selectors: Vec<_> = row_counts
            .iter()
            .zip(page_filter.iter())
            .map(|(&row_count, &is_selected)| {
                if is_selected {
                    RowSelector::select(row_count)
                } else {
                    RowSelector::skip(row_count)
                }
            })
            .collect();

        Ok(row_selectors.into())
    }

    /// Returns a list of row counts per page
    fn calc_row_counts(&self, offset_index: &OffsetIndexMetaData) -> Vec<usize> {
        let mut remaining_rows = self.row_group_metadata.num_rows() as usize;
        let mut row_counts = Vec::with_capacity(self.offset_index.len());

        let page_locations = offset_index.page_locations();
        for (idx, page_location) in page_locations.iter().enumerate() {
            let row_count = if idx < page_locations.len() - 1 {
                let row_count = (page_locations[idx + 1].first_row_index
                    - page_location.first_row_index) as usize;
                remaining_rows -= row_count;
                row_count
            } else {
                remaining_rows
            };

            row_counts.push(row_count);
        }

        row_counts
    }

    fn apply_predicate_to_column_index<F>(
        predicate: F,
        field_type: &PrimitiveType,
        column_index: &Index,
        row_counts: &[usize],
    ) -> Result<Option<Vec<bool>>>
    where
        F: Fn(Option<Datum>, Option<Datum>, PageNullCount) -> Result<bool>,
    {
        let result: Result<Vec<bool>> = match column_index {
            Index::NONE => {
                return Ok(None);
            }
            Index::BOOLEAN(idx) => idx
                .indexes
                .iter()
                .zip(row_counts.iter())
                .map(|(page_idx, &row_count)| {
                    predicate(
                        page_idx.min.as_ref().map(|&val| {
                            Datum::new(field_type.clone(), PrimitiveLiteral::Boolean(val))
                        }),
                        page_idx.max.as_ref().map(|&val| {
                            Datum::new(field_type.clone(), PrimitiveLiteral::Boolean(val))
                        }),
                        PageNullCount::from_row_and_null_counts(row_count, page_idx.null_count),
                    )
                })
                .collect(),
            Index::INT32(idx) => {
                idx.indexes
                    .iter()
                    .zip(row_counts.iter())
                    .map(|(page_idx, &row_count)| {
                        predicate(
                            page_idx.min.as_ref().map(|&val| {
                                Datum::new(field_type.clone(), PrimitiveLiteral::Int(val))
                            }),
                            page_idx.max.as_ref().map(|&val| {
                                Datum::new(field_type.clone(), PrimitiveLiteral::Int(val))
                            }),
                            PageNullCount::from_row_and_null_counts(row_count, page_idx.null_count),
                        )
                    })
                    .collect()
            }
            Index::INT64(idx) => idx
                .indexes
                .iter()
                .zip(row_counts.iter())
                .map(|(page_idx, &row_count)| {
                    predicate(
                        page_idx.min.as_ref().map(|&val| {
                            Datum::new(field_type.clone(), PrimitiveLiteral::Long(val))
                        }),
                        page_idx.max.as_ref().map(|&val| {
                            Datum::new(field_type.clone(), PrimitiveLiteral::Long(val))
                        }),
                        PageNullCount::from_row_and_null_counts(row_count, page_idx.null_count),
                    )
                })
                .collect(),
            Index::FLOAT(idx) => idx
                .indexes
                .iter()
                .zip(row_counts.iter())
                .map(|(page_idx, &row_count)| {
                    predicate(
                        page_idx.min.as_ref().map(|&val| {
                            Datum::new(
                                field_type.clone(),
                                PrimitiveLiteral::Float(OrderedFloat::from(val)),
                            )
                        }),
                        page_idx.max.as_ref().map(|&val| {
                            Datum::new(
                                field_type.clone(),
                                PrimitiveLiteral::Float(OrderedFloat::from(val)),
                            )
                        }),
                        PageNullCount::from_row_and_null_counts(row_count, page_idx.null_count),
                    )
                })
                .collect(),
            Index::DOUBLE(idx) => idx
                .indexes
                .iter()
                .zip(row_counts.iter())
                .map(|(page_idx, &row_count)| {
                    predicate(
                        page_idx.min.as_ref().map(|&val| {
                            Datum::new(
                                field_type.clone(),
                                PrimitiveLiteral::Double(OrderedFloat::from(val)),
                            )
                        }),
                        page_idx.max.as_ref().map(|&val| {
                            Datum::new(
                                field_type.clone(),
                                PrimitiveLiteral::Double(OrderedFloat::from(val)),
                            )
                        }),
                        PageNullCount::from_row_and_null_counts(row_count, page_idx.null_count),
                    )
                })
                .collect(),
            Index::BYTE_ARRAY(idx) => idx
                .indexes
                .iter()
                .zip(row_counts.iter())
                .map(|(page_idx, &row_count)| {
                    predicate(
                        page_idx.min.as_ref().map(|val| {
                            Datum::new(
                                field_type.clone(),
                                PrimitiveLiteral::String(
                                    String::from_utf8(val.as_ref().to_vec()).unwrap(),
                                ),
                            )
                        }),
                        page_idx.max.as_ref().map(|val| {
                            Datum::new(
                                field_type.clone(),
                                PrimitiveLiteral::String(
                                    String::from_utf8(val.as_ref().to_vec()).unwrap(),
                                ),
                            )
                        }),
                        PageNullCount::from_row_and_null_counts(row_count, page_idx.null_count),
                    )
                })
                .collect(),
            Index::FIXED_LEN_BYTE_ARRAY(_) => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "unsupported 'FIXED_LEN_BYTE_ARRAY' index type in column_index",
                ));
            }
            Index::INT96(_) => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "unsupported 'INT96' index type in column_index",
                ));
            }
        };

        Ok(Some(result?))
    }

    fn visit_inequality(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        cmp_fn: fn(&Datum, &Datum) -> bool,
        use_lower_bound: bool,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        self.calc_row_selection(
            field_id,
            |min, max, null_count| {
                if matches!(null_count, PageNullCount::AllNull) {
                    return Ok(false);
                }

                if datum.is_nan() {
                    // NaN indicates unreliable bounds.
                    return Ok(true);
                }

                let bound = if use_lower_bound { min } else { max };

                if let Some(bound) = bound {
                    if cmp_fn(&bound, datum) {
                        return Ok(true);
                    }

                    return Ok(false);
                }

                Ok(true)
            },
            MissingColBehavior::MightMatch,
        )
    }
}

impl BoundPredicateVisitor for PageIndexEvaluator<'_> {
    type T = RowSelection;

    fn always_true(&mut self) -> Result<RowSelection> {
        self.select_all_rows()
    }

    fn always_false(&mut self) -> Result<RowSelection> {
        self.skip_all_rows()
    }

    fn and(&mut self, lhs: RowSelection, rhs: RowSelection) -> Result<RowSelection> {
        Ok(lhs.intersection(&rhs))
    }

    fn or(&mut self, lhs: RowSelection, rhs: RowSelection) -> Result<RowSelection> {
        Ok(lhs.union(&rhs))
    }

    fn not(&mut self, _: RowSelection) -> Result<RowSelection> {
        Err(Error::new(
            ErrorKind::Unexpected,
            "NOT unsupported at this point. NOT-rewrite should be performed first",
        ))
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        self.calc_row_selection(
            field_id,
            |_max, _min, null_count| Ok(!matches!(null_count, PageNullCount::NoneNull)),
            MissingColBehavior::MightMatch,
        )
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        self.calc_row_selection(
            field_id,
            |_max, _min, null_count| Ok(!matches!(null_count, PageNullCount::AllNull)),
            MissingColBehavior::CantMatch,
        )
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        // NaN counts not present in ColumnChunkMetadata Statistics.
        // Only float columns can be NaN.
        if reference.field().field_type.is_floating_type() {
            self.select_all_rows()
        } else {
            self.skip_all_rows()
        }
    }

    fn not_nan(
        &mut self,
        _reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        // NaN counts not present in ColumnChunkMetadata Statistics
        self.select_all_rows()
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        self.visit_inequality(reference, datum, PartialOrd::lt, true)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        self.visit_inequality(reference, datum, PartialOrd::le, true)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        self.visit_inequality(reference, datum, PartialOrd::gt, false)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        self.visit_inequality(reference, datum, PartialOrd::ge, false)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        self.calc_row_selection(
            field_id,
            |min, max, nulls| {
                if matches!(nulls, PageNullCount::AllNull) {
                    return Ok(false);
                }

                if let Some(min) = min
                    && min.gt(datum)
                {
                    return Ok(false);
                }

                if let Some(max) = max
                    && max.lt(datum)
                {
                    return Ok(false);
                }

                Ok(true)
            },
            MissingColBehavior::CantMatch,
        )
    }

    fn not_eq(
        &mut self,
        _reference: &BoundReference,
        _datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        // Because the bounds are not necessarily a min or max value,
        // this cannot be answered using them. notEq(col, X) with (X, Y)
        // doesn't guarantee that X is a value in col.
        self.select_all_rows()
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        let PrimitiveLiteral::String(datum) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        self.calc_row_selection(
            field_id,
            |min, max, nulls| {
                if matches!(nulls, PageNullCount::AllNull) {
                    return Ok(false);
                }

                if let Some(lower_bound) = min {
                    let PrimitiveLiteral::String(lower_bound) = lower_bound.literal() else {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "Cannot use StartsWith operator on non-string lower_bound value",
                        ));
                    };

                    let prefix_length = lower_bound.chars().count().min(datum.chars().count());

                    // truncate lower bound so that its length
                    // is not greater than the length of prefix
                    let truncated_lower_bound =
                        lower_bound.chars().take(prefix_length).collect::<String>();
                    if datum < &truncated_lower_bound {
                        return Ok(false);
                    }
                }

                if let Some(upper_bound) = max {
                    let PrimitiveLiteral::String(upper_bound) = upper_bound.literal() else {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "Cannot use StartsWith operator on non-string upper_bound value",
                        ));
                    };

                    let prefix_length = upper_bound.chars().count().min(datum.chars().count());

                    // truncate upper bound so that its length
                    // is not greater than the length of prefix
                    let truncated_upper_bound =
                        upper_bound.chars().take(prefix_length).collect::<String>();
                    if datum > &truncated_upper_bound {
                        return Ok(false);
                    }
                }

                Ok(true)
            },
            MissingColBehavior::CantMatch,
        )
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        // notStartsWith will match unless all values must start with the prefix.
        // This happens when the lower and upper bounds both start with the prefix.

        let PrimitiveLiteral::String(prefix) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        self.calc_row_selection(
            field_id,
            |min, max, nulls| {
                if !matches!(nulls, PageNullCount::NoneNull) {
                    return Ok(true);
                }

                let Some(lower_bound) = min else {
                    return Ok(true);
                };

                let PrimitiveLiteral::String(lower_bound_str) = lower_bound.literal() else {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "Cannot use NotStartsWith operator on non-string lower_bound value",
                    ));
                };

                if lower_bound_str < prefix {
                    // if lower is shorter than the prefix then lower doesn't start with the prefix
                    return Ok(true);
                }

                let prefix_len = prefix.chars().count();

                if lower_bound_str.chars().take(prefix_len).collect::<String>() == *prefix {
                    // lower bound matches the prefix

                    let Some(upper_bound) = max else {
                        return Ok(true);
                    };

                    let PrimitiveLiteral::String(upper_bound) = upper_bound.literal() else {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "Cannot use NotStartsWith operator on non-string upper_bound value",
                        ));
                    };

                    // if upper is shorter than the prefix then upper can't start with the prefix
                    if upper_bound.chars().count() < prefix_len {
                        return Ok(true);
                    }

                    if upper_bound.chars().take(prefix_len).collect::<String>() == *prefix {
                        // both bounds match the prefix, so all rows must match the
                        // prefix and therefore do not satisfy the predicate
                        return Ok(false);
                    }
                }

                Ok(true)
            },
            MissingColBehavior::MightMatch,
        )
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        let field_id = reference.field().id;

        if literals.len() > IN_PREDICATE_LIMIT {
            // skip evaluating the predicate if the number of values is too big
            return self.select_all_rows();
        }
        self.calc_row_selection(
            field_id,
            |min, max, nulls| {
                if matches!(nulls, PageNullCount::AllNull) {
                    return Ok(false);
                }

                match (min, max) {
                    (Some(min), Some(max)) => {
                        if literals
                            .iter()
                            .all(|datum| datum.lt(&min) || datum.gt(&max))
                        {
                            // if all values are outside the bounds, rows cannot match.
                            return Ok(false);
                        }
                    }
                    (Some(min), _) => {
                        if !literals.iter().any(|datum| datum.ge(&min)) {
                            // if none of the values are greater than the min bound, rows cant match
                            return Ok(false);
                        }
                    }
                    (_, Some(max)) => {
                        if !literals.iter().any(|datum| datum.le(&max)) {
                            // if all values are greater than upper bound, rows cannot match.
                            return Ok(false);
                        }
                    }

                    _ => {}
                }

                Ok(true)
            },
            MissingColBehavior::CantMatch,
        )
    }

    fn not_in(
        &mut self,
        _reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<RowSelection> {
        // Because the bounds are not necessarily a min or max value,
        // this cannot be answered using them. notIn(col, {X, ...})
        // with (X, Y) doesn't guarantee that X is a value in col.
        self.select_all_rows()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::{
        ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelector,
    };
    use parquet::file::metadata::ParquetMetaData;
    use parquet::file::properties::WriterProperties;
    use rand::{Rng, thread_rng};
    use tempfile::NamedTempFile;

    use super::PageIndexEvaluator;
    use crate::expr::{Bind, Reference};
    use crate::spec::{Datum, NestedField, PrimitiveType, Schema, Type};
    use crate::{ErrorKind, Result};

    /// Helper function to create a test parquet file with page indexes
    /// and return the metadata needed for testing
    fn create_test_parquet_file() -> Result<(Arc<ParquetMetaData>, NamedTempFile)> {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("col_float", DataType::Float32, true),
            Field::new("col_string", DataType::Utf8, true),
        ]));

        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();

        let props = WriterProperties::builder()
            .set_data_page_row_count_limit(1024)
            .set_write_batch_size(512)
            .build();

        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();

        let mut batches = vec![];

        // Batch 1: 1024 rows - strings with AARDVARK, BEAR, BISON
        let float_vals: Vec<Option<f32>> = vec![None; 1024];
        let mut string_vals = vec![];
        string_vals.push(Some("AARDVARK".to_string()));
        for _ in 1..1023 {
            string_vals.push(Some("BEAR".to_string()));
        }
        string_vals.push(Some("BISON".to_string()));

        batches.push(
            RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(Float32Array::from(float_vals)),
                Arc::new(StringArray::from(string_vals)),
            ])
            .unwrap(),
        );

        // Batch 2: 1024 rows - all DEER
        let float_vals: Vec<Option<f32>> = vec![None; 1024];
        let string_vals = vec![Some("DEER".to_string()); 1024];

        batches.push(
            RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(Float32Array::from(float_vals)),
                Arc::new(StringArray::from(string_vals)),
            ])
            .unwrap(),
        );

        // Batch 3: 1024 rows - float 0-10
        let mut float_vals = vec![];
        for i in 0..1024 {
            float_vals.push(Some(i as f32 * 10.0 / 1024.0));
        }
        let mut string_vals = vec![];
        string_vals.push(Some("GIRAFFE".to_string()));
        string_vals.push(None);
        for _ in 2..1024 {
            string_vals.push(Some("HIPPO".to_string()));
        }

        batches.push(
            RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(Float32Array::from(float_vals)),
                Arc::new(StringArray::from(string_vals)),
            ])
            .unwrap(),
        );

        // Batch 4: 1024 rows - float 10-20
        let mut float_vals = vec![None];
        for i in 1..1024 {
            float_vals.push(Some(10.0 + i as f32 * 10.0 / 1024.0));
        }
        let string_vals = vec![Some("HIPPO".to_string()); 1024];

        batches.push(
            RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(Float32Array::from(float_vals)),
                Arc::new(StringArray::from(string_vals)),
            ])
            .unwrap(),
        );

        // Write rows one at a time to give the writer a chance to split into pages
        for batch in &batches {
            for i in 0..batch.num_rows() {
                writer.write(&batch.slice(i, 1)).unwrap();
            }
        }

        writer.close().unwrap();

        let file = temp_file.reopen().unwrap();
        let options = ArrowReaderOptions::new().with_page_index(true);
        let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
        let metadata = reader.metadata().clone();

        Ok((metadata, temp_file))
    }

    /// Get the test metadata components for testing
    fn get_test_metadata(
        metadata: &ParquetMetaData,
    ) -> (
        Vec<parquet::file::page_index::index::Index>,
        Vec<parquet::file::page_index::offset_index::OffsetIndexMetaData>,
        &parquet::file::metadata::RowGroupMetaData,
    ) {
        let row_group_metadata = metadata.row_group(0);
        let column_index = metadata.column_index().unwrap()[0].to_vec();
        let offset_index = metadata.offset_index().unwrap()[0].to_vec();
        (column_index, offset_index, row_group_metadata)
    }

    #[test]
    fn eval_matches_no_rows_for_empty_row_group() -> Result<()> {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("col_float", DataType::Float32, true),
            Field::new("col_string", DataType::Utf8, true),
        ]));

        let empty_float: ArrayRef = Arc::new(Float32Array::from(Vec::<Option<f32>>::new()));
        let empty_string: ArrayRef = Arc::new(StringArray::from(Vec::<Option<String>>::new()));
        let empty_batch =
            RecordBatch::try_new(arrow_schema.clone(), vec![empty_float, empty_string]).unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let file = temp_file.reopen().unwrap();

        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
        writer.write(&empty_batch).unwrap();
        writer.close().unwrap();

        let file = temp_file.reopen().unwrap();
        let options = ArrowReaderOptions::new().with_page_index(true);
        let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
        let metadata = reader.metadata();

        if metadata.num_row_groups() == 0 || metadata.row_group(0).num_rows() == 0 {
            return Ok(());
        }

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .greater_than(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let row_group_metadata = metadata.row_group(0);
        let column_index = metadata.column_index().unwrap()[0].to_vec();
        let offset_index = metadata.offset_index().unwrap()[0].to_vec();

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert_eq!(result.len(), 0);

        Ok(())
    }

    #[test]
    fn eval_is_null_select_only_pages_with_nulls() -> Result<()> {
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_null()
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        let expected = vec![
            RowSelector::select(2048),
            RowSelector::skip(1024),
            RowSelector::select(1024),
        ];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn eval_is_not_null_dont_select_pages_with_all_nulls() -> Result<()> {
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_not_null()
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        let expected = vec![RowSelector::skip(2048), RowSelector::select(2048)];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn eval_is_nan_select_all() -> Result<()> {
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_nan()
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        let expected = vec![RowSelector::select(4096)];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn eval_not_nan_select_all() -> Result<()> {
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_not_nan()
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        let expected = vec![RowSelector::select(4096)];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn eval_inequality_nan_datum_all_rows_except_all_null_pages() -> Result<()> {
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .less_than(Datum::float(f32::NAN))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        let expected = vec![RowSelector::skip(2048), RowSelector::select(2048)];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn eval_inequality_pages_containing_value_except_all_null_pages() -> Result<()> {
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .less_than(Datum::float(5.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        let expected = vec![
            RowSelector::skip(2048),
            RowSelector::select(1024),
            RowSelector::skip(1024),
        ];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn eval_eq_pages_containing_value_except_all_null_pages() -> Result<()> {
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .equal_to(Datum::float(5.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        // Pages 0-1: all null (skip)
        // Page 2: 0-10 (select, might contain 5.0)
        // Page 3: 10-20 (skip, min > 5.0)
        let expected = vec![
            RowSelector::skip(2048),
            RowSelector::select(1024),
            RowSelector::skip(1024),
        ];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn eval_not_eq_all_rows() -> Result<()> {
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .not_equal_to(Datum::float(5.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        let expected = vec![RowSelector::select(4096)];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn eval_starts_with_error_float_col() -> Result<()> {
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .starts_with(Datum::float(5.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        );

        assert_eq!(result.unwrap_err().kind(), ErrorKind::Unexpected);

        Ok(())
    }

    #[test]
    fn eval_starts_with_pages_containing_value_except_all_null_pages() -> Result<()> {
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        // Test starts_with on string column where only some pages match
        // Our file has 4 pages: ["AARDVARK".."BISON"], ["DEER"], ["GIRAFFE".."HIPPO"], ["HIPPO"]
        // Testing starts_with("B") should select only page 0
        let filter = Reference::new("col_string")
            .starts_with(Datum::string("B"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        // Page 0 has "BEAR" and "BISON" (starts with B), rest don't
        let expected = vec![RowSelector::select(1024), RowSelector::skip(3072)];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn eval_not_starts_with_pages_containing_value_except_pages_with_min_and_max_equal_to_prefix_and_all_null_pages()
    -> Result<()> {
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        // Test not_starts_with where one page has ALL values starting with prefix
        // Our file has page 1 with all "DEER" (min="DEER", max="DEER")
        // Testing not_starts_with("DE") should skip page 1 where all values start with "DE"
        let filter = Reference::new("col_string")
            .not_starts_with(Datum::string("DE"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        // Page 0: mixed values (select)
        // Page 1: all "DEER" starting with "DE" (skip)
        // Pages 2-3: other values not all starting with "DE" (select)
        let expected = vec![
            RowSelector::select(1024),
            RowSelector::skip(1024),
            RowSelector::select(2048),
        ];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn eval_in_length_of_set_above_limit_all_rows() -> Result<()> {
        let mut rng = thread_rng();
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_in(std::iter::repeat_with(|| Datum::float(rng.gen_range(0.0..10.0))).take(1000))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        let expected = vec![RowSelector::select(4096)];

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn eval_in_valid_set_size_some_rows() -> Result<()> {
        let (metadata, _temp_file) = create_test_parquet_file()?;
        let (column_index, offset_index, row_group_metadata) = get_test_metadata(&metadata);
        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        // Test is_in with multiple values using min/max bounds
        // Our file has 4 pages: ["AARDVARK".."BISON"], ["DEER"], ["GIRAFFE".."HIPPO"], ["HIPPO"]
        // Testing is_in(["AARDVARK", "GIRAFFE"]) - both are in different pages
        let filter = Reference::new("col_string")
            .is_in([Datum::string("AARDVARK"), Datum::string("GIRAFFE")])
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = PageIndexEvaluator::eval(
            &filter,
            &column_index,
            &offset_index,
            row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        // Page 0 contains "AARDVARK", page 1 doesn't contain either, page 2 contains "GIRAFFE", page 3 doesn't
        let expected = vec![
            RowSelector::select(1024),
            RowSelector::skip(1024),
            RowSelector::select(1024),
            RowSelector::skip(1024),
        ];

        assert_eq!(result, expected);

        Ok(())
    }

    fn build_iceberg_schema_and_field_map() -> Result<(Arc<Schema>, HashMap<i32, usize>)> {
        let iceberg_schema = Schema::builder()
            .with_fields([
                Arc::new(NestedField::new(
                    1,
                    "col_float",
                    Type::Primitive(PrimitiveType::Float),
                    false,
                )),
                Arc::new(NestedField::new(
                    2,
                    "col_string",
                    Type::Primitive(PrimitiveType::String),
                    false,
                )),
            ])
            .build()?;
        let iceberg_schema_ref = Arc::new(iceberg_schema);

        let field_id_map = HashMap::from_iter([(1, 0), (2, 1)]);

        Ok((iceberg_schema_ref, field_id_map))
    }
}
