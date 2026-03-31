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

//! Evaluates Parquet Row Group metrics

use std::collections::HashMap;

use fnv::FnvHashSet;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;

use crate::arrow::{get_parquet_stat_max_as_datum, get_parquet_stat_min_as_datum};
use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
use crate::expr::{BoundPredicate, BoundReference};
use crate::spec::{Datum, PrimitiveLiteral, PrimitiveType, Schema};
use crate::{Error, ErrorKind, Result};

pub(crate) struct RowGroupMetricsEvaluator<'a> {
    row_group_metadata: &'a RowGroupMetaData,
    iceberg_field_id_to_parquet_column_index: &'a HashMap<i32, usize>,
    snapshot_schema: &'a Schema,
}

const IN_PREDICATE_LIMIT: usize = 200;
const ROW_GROUP_MIGHT_MATCH: Result<bool> = Ok(true);
const ROW_GROUP_CANT_MATCH: Result<bool> = Ok(false);

impl<'a> RowGroupMetricsEvaluator<'a> {
    fn new(
        row_group_metadata: &'a RowGroupMetaData,
        field_id_map: &'a HashMap<i32, usize>,
        snapshot_schema: &'a Schema,
    ) -> Self {
        Self {
            row_group_metadata,
            iceberg_field_id_to_parquet_column_index: field_id_map,
            snapshot_schema,
        }
    }

    /// Evaluate this `RowGroupMetricsEvaluator`'s filter predicate against the
    /// provided [`RowGroupMetaData`]'. Used by [`ArrowReader`] to
    /// see if a Parquet file RowGroup could possibly contain data that matches
    /// the scan's filter.
    pub(crate) fn eval(
        filter: &'a BoundPredicate,
        row_group_metadata: &'a RowGroupMetaData,
        field_id_map: &'a HashMap<i32, usize>,
        snapshot_schema: &'a Schema,
    ) -> Result<bool> {
        if row_group_metadata.num_rows() == 0 {
            return ROW_GROUP_CANT_MATCH;
        }

        let mut evaluator = Self::new(row_group_metadata, field_id_map, snapshot_schema);

        visit(&mut evaluator, filter)
    }

    fn stats_for_field_id(&self, field_id: i32) -> Option<&Statistics> {
        let parquet_column_index = *self
            .iceberg_field_id_to_parquet_column_index
            .get(&field_id)?;
        self.row_group_metadata
            .column(parquet_column_index)
            .statistics()
    }

    fn null_count(&self, field_id: i32) -> Option<u64> {
        self.stats_for_field_id(field_id)?.null_count_opt()
    }

    fn value_count(&self) -> u64 {
        self.row_group_metadata.num_rows() as u64
    }

    fn contains_nulls_only(&self, field_id: i32) -> bool {
        let null_count = self.null_count(field_id);
        let value_count = self.value_count();

        null_count == Some(value_count)
    }

    fn may_contain_null(&self, field_id: i32) -> bool {
        if let Some(null_count) = self.null_count(field_id) {
            null_count > 0
        } else {
            true
        }
    }

    fn stats_and_type_for_field_id(
        &self,
        field_id: i32,
    ) -> Result<Option<(&Statistics, PrimitiveType)>> {
        let Some(stats) = self.stats_for_field_id(field_id) else {
            // No statistics for column
            return Ok(None);
        };

        let Some(field) = self.snapshot_schema.field_by_id(field_id) else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Could not find a field with id '{}' in the snapshot schema",
                    &field_id
                ),
            ));
        };

        let Some(primitive_type) = field.field_type.as_primitive_type() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Could not determine the PrimitiveType for field id '{}'",
                    &field_id
                ),
            ));
        };

        Ok(Some((stats, primitive_type.clone())))
    }

    fn min_value(&self, field_id: i32) -> Result<Option<Datum>> {
        let Some((stats, primitive_type)) = self.stats_and_type_for_field_id(field_id)? else {
            return Ok(None);
        };

        get_parquet_stat_min_as_datum(&primitive_type, stats)
    }

    fn max_value(&self, field_id: i32) -> Result<Option<Datum>> {
        let Some((stats, primitive_type)) = self.stats_and_type_for_field_id(field_id)? else {
            return Ok(None);
        };

        get_parquet_stat_max_as_datum(&primitive_type, stats)
    }

    fn visit_inequality(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        cmp_fn: fn(&Datum, &Datum) -> bool,
        use_lower_bound: bool,
    ) -> Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROW_GROUP_CANT_MATCH;
        }

        if datum.is_nan() {
            // NaN indicates unreliable bounds.
            // See the InclusiveMetricsEvaluator docs for more.
            return ROW_GROUP_MIGHT_MATCH;
        }

        let bound = if use_lower_bound {
            self.min_value(field_id)
        } else {
            self.max_value(field_id)
        }?;

        if let Some(bound) = bound {
            if cmp_fn(&bound, datum) {
                return ROW_GROUP_MIGHT_MATCH;
            }

            return ROW_GROUP_CANT_MATCH;
        }

        ROW_GROUP_MIGHT_MATCH
    }
}

impl BoundPredicateVisitor for RowGroupMetricsEvaluator<'_> {
    type T = bool;

    fn always_true(&mut self) -> Result<bool> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn always_false(&mut self) -> Result<bool> {
        ROW_GROUP_CANT_MATCH
    }

    fn and(&mut self, lhs: bool, rhs: bool) -> Result<bool> {
        Ok(lhs && rhs)
    }

    fn or(&mut self, lhs: bool, rhs: bool) -> Result<bool> {
        Ok(lhs || rhs)
    }

    fn not(&mut self, inner: bool) -> Result<bool> {
        Ok(!inner)
    }

    fn is_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<bool> {
        let field_id = reference.field().id;

        match self.null_count(field_id) {
            Some(0) => ROW_GROUP_CANT_MATCH,
            Some(_) => ROW_GROUP_MIGHT_MATCH,
            None => ROW_GROUP_MIGHT_MATCH,
        }
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROW_GROUP_CANT_MATCH;
        }

        ROW_GROUP_MIGHT_MATCH
    }

    fn is_nan(&mut self, _reference: &BoundReference, _predicate: &BoundPredicate) -> Result<bool> {
        // NaN counts not in ColumnChunkMetadata Statistics
        ROW_GROUP_MIGHT_MATCH
    }

    fn not_nan(
        &mut self,
        _reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        // NaN counts not in ColumnChunkMetadata Statistics
        ROW_GROUP_MIGHT_MATCH
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::lt, true)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::le, true)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::gt, false)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::ge, false)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROW_GROUP_CANT_MATCH;
        }

        if let Some(lower_bound) = self.min_value(field_id)? {
            if lower_bound.is_nan() {
                // NaN indicates unreliable bounds.
                // See the InclusiveMetricsEvaluator docs for more.
                return ROW_GROUP_MIGHT_MATCH;
            } else if lower_bound.gt(datum) {
                return ROW_GROUP_CANT_MATCH;
            }
        }

        if let Some(upper_bound) = self.max_value(field_id)? {
            if upper_bound.is_nan() {
                // NaN indicates unreliable bounds.
                // See the InclusiveMetricsEvaluator docs for more.
                return ROW_GROUP_MIGHT_MATCH;
            } else if upper_bound.lt(datum) {
                return ROW_GROUP_CANT_MATCH;
            }
        }

        ROW_GROUP_MIGHT_MATCH
    }

    fn not_eq(
        &mut self,
        _reference: &BoundReference,
        _datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        // Because the bounds are not necessarily a min or max value,
        // this cannot be answered using them. notEq(col, X) with (X, Y)
        // doesn't guarantee that X is a value in col.
        ROW_GROUP_MIGHT_MATCH
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROW_GROUP_CANT_MATCH;
        }

        let PrimitiveLiteral::String(datum) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        if let Some(lower_bound) = self.min_value(field_id)? {
            let PrimitiveLiteral::String(lower_bound) = lower_bound.literal() else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use StartsWith operator on non-string lower_bound value",
                ));
            };

            let prefix_length = lower_bound.chars().count().min(datum.chars().count());

            // truncate lower bound so that its length
            // is not greater than the length of prefix
            let truncated_lower_bound = lower_bound.chars().take(prefix_length).collect::<String>();
            if datum < &truncated_lower_bound {
                return ROW_GROUP_CANT_MATCH;
            }
        }

        if let Some(upper_bound) = self.max_value(field_id)? {
            let PrimitiveLiteral::String(upper_bound) = upper_bound.literal() else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use StartsWith operator on non-string upper_bound value",
                ));
            };

            let prefix_length = upper_bound.chars().count().min(datum.chars().count());

            // truncate upper bound so that its length
            // is not greater than the length of prefix
            let truncated_upper_bound = upper_bound.chars().take(prefix_length).collect::<String>();
            if datum > &truncated_upper_bound {
                return ROW_GROUP_CANT_MATCH;
            }
        }

        ROW_GROUP_MIGHT_MATCH
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_id = reference.field().id;

        if self.may_contain_null(field_id) {
            return ROW_GROUP_MIGHT_MATCH;
        }

        // notStartsWith will match unless all values must start with the prefix.
        // This happens when the lower and upper bounds both start with the prefix.

        let PrimitiveLiteral::String(prefix) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        let Some(lower_bound) = self.min_value(field_id)? else {
            return ROW_GROUP_MIGHT_MATCH;
        };

        let PrimitiveLiteral::String(lower_bound_str) = lower_bound.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use NotStartsWith operator on non-string lower_bound value",
            ));
        };

        if lower_bound_str < prefix {
            // if lower is shorter than the prefix then lower doesn't start with the prefix
            return ROW_GROUP_MIGHT_MATCH;
        }

        let prefix_len = prefix.chars().count();

        if lower_bound_str.chars().take(prefix_len).collect::<String>() == *prefix {
            // lower bound matches the prefix

            let Some(upper_bound) = self.max_value(field_id)? else {
                return ROW_GROUP_MIGHT_MATCH;
            };

            let PrimitiveLiteral::String(upper_bound) = upper_bound.literal() else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use NotStartsWith operator on non-string upper_bound value",
                ));
            };

            // if upper is shorter than the prefix then upper can't start with the prefix
            if upper_bound.chars().count() < prefix_len {
                return ROW_GROUP_MIGHT_MATCH;
            }

            if upper_bound.chars().take(prefix_len).collect::<String>() == *prefix {
                // both bounds match the prefix, so all rows must match the
                // prefix and therefore do not satisfy the predicate
                return ROW_GROUP_CANT_MATCH;
            }
        }

        ROW_GROUP_MIGHT_MATCH
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROW_GROUP_CANT_MATCH;
        }

        if literals.len() > IN_PREDICATE_LIMIT {
            // skip evaluating the predicate if the number of values is too big
            return ROW_GROUP_MIGHT_MATCH;
        }

        if let Some(lower_bound) = self.min_value(field_id)? {
            if lower_bound.is_nan() {
                // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
                return ROW_GROUP_MIGHT_MATCH;
            }

            if !literals.iter().any(|datum| datum.ge(&lower_bound)) {
                // if all values are less than lower bound, rows cannot match.
                return ROW_GROUP_CANT_MATCH;
            }
        }

        if let Some(upper_bound) = self.max_value(field_id)? {
            if upper_bound.is_nan() {
                // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
                return ROW_GROUP_MIGHT_MATCH;
            }

            if !literals.iter().any(|datum| datum.le(&upper_bound)) {
                // if all values are greater than upper bound, rows cannot match.
                return ROW_GROUP_CANT_MATCH;
            }
        }

        ROW_GROUP_MIGHT_MATCH
    }

    fn not_in(
        &mut self,
        _reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<bool> {
        // Because the bounds are not necessarily a min or max value,
        // this cannot be answered using them. notIn(col, {X, ...})
        // with (X, Y) doesn't guarantee that X is a value in col.
        ROW_GROUP_MIGHT_MATCH
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use parquet::basic::{LogicalType as ParquetLogicalType, Type as ParquetPhysicalType};
    use parquet::data_type::ByteArray;
    use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
    use parquet::file::statistics::Statistics;
    use parquet::schema::types::{
        ColumnDescriptor, ColumnPath, SchemaDescriptor, Type as parquetSchemaType,
    };
    use rand::{Rng, thread_rng};

    use super::RowGroupMetricsEvaluator;
    use crate::Result;
    use crate::expr::{Bind, Reference};
    use crate::spec::{Datum, NestedField, PrimitiveType, Schema, Type};

    #[test]
    fn eval_matches_no_rows_for_empty_row_group() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(0, 0, None, 0, None)?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .greater_than(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);

        Ok(())
    }

    #[test]
    fn eval_true_for_row_group_no_bounds_present() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(1, 1, None, 1, None)?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .greater_than(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);

        Ok(())
    }

    #[test]
    fn eval_false_for_meta_all_null_filter_not_null() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(None, None, None, Some(1), false)),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_not_null()
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_true_for_meta_all_null_filter_is_null() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(None, None, None, Some(1), false)),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_null()
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_true_for_meta_none_null_filter_not_null() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(None, None, None, Some(0), false)),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_not_null()
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_false_for_meta_none_null_filter_is_null() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(None, None, None, Some(0), false)),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_null()
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_false_for_meta_all_nulls_filter_inequality() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(None, None, None, Some(1), false)),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .greater_than(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_true_for_datum_nan_filter_inequality() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(0.0),
                Some(2.0),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .greater_than(Datum::float(f32::NAN))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_true_for_meta_missing_bound_valid_other_bound_filter_inequality() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(None, Some(2.0), None, Some(0), false)),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .greater_than(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_false_for_meta_failing_bound_filter_inequality() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(0.0),
                Some(0.9),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .greater_than(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_true_for_meta_passing_bound_filter_inequality() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(0.0),
                Some(2.0),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .greater_than(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_false_for_meta_all_nulls_filter_eq() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(None, None, None, Some(1), false)),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .equal_to(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_true_for_meta_lower_nan_filter_eq() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(f32::NAN),
                Some(2.0),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .equal_to(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_false_for_meta_lower_gt_than_datum_filter_eq() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(1.5),
                Some(2.0),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .equal_to(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_true_for_meta_upper_nan_filter_eq() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(0.0),
                Some(f32::NAN),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .equal_to(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_false_for_meta_upper_lt_than_datum_filter_eq() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(0.0),
                Some(0.5),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .equal_to(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_true_for_meta_good_bounds_than_datum_filter_eq() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(0.0),
                Some(2.0),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .equal_to(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_true_for_meta_bounds_eq_datum_filter_neq() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(1.0),
                Some(1.0),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .not_equal_to(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_false_for_meta_all_nulls_filter_starts_with() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            Some(Statistics::byte_array(None, None, None, Some(1), false)),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_error_for_starts_with_non_string_filter_datum() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            Some(Statistics::byte_array(None, None, None, Some(0), false)),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .starts_with(Datum::float(1.0))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn eval_error_for_starts_with_non_utf8_lower_bound() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // min val of 0xff is not valid utf-8 string. Max val of 0x20 is valid utf8
            Some(Statistics::byte_array(
                Some(ByteArray::from(vec![255u8])),
                Some(ByteArray::from(vec![32u8])),
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn eval_error_for_starts_with_non_utf8_upper_bound() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // Max val of 0xFF is not valid utf8
            Some(Statistics::byte_array(
                Some(ByteArray::from("ice".as_bytes())),
                Some(ByteArray::from(vec![255u8])),
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn eval_false_for_starts_with_meta_all_nulls() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // Max val of 0xFF is not valid utf8
            Some(Statistics::byte_array(None, None, None, Some(1), false)),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_false_for_starts_with_datum_below_min_bound() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // Max val of 0xFF is not valid utf8
            Some(Statistics::byte_array(
                Some(ByteArray::from("id".as_bytes())),
                Some(ByteArray::from("ie".as_bytes())),
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_false_for_starts_with_datum_above_max_bound() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // Max val of 0xFF is not valid utf8
            Some(Statistics::byte_array(
                Some(ByteArray::from("h".as_bytes())),
                Some(ByteArray::from("ib".as_bytes())),
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_true_for_starts_with_datum_between_bounds() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // Max val of 0xFF is not valid utf8
            Some(Statistics::byte_array(
                Some(ByteArray::from("h".as_bytes())),
                Some(ByteArray::from("j".as_bytes())),
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_true_for_meta_all_nulls_filter_not_starts_with() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            Some(Statistics::byte_array(None, None, None, Some(1), false)),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .not_starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_error_for_not_starts_with_non_utf8_lower_bound() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // min val of 0xff is not valid utf-8 string. Max val of 0x20 is valid utf8
            Some(Statistics::byte_array(
                Some(ByteArray::from(vec![255u8])),
                Some(ByteArray::from(vec![32u8])),
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .not_starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn eval_error_for_not_starts_with_non_utf8_upper_bound() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // Max val of 0xFF is not valid utf8
            Some(Statistics::byte_array(
                Some(ByteArray::from("iceberg".as_bytes())),
                Some(ByteArray::from(vec![255u8])),
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .not_starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn eval_true_for_not_starts_with_no_min_bound() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // Max val of 0xFF is not valid utf8
            Some(Statistics::byte_array(
                None,
                Some(ByteArray::from("iceberg".as_bytes())),
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .not_starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_true_for_not_starts_with_datum_longer_min_max_bound() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // Max val of 0xFF is not valid utf8
            Some(Statistics::byte_array(
                Some(ByteArray::from("ice".as_bytes())),
                Some(ByteArray::from("iceberg".as_bytes())),
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .not_starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_true_for_not_starts_with_datum_matches_lower_no_upper() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // Max val of 0xFF is not valid utf8
            Some(Statistics::byte_array(
                Some(ByteArray::from("iceberg".as_bytes())),
                None,
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .not_starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_true_for_not_starts_with_datum_matches_lower_upper_shorter() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // Max val of 0xFF is not valid utf8
            Some(Statistics::byte_array(
                Some(ByteArray::from("iceberg".as_bytes())),
                Some(ByteArray::from("icy".as_bytes())),
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .not_starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_false_for_not_starts_with_datum_matches_lower_and_upper() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // Max val of 0xFF is not valid utf8
            Some(Statistics::byte_array(
                Some(ByteArray::from("iceberg".as_bytes())),
                Some(ByteArray::from("iceberg".as_bytes())),
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .not_starts_with(Datum::string("iceberg"))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_false_for_meta_all_nulls_filter_is_in() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            Some(Statistics::byte_array(
                Some(ByteArray::from("iceberg".as_bytes())),
                Some(ByteArray::from("iceberg".as_bytes())),
                None,
                Some(1),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .is_in([Datum::string("ice"), Datum::string("berg")])
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_true_for_too_many_literals_filter_is_in() -> Result<()> {
        let mut rng = thread_rng();

        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(11.0),
                Some(12.0),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_in(std::iter::repeat_with(|| Datum::float(rng.gen_range(0.0..10.0))).take(1000))
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_true_for_missing_bounds_filter_is_in() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            Some(Statistics::byte_array(None, None, None, Some(0), false)),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .is_in([Datum::string("ice")])
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_true_for_lower_bound_is_nan_filter_is_in() -> Result<()> {
        // TODO: should this be false, since the max stat
        //       is lower than the min val in the set?
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(f32::NAN),
                Some(1.0),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_in([Datum::float(2.0), Datum::float(3.0)])
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_false_for_lower_bound_greater_than_all_vals_is_in() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(Some(4.0), None, None, Some(0), false)),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_in([Datum::float(2.0), Datum::float(3.0)])
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_true_for_nan_upper_bound_is_in() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(0.0),
                Some(f32::NAN),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_in([Datum::float(2.0), Datum::float(3.0)])
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
        Ok(())
    }

    #[test]
    fn eval_false_for_upper_bound_below_all_vals_is_in() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            Some(Statistics::float(
                Some(0.0),
                Some(1.0),
                None,
                Some(0),
                false,
            )),
            1,
            None,
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_float")
            .is_in([Datum::float(2.0), Datum::float(3.0)])
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(!result);
        Ok(())
    }

    #[test]
    fn eval_true_for_not_in() -> Result<()> {
        let row_group_metadata = create_row_group_metadata(
            1,
            1,
            None,
            1,
            // Max val of 0xFF is not valid utf8
            Some(Statistics::byte_array(
                Some(ByteArray::from("iceberg".as_bytes())),
                Some(ByteArray::from("iceberg".as_bytes())),
                None,
                Some(0),
                false,
            )),
        )?;

        let (iceberg_schema_ref, field_id_map) = build_iceberg_schema_and_field_map()?;

        let filter = Reference::new("col_string")
            .is_not_in([Datum::string("iceberg")])
            .bind(iceberg_schema_ref.clone(), false)?;

        let result = RowGroupMetricsEvaluator::eval(
            &filter,
            &row_group_metadata,
            &field_id_map,
            iceberg_schema_ref.as_ref(),
        )?;

        assert!(result);
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

    fn build_parquet_schema_descriptor() -> Result<Arc<SchemaDescriptor>> {
        let field_1 = Arc::new(
            parquetSchemaType::primitive_type_builder("col_float", ParquetPhysicalType::FLOAT)
                .with_id(Some(1))
                .build()?,
        );

        let field_2 = Arc::new(
            parquetSchemaType::primitive_type_builder(
                "col_string",
                ParquetPhysicalType::BYTE_ARRAY,
            )
            .with_id(Some(2))
            .with_logical_type(Some(ParquetLogicalType::String))
            .build()?,
        );

        let group_type = Arc::new(
            parquetSchemaType::group_type_builder("all")
                .with_id(Some(1000))
                .with_fields(vec![field_1, field_2])
                .build()?,
        );

        let schema_descriptor = SchemaDescriptor::new(group_type);
        let schema_descriptor_arc = Arc::new(schema_descriptor);
        Ok(schema_descriptor_arc)
    }

    fn create_row_group_metadata(
        num_rows: i64,
        col_1_num_vals: i64,
        col_1_stats: Option<Statistics>,
        col_2_num_vals: i64,
        col_2_stats: Option<Statistics>,
    ) -> Result<RowGroupMetaData> {
        let schema_descriptor_arc = build_parquet_schema_descriptor()?;

        let column_1_desc_ptr = Arc::new(ColumnDescriptor::new(
            schema_descriptor_arc.column(0).self_type_ptr(),
            1,
            1,
            ColumnPath::new(vec!["col_float".to_string()]),
        ));

        let column_2_desc_ptr = Arc::new(ColumnDescriptor::new(
            schema_descriptor_arc.column(1).self_type_ptr(),
            1,
            1,
            ColumnPath::new(vec!["col_string".to_string()]),
        ));

        let mut col_1_meta =
            ColumnChunkMetaData::builder(column_1_desc_ptr).set_num_values(col_1_num_vals);
        if let Some(stats1) = col_1_stats {
            col_1_meta = col_1_meta.set_statistics(stats1)
        }

        let mut col_2_meta =
            ColumnChunkMetaData::builder(column_2_desc_ptr).set_num_values(col_2_num_vals);
        if let Some(stats2) = col_2_stats {
            col_2_meta = col_2_meta.set_statistics(stats2)
        }

        let row_group_metadata = RowGroupMetaData::builder(schema_descriptor_arc)
            .set_num_rows(num_rows)
            .set_column_metadata(vec![
                col_1_meta.build()?,
                // .set_statistics(Statistics::float(None, None, None, Some(1), false))
                col_2_meta.build()?,
            ])
            .build();

        Ok(row_group_metadata?)
    }
}
