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

use fnv::FnvHashSet;

use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
use crate::expr::{BoundPredicate, BoundReference};
use crate::spec::{DataFile, Datum, PrimitiveLiteral};
use crate::{Error, ErrorKind};

const IN_PREDICATE_LIMIT: usize = 200;
const ROWS_MIGHT_MATCH: crate::Result<bool> = Ok(true);
const ROWS_CANNOT_MATCH: crate::Result<bool> = Ok(false);

pub(crate) struct InclusiveMetricsEvaluator<'a> {
    data_file: &'a DataFile,
}

impl<'a> InclusiveMetricsEvaluator<'a> {
    fn new(data_file: &'a DataFile) -> Self {
        InclusiveMetricsEvaluator { data_file }
    }

    /// Evaluate this `InclusiveMetricsEvaluator`'s filter predicate against the
    /// provided [`DataFile`]'s metrics. Used by [`TableScan`] to
    /// see if this `DataFile` contains data that could match
    /// the scan's filter.
    pub(crate) fn eval(
        filter: &'a BoundPredicate,
        data_file: &'a DataFile,
        include_empty_files: bool,
    ) -> crate::Result<bool> {
        if !include_empty_files && data_file.record_count == 0 {
            return ROWS_CANNOT_MATCH;
        }

        let mut evaluator = Self::new(data_file);
        visit(&mut evaluator, filter)
    }

    fn nan_count(&self, field_id: i32) -> Option<&u64> {
        self.data_file.nan_value_counts.get(&field_id)
    }

    fn null_count(&self, field_id: i32) -> Option<&u64> {
        self.data_file.null_value_counts.get(&field_id)
    }

    fn value_count(&self, field_id: i32) -> Option<&u64> {
        self.data_file.value_counts.get(&field_id)
    }

    fn lower_bound(&self, field_id: i32) -> Option<&Datum> {
        self.data_file.lower_bounds.get(&field_id)
    }

    fn upper_bound(&self, field_id: i32) -> Option<&Datum> {
        self.data_file.upper_bounds.get(&field_id)
    }

    fn contains_nans_only(&self, field_id: i32) -> bool {
        let nan_count = self.nan_count(field_id);
        let value_count = self.value_count(field_id);

        nan_count.is_some() && nan_count == value_count
    }

    fn contains_nulls_only(&self, field_id: i32) -> bool {
        let null_count = self.null_count(field_id);
        let value_count = self.value_count(field_id);

        null_count.is_some() && null_count == value_count
    }

    fn may_contain_null(&self, field_id: i32) -> bool {
        if let Some(&null_count) = self.null_count(field_id) {
            null_count > 0
        } else {
            true
        }
    }

    fn visit_inequality(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        cmp_fn: fn(&Datum, &Datum) -> bool,
        use_lower_bound: bool,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) || self.contains_nans_only(field_id) {
            return ROWS_CANNOT_MATCH;
        }

        if datum.is_nan() {
            // NaN indicates unreliable bounds.
            // See the InclusiveMetricsEvaluator docs for more.
            return ROWS_MIGHT_MATCH;
        }

        let bound = if use_lower_bound {
            self.lower_bound(field_id)
        } else {
            self.upper_bound(field_id)
        };

        if let Some(bound) = bound {
            if cmp_fn(bound, datum) {
                return ROWS_MIGHT_MATCH;
            }

            return ROWS_CANNOT_MATCH;
        }

        ROWS_MIGHT_MATCH
    }
}

impl BoundPredicateVisitor for InclusiveMetricsEvaluator<'_> {
    type T = bool;

    fn always_true(&mut self) -> crate::Result<bool> {
        ROWS_MIGHT_MATCH
    }

    fn always_false(&mut self) -> crate::Result<bool> {
        ROWS_CANNOT_MATCH
    }

    fn and(&mut self, lhs: bool, rhs: bool) -> crate::Result<bool> {
        Ok(lhs && rhs)
    }

    fn or(&mut self, lhs: bool, rhs: bool) -> crate::Result<bool> {
        Ok(lhs || rhs)
    }

    fn not(&mut self, inner: bool) -> crate::Result<bool> {
        Ok(!inner)
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        match self.null_count(field_id) {
            Some(&0) => ROWS_CANNOT_MATCH,
            Some(_) => ROWS_MIGHT_MATCH,
            None => ROWS_MIGHT_MATCH,
        }
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROWS_CANNOT_MATCH;
        }

        ROWS_MIGHT_MATCH
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        match self.nan_count(field_id) {
            Some(&0) => ROWS_CANNOT_MATCH,
            _ if self.contains_nulls_only(field_id) => ROWS_CANNOT_MATCH,
            _ => ROWS_MIGHT_MATCH,
        }
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nans_only(field_id) {
            return ROWS_CANNOT_MATCH;
        }

        ROWS_MIGHT_MATCH
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::lt, true)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::le, true)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::gt, false)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::ge, false)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) || self.contains_nans_only(field_id) {
            return ROWS_CANNOT_MATCH;
        }

        if let Some(lower_bound) = self.lower_bound(field_id) {
            if lower_bound.is_nan() {
                // NaN indicates unreliable bounds.
                // See the InclusiveMetricsEvaluator docs for more.
                return ROWS_MIGHT_MATCH;
            } else if lower_bound.gt(datum) {
                return ROWS_CANNOT_MATCH;
            }
        }

        if let Some(upper_bound) = self.upper_bound(field_id) {
            if upper_bound.is_nan() {
                // NaN indicates unreliable bounds.
                // See the InclusiveMetricsEvaluator docs for more.
                return ROWS_MIGHT_MATCH;
            } else if upper_bound.lt(datum) {
                return ROWS_CANNOT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }

    fn not_eq(
        &mut self,
        _reference: &BoundReference,
        _datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        // Because the bounds are not necessarily a min or max value,
        // this cannot be answered using them. notEq(col, X) with (X, Y)
        // doesn't guarantee that X is a value in col.
        ROWS_MIGHT_MATCH
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROWS_CANNOT_MATCH;
        }

        let PrimitiveLiteral::String(datum) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        if let Some(lower_bound) = self.lower_bound(field_id) {
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
                return ROWS_CANNOT_MATCH;
            }
        }

        if let Some(upper_bound) = self.upper_bound(field_id) {
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
                return ROWS_CANNOT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.may_contain_null(field_id) {
            return ROWS_MIGHT_MATCH;
        }

        // notStartsWith will match unless all values must start with the prefix.
        // This happens when the lower and upper bounds both start with the prefix.

        let PrimitiveLiteral::String(prefix) = datum.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use StartsWith operator on non-string values",
            ));
        };

        let Some(lower_bound) = self.lower_bound(field_id) else {
            return ROWS_MIGHT_MATCH;
        };

        let PrimitiveLiteral::String(lower_bound_str) = lower_bound.literal() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot use NotStartsWith operator on non-string lower_bound value",
            ));
        };

        if lower_bound_str < prefix {
            // if lower is shorter than the prefix then lower doesn't start with the prefix
            return ROWS_MIGHT_MATCH;
        }

        let prefix_len = prefix.chars().count();

        if lower_bound_str.chars().take(prefix_len).collect::<String>() == *prefix {
            // lower bound matches the prefix

            let Some(upper_bound) = self.upper_bound(field_id) else {
                return ROWS_MIGHT_MATCH;
            };

            let PrimitiveLiteral::String(upper_bound) = upper_bound.literal() else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Cannot use NotStartsWith operator on non-string upper_bound value",
                ));
            };

            // if upper is shorter than the prefix then upper can't start with the prefix
            if upper_bound.chars().count() < prefix_len {
                return ROWS_MIGHT_MATCH;
            }

            if upper_bound.chars().take(prefix_len).collect::<String>() == *prefix {
                // both bounds match the prefix, so all rows must match the
                // prefix and therefore do not satisfy the predicate
                return ROWS_CANNOT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) || self.contains_nans_only(field_id) {
            return ROWS_CANNOT_MATCH;
        }

        if literals.len() > IN_PREDICATE_LIMIT {
            // skip evaluating the predicate if the number of values is too big
            return ROWS_MIGHT_MATCH;
        }

        if let Some(lower_bound) = self.lower_bound(field_id) {
            if lower_bound.is_nan() {
                // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
                return ROWS_MIGHT_MATCH;
            }

            if !literals.iter().any(|datum| datum.ge(lower_bound)) {
                // if all values are less than lower bound, rows cannot match.
                return ROWS_CANNOT_MATCH;
            }
        }

        if let Some(upper_bound) = self.upper_bound(field_id) {
            if upper_bound.is_nan() {
                // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
                return ROWS_MIGHT_MATCH;
            }

            if !literals.iter().any(|datum| datum.le(upper_bound)) {
                // if all values are greater than upper bound, rows cannot match.
                return ROWS_CANNOT_MATCH;
            }
        }

        ROWS_MIGHT_MATCH
    }

    fn not_in(
        &mut self,
        _reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        // Because the bounds are not necessarily a min or max value,
        // this cannot be answered using them. notIn(col, {X, ...})
        // with (X, Y) doesn't guarantee that X is a value in col.
        ROWS_MIGHT_MATCH
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::ops::Not;
    use std::sync::Arc;

    use fnv::FnvHashSet;

    use crate::expr::PredicateOperator::{
        Eq, GreaterThan, GreaterThanOrEq, In, IsNan, IsNull, LessThan, LessThanOrEq, NotEq, NotIn,
        NotNan, NotNull, NotStartsWith, StartsWith,
    };
    use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
    use crate::expr::{
        BinaryExpression, Bind, BoundPredicate, Predicate, Reference, SetExpression,
        UnaryExpression,
    };
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, Datum, NestedField, PartitionSpec,
        PartitionSpecRef, PrimitiveType, Schema, SchemaRef, Struct, Transform, Type,
        UnboundPartitionField,
    };

    const INT_MIN_VALUE: i32 = 30;
    const INT_MAX_VALUE: i32 = 79;

    #[test]
    fn test_data_file_no_partitions() {
        let (_partition_spec_ref, schema_ref) = create_test_partition_spec();

        let partition_filter = Predicate::AlwaysTrue
            .bind(schema_ref.clone(), false)
            .unwrap();

        let case_sensitive = false;

        let data_file = create_test_data_file();

        let result =
            InclusiveMetricsEvaluator::eval(&partition_filter, &data_file, case_sensitive).unwrap();

        assert!(result);
    }

    #[test]
    fn test_all_nulls() {
        let result =
            InclusiveMetricsEvaluator::eval(&not_null("all_nulls"), &get_test_file_1(), true)
                .unwrap();
        assert!(!result, "Should skip: no non-null value in all null column");

        let result =
            InclusiveMetricsEvaluator::eval(&less_than("all_nulls", "a"), &get_test_file_1(), true)
                .unwrap();
        assert!(!result, "Should skip: LessThan on an all null column");

        let result = InclusiveMetricsEvaluator::eval(
            &less_than_or_equal("all_nulls", "a"),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            !result,
            "Should skip: LessThanOrEqual on an all null column"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &greater_than("all_nulls", "a"),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: GreaterThan on an all null column");

        let result = InclusiveMetricsEvaluator::eval(
            &greater_than_or_equal("all_nulls", "a"),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            !result,
            "Should skip: GreaterThanOrEqual on an all null column"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&equal("all_nulls", "a"), &get_test_file_1(), true)
                .unwrap();
        assert!(!result, "Should skip: Equal on an all null column");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("all_nulls", "a"),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: StartsWith on an all null column");

        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("all_nulls", "a"),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: NotStartsWith on an all null column");

        let result =
            InclusiveMetricsEvaluator::eval(&not_null("some_nulls"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            result,
            "Should read: col with some nulls could contain a non-null value"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&not_null("no_nulls"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            result,
            "Should read: col with all nulls contains a non-null value"
        );
    }

    #[test]
    fn test_no_nulls() {
        let result =
            InclusiveMetricsEvaluator::eval(&is_null("all_nulls"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            result,
            "Should read: col with all nulls contains a non-null value"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&is_null("some_nulls"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            result,
            "Should read: col with some nulls could contain a non-null value"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&is_null("no_nulls"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            !result,
            "Should skip: col with no nulls can't contains a non-null value"
        );
    }

    #[test]
    fn test_is_nan() {
        let result =
            InclusiveMetricsEvaluator::eval(&is_nan("all_nans"), &get_test_file_1(), true).unwrap();
        assert!(
            result,
            "Should read: col with all nans must contains a nan value"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&is_nan("some_nans"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            result,
            "Should read: col with some nans could contains a nan value"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&is_nan("no_nans"), &get_test_file_1(), true).unwrap();
        assert!(
            !result,
            "Should skip: col with no nans can't contains a nan value"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&is_nan("all_nulls_double"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            !result,
            "Should skip: col with no nans can't contains a nan value"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&is_nan("no_nan_stats"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            result,
            "Should read: no guarantee col is nan-free without nan stats"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&is_nan("all_nans_v1_stats"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            result,
            "Should read: col with all nans must contains a nan value"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&is_nan("nan_and_null_only"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            result,
            "Should read: col with nans and nulls must contain a nan value"
        );
    }

    #[test]
    fn test_not_nan() {
        let result =
            InclusiveMetricsEvaluator::eval(&not_nan("all_nans"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            !result,
            "Should read: col with all nans must contains a nan value"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&not_nan("some_nans"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            result,
            "Should read: col with some nans could contains a nan value"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&not_nan("no_nans"), &get_test_file_1(), true).unwrap();
        assert!(
            result,
            "Should read: col with no nans might contains a non-nan value"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&not_nan("all_nulls_double"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            result,
            "Should read: col with no nans can't contains a nan value"
        );

        let result =
            InclusiveMetricsEvaluator::eval(&not_nan("no_nan_stats"), &get_test_file_1(), true)
                .unwrap();
        assert!(
            result,
            "Should read: no guarantee col is nan-free without nan stats"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &not_nan("all_nans_v1_stats"),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            result,
            "Should read: col with all nans must contains a nan value"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &not_nan("nan_and_null_only"),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            result,
            "Should read: col with nans and nulls may contain a non-nan value"
        );
    }

    #[test]
    fn test_required_column() {
        let result =
            InclusiveMetricsEvaluator::eval(&not_null("required"), &get_test_file_1(), true)
                .unwrap();
        assert!(result, "Should read: required columns are always non-null");

        let result =
            InclusiveMetricsEvaluator::eval(&is_null("required"), &get_test_file_1(), true)
                .unwrap();
        assert!(!result, "Should skip: required columns are always non-null");
    }

    #[test]
    #[should_panic]
    fn test_missing_column() {
        let _result =
            InclusiveMetricsEvaluator::eval(&less_than("missing", "a"), &get_test_file_1(), true);
    }

    #[test]
    fn test_missing_stats() {
        let missing_stats_datafile = create_test_data_file();

        let expressions = [
            less_than_int("no_stats", 5),
            less_than_or_equal_int("no_stats", 30),
            equal_int("no_stats", 70),
            greater_than_int("no_stats", 78),
            greater_than_or_equal_int("no_stats", 90),
            not_equal_int("no_stats", 101),
            is_null("no_stats"),
            not_null("no_stats"),
            // is_nan("no_stats"),
            // not_nan("no_stats"),
        ];

        for expression in expressions {
            let result =
                InclusiveMetricsEvaluator::eval(&expression, &missing_stats_datafile, true)
                    .unwrap();

            assert!(
                result,
                "Should read if stats are missing for {:?}",
                &expression
            );
        }
    }

    #[test]
    fn test_zero_record_file() {
        let zero_records_datafile = create_zero_records_data_file();

        let expressions = [
            less_than_int("no_stats", 5),
            less_than_or_equal_int("no_stats", 30),
            equal_int("no_stats", 70),
            greater_than_int("no_stats", 78),
            greater_than_or_equal_int("no_stats", 90),
            not_equal_int("no_stats", 101),
            is_null("no_stats"),
            not_null("no_stats"),
            // is_nan("no_stats"),
            // not_nan("no_stats"),
        ];

        for expression in expressions {
            let result =
                InclusiveMetricsEvaluator::eval(&expression, &zero_records_datafile, true).unwrap();

            assert!(
                result,
                "Should skip if data file has zero records (expression: {:?})",
                &expression
            );
        }
    }

    #[test]
    fn test_not() {
        // Not sure if we need a test for this, as we'd expect,
        // as a precondition, that rewrite-not has already been applied.

        let result = InclusiveMetricsEvaluator::eval(
            &not_less_than_int("id", INT_MIN_VALUE - 25),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: not(false)");

        let result = InclusiveMetricsEvaluator::eval(
            &not_greater_than_int("id", INT_MIN_VALUE - 25),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: not(true)");
    }

    #[test]
    fn test_and() {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .and(Predicate::Binary(BinaryExpression::new(
            GreaterThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 30),
        )));

        let bound_pred = filter.bind(schema.clone(), true).unwrap();

        let result =
            InclusiveMetricsEvaluator::eval(&bound_pred, &get_test_file_1(), true).unwrap();
        assert!(!result, "Should skip: and(false, true)");

        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .and(Predicate::Binary(BinaryExpression::new(
            GreaterThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MAX_VALUE + 1),
        )));

        let bound_pred = filter.bind(schema.clone(), true).unwrap();

        let result =
            InclusiveMetricsEvaluator::eval(&bound_pred, &get_test_file_1(), true).unwrap();
        assert!(!result, "Should skip: and(false, false)");

        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            GreaterThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .and(Predicate::Binary(BinaryExpression::new(
            LessThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE),
        )));

        let bound_pred = filter.bind(schema.clone(), true).unwrap();

        let result =
            InclusiveMetricsEvaluator::eval(&bound_pred, &get_test_file_1(), true).unwrap();
        assert!(result, "Should read: and(true, true)");
    }

    #[test]
    fn test_or() {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .or(Predicate::Binary(BinaryExpression::new(
            GreaterThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 30),
        )));

        let bound_pred = filter.bind(schema.clone(), true).unwrap();

        let result =
            InclusiveMetricsEvaluator::eval(&bound_pred, &get_test_file_1(), true).unwrap();
        assert!(result, "Should read: or(false, true)");

        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .or(Predicate::Binary(BinaryExpression::new(
            GreaterThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MAX_VALUE + 1),
        )));

        let bound_pred = filter.bind(schema.clone(), true).unwrap();

        let result =
            InclusiveMetricsEvaluator::eval(&bound_pred, &get_test_file_1(), true).unwrap();
        assert!(!result, "Should skip: or(false, false)");
    }

    #[test]
    fn test_integer_lt() {
        let result = InclusiveMetricsEvaluator::eval(
            &less_than_int("id", INT_MIN_VALUE - 25),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: id range below lower bound (5 < 30)");

        let result = InclusiveMetricsEvaluator::eval(
            &less_than_int("id", INT_MIN_VALUE),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            !result,
            "Should skip: id range below lower bound (30 is not < 30)"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &less_than_int("id", INT_MIN_VALUE + 1),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: one possible id");

        let result = InclusiveMetricsEvaluator::eval(
            &less_than_int("id", INT_MAX_VALUE),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: many possible ids");
    }

    #[test]
    fn test_integer_lt_eq() {
        let result = InclusiveMetricsEvaluator::eval(
            &less_than_or_equal_int("id", INT_MIN_VALUE - 25),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: id range below lower bound (5 < 30)");

        let result = InclusiveMetricsEvaluator::eval(
            &less_than_or_equal_int("id", INT_MIN_VALUE - 1),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: id range below lower bound (29 < 30)");

        let result = InclusiveMetricsEvaluator::eval(
            &less_than_or_equal_int("id", INT_MIN_VALUE),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: one possible id");

        let result = InclusiveMetricsEvaluator::eval(
            &less_than_or_equal_int("id", INT_MAX_VALUE),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: many possible ids");
    }

    #[test]
    fn test_integer_gt() {
        let result = InclusiveMetricsEvaluator::eval(
            &greater_than_int("id", INT_MAX_VALUE + 6),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: id range above upper bound (85 > 79)");

        let result = InclusiveMetricsEvaluator::eval(
            &greater_than_int("id", INT_MAX_VALUE),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            !result,
            "Should skip: id range above upper bound (79 is not > 79)"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &greater_than_int("id", INT_MAX_VALUE - 1),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: one possible id");

        let result = InclusiveMetricsEvaluator::eval(
            &greater_than_int("id", INT_MAX_VALUE - 4),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: many possible ids");
    }

    #[test]
    fn test_integer_gt_eq() {
        let result = InclusiveMetricsEvaluator::eval(
            &greater_than_or_equal_int("id", INT_MAX_VALUE + 6),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: id range above upper bound (85 < 79)");

        let result = InclusiveMetricsEvaluator::eval(
            &greater_than_or_equal_int("id", INT_MAX_VALUE + 1),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: id range above upper bound (80 > 79)");

        let result = InclusiveMetricsEvaluator::eval(
            &greater_than_or_equal_int("id", INT_MAX_VALUE),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: one possible id");

        let result = InclusiveMetricsEvaluator::eval(
            &greater_than_or_equal_int("id", INT_MAX_VALUE - 4),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: many possible ids");
    }

    #[test]
    fn test_integer_eq() {
        let result = InclusiveMetricsEvaluator::eval(
            &equal_int("id", INT_MIN_VALUE - 25),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: id below lower bound");

        let result = InclusiveMetricsEvaluator::eval(
            &equal_int("id", INT_MIN_VALUE - 1),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: id below lower bound");

        let result = InclusiveMetricsEvaluator::eval(
            &equal_int("id", INT_MIN_VALUE),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id equal to lower bound");

        let result = InclusiveMetricsEvaluator::eval(
            &equal_int("id", INT_MAX_VALUE - 4),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id between lower and upper bounds");

        let result = InclusiveMetricsEvaluator::eval(
            &equal_int("id", INT_MAX_VALUE),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id equal to upper bound");

        let result = InclusiveMetricsEvaluator::eval(
            &equal_int("id", INT_MAX_VALUE + 1),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: id above upper bound");

        let result = InclusiveMetricsEvaluator::eval(
            &equal_int("id", INT_MAX_VALUE + 6),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: id above upper bound");
    }

    #[test]
    fn test_integer_not_eq() {
        let result = InclusiveMetricsEvaluator::eval(
            &not_equal_int("id", INT_MIN_VALUE - 25),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id below lower bound");

        let result = InclusiveMetricsEvaluator::eval(
            &not_equal_int("id", INT_MIN_VALUE - 1),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id below lower bound");

        let result = InclusiveMetricsEvaluator::eval(
            &not_equal_int("id", INT_MIN_VALUE),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id equal to lower bound");

        let result = InclusiveMetricsEvaluator::eval(
            &not_equal_int("id", INT_MAX_VALUE - 4),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id between lower and upper bounds");

        let result = InclusiveMetricsEvaluator::eval(
            &not_equal_int("id", INT_MAX_VALUE),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id equal to upper bound");

        let result = InclusiveMetricsEvaluator::eval(
            &not_equal_int("id", INT_MAX_VALUE + 1),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id above upper bound");

        let result = InclusiveMetricsEvaluator::eval(
            &not_equal_int("id", INT_MAX_VALUE + 6),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id above upper bound");
    }

    #[test]
    #[should_panic]
    fn test_case_sensitive_integer_not_eq_rewritten() {
        let _result =
            InclusiveMetricsEvaluator::eval(&equal_int_not("ID", 5), &get_test_file_1(), true)
                .unwrap();
    }

    #[test]
    fn test_string_starts_with() {
        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("required", "a"),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: no stats");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("required", "a"),
            &get_test_file_2(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("required", "aa"),
            &get_test_file_2(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("required", "aaa"),
            &get_test_file_2(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("required", "1s"),
            &get_test_file_3(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("required", "1str1x"),
            &get_test_file_3(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("required", "ff"),
            &get_test_file_4(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("required", "aB"),
            &get_test_file_2(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: range does not match");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("required", "dWX"),
            &get_test_file_2(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: range does not match");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("required", "5"),
            &get_test_file_3(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: range does not match");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("required", "3str3x"),
            &get_test_file_3(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: range does not match");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("some_empty", "房东整租霍"),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range does matches");

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("all_nulls", ""),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: range does not match");

        // Note: This string has been created manually by taking
        // the string "イロハニホヘト", which is an upper bound in
        // the datafile returned by get_test_file_4(), truncating it
        // to four character, and then appending the "ボ" character,
        // which occupies the next code point after the 5th
        // character in the string above, "ホ".
        // In the Java implementation of Iceberg, this is done by
        // the `truncateStringMax` function, but we don't yet have
        // this implemented in iceberg-rust.
        let above_max = "イロハニボ";

        let result = InclusiveMetricsEvaluator::eval(
            &starts_with("required", above_max),
            &get_test_file_4(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: range does not match");
    }

    #[test]
    fn test_string_not_starts_with() {
        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("required", "a"),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: no stats");

        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("required", "a"),
            &get_test_file_2(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("required", "aa"),
            &get_test_file_2(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("required", "aaa"),
            &get_test_file_2(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("required", "1s"),
            &get_test_file_3(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("required", "1str1x"),
            &get_test_file_3(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("required", "ff"),
            &get_test_file_4(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("required", "aB"),
            &get_test_file_2(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("required", "dWX"),
            &get_test_file_2(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("required", "5"),
            &get_test_file_3(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("required", "3str3x"),
            &get_test_file_3(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");

        let above_max = "イロハニホヘト";
        let result = InclusiveMetricsEvaluator::eval(
            &not_starts_with("required", above_max),
            &get_test_file_4(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: range matches");
    }

    #[test]
    fn test_integer_in() {
        let result = InclusiveMetricsEvaluator::eval(
            &r#in_int("id", &[INT_MIN_VALUE - 25, INT_MIN_VALUE - 24]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            !result,
            "Should skip: id below lower bound (5 < 30, 6 < 30)"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &r#in_int("id", &[INT_MIN_VALUE - 2, INT_MIN_VALUE - 1]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            !result,
            "Should skip: id below lower bound (28 < 30, 29 < 30)"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &r#in_int("id", &[INT_MIN_VALUE - 1, INT_MIN_VALUE]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id equal to lower bound (30 == 30)");

        let result = InclusiveMetricsEvaluator::eval(
            &r#in_int("id", &[INT_MAX_VALUE - 4, INT_MAX_VALUE - 3]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            result,
            "Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &r#in_int("id", &[INT_MAX_VALUE, INT_MAX_VALUE + 1]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id equal to upper bound (79 == 79)");

        let result = InclusiveMetricsEvaluator::eval(
            &r#in_int("id", &[INT_MAX_VALUE + 1, INT_MAX_VALUE + 2]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            !result,
            "Should skip: id above upper bound (80 > 79, 81 > 79)"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &r#in_int("id", &[INT_MAX_VALUE + 6, INT_MAX_VALUE + 7]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            !result,
            "Should skip: id above upper bound (85 > 79, 86 > 79)"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &r#in_str("all_nulls", &["abc", "def"]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(!result, "Should skip: in on all nulls column");

        let result = InclusiveMetricsEvaluator::eval(
            &r#in_str("some_nulls", &["abc", "def"]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: in on some nulls column");

        let result = InclusiveMetricsEvaluator::eval(
            &r#in_str("no_nulls", &["abc", "def"]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: in on no nulls column");

        let ids = (-400..=0).collect::<Vec<_>>();
        let result =
            InclusiveMetricsEvaluator::eval(&r#in_int("id", &ids), &get_test_file_1(), true)
                .unwrap();
        assert!(
            result,
            "Should read: number of items in In expression greater than threshold"
        );
    }

    #[test]
    fn test_integer_not_in() {
        let result = InclusiveMetricsEvaluator::eval(
            &r#not_in_int("id", &[INT_MIN_VALUE - 25, INT_MIN_VALUE - 24]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id below lower bound (5 < 30, 6 < 30)");

        let result = InclusiveMetricsEvaluator::eval(
            &r#not_in_int("id", &[INT_MIN_VALUE - 2, INT_MIN_VALUE - 1]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            result,
            "Should read: id below lower bound (28 < 30, 29 < 30)"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &r#not_in_int("id", &[INT_MIN_VALUE - 1, INT_MIN_VALUE]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id equal to lower bound (30 == 30)");

        let result = InclusiveMetricsEvaluator::eval(
            &r#not_in_int("id", &[INT_MAX_VALUE - 4, INT_MAX_VALUE - 3]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            result,
            "Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &r#not_in_int("id", &[INT_MAX_VALUE, INT_MAX_VALUE + 1]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: id equal to upper bound (79 == 79)");

        let result = InclusiveMetricsEvaluator::eval(
            &r#not_in_int("id", &[INT_MAX_VALUE + 1, INT_MAX_VALUE + 2]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            result,
            "Should read: id above upper bound (80 > 79, 81 > 79)"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &r#not_in_int("id", &[INT_MAX_VALUE + 6, INT_MAX_VALUE + 7]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(
            result,
            "Should read: id above upper bound (85 > 79, 86 > 79)"
        );

        let result = InclusiveMetricsEvaluator::eval(
            &r#not_in_str("all_nulls", &["abc", "def"]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: NotIn on all nulls column");

        let result = InclusiveMetricsEvaluator::eval(
            &r#not_in_str("some_nulls", &["abc", "def"]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: NotIn on some nulls column");

        let result = InclusiveMetricsEvaluator::eval(
            &r#not_in_str("no_nulls", &["abc", "def"]),
            &get_test_file_1(),
            true,
        )
        .unwrap();
        assert!(result, "Should read: NotIn on no nulls column");
    }

    fn create_test_partition_spec() -> (PartitionSpecRef, SchemaRef) {
        let table_schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Float),
            ))])
            .build()
            .unwrap();
        let table_schema_ref = Arc::new(table_schema);

        let partition_spec = PartitionSpec::builder(table_schema_ref.clone())
            .with_spec_id(1)
            .add_unbound_fields(vec![
                UnboundPartitionField::builder()
                    .source_id(1)
                    .name("a".to_string())
                    .field_id(1)
                    .transform(Transform::Identity)
                    .build(),
            ])
            .unwrap()
            .build()
            .unwrap();
        (Arc::new(partition_spec), table_schema_ref)
    }

    fn not_null(reference: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Unary(UnaryExpression::new(NotNull, Reference::new(reference)));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn is_null(reference: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Unary(UnaryExpression::new(IsNull, Reference::new(reference)));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn not_nan(reference: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Unary(UnaryExpression::new(NotNan, Reference::new(reference)));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn is_nan(reference: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Unary(UnaryExpression::new(IsNan, Reference::new(reference)));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn less_than(reference: &str, str_literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            LessThan,
            Reference::new(reference),
            Datum::string(str_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn less_than_or_equal(reference: &str, str_literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            LessThanOrEq,
            Reference::new(reference),
            Datum::string(str_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn greater_than(reference: &str, str_literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            GreaterThan,
            Reference::new(reference),
            Datum::string(str_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn greater_than_or_equal(reference: &str, str_literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            GreaterThanOrEq,
            Reference::new(reference),
            Datum::string(str_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn equal(reference: &str, str_literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            Eq,
            Reference::new(reference),
            Datum::string(str_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn less_than_int(reference: &str, int_literal: i32) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            LessThan,
            Reference::new(reference),
            Datum::int(int_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn not_less_than_int(reference: &str, int_literal: i32) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            LessThan,
            Reference::new(reference),
            Datum::int(int_literal),
        ))
        .not();
        filter.bind(schema.clone(), true).unwrap()
    }

    fn less_than_or_equal_int(reference: &str, int_literal: i32) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            LessThanOrEq,
            Reference::new(reference),
            Datum::int(int_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn greater_than_int(reference: &str, int_literal: i32) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            GreaterThan,
            Reference::new(reference),
            Datum::int(int_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn not_greater_than_int(reference: &str, int_literal: i32) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            GreaterThan,
            Reference::new(reference),
            Datum::int(int_literal),
        ))
        .not();
        filter.bind(schema.clone(), true).unwrap()
    }

    fn greater_than_or_equal_int(reference: &str, int_literal: i32) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            GreaterThanOrEq,
            Reference::new(reference),
            Datum::int(int_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn equal_int(reference: &str, int_literal: i32) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            Eq,
            Reference::new(reference),
            Datum::int(int_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn equal_int_not(reference: &str, int_literal: i32) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            Eq,
            Reference::new(reference),
            Datum::int(int_literal),
        ))
        .not();
        filter.bind(schema.clone(), true).unwrap()
    }

    fn not_equal_int(reference: &str, int_literal: i32) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            NotEq,
            Reference::new(reference),
            Datum::int(int_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn starts_with(reference: &str, str_literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            StartsWith,
            Reference::new(reference),
            Datum::string(str_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn not_starts_with(reference: &str, str_literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            NotStartsWith,
            Reference::new(reference),
            Datum::string(str_literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn in_int(reference: &str, int_literals: &[i32]) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Set(SetExpression::new(
            In,
            Reference::new(reference),
            FnvHashSet::from_iter(int_literals.iter().map(|&lit| Datum::int(lit))),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn in_str(reference: &str, str_literals: &[&str]) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Set(SetExpression::new(
            In,
            Reference::new(reference),
            FnvHashSet::from_iter(str_literals.iter().map(Datum::string)),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn not_in_int(reference: &str, int_literals: &[i32]) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Set(SetExpression::new(
            NotIn,
            Reference::new(reference),
            FnvHashSet::from_iter(int_literals.iter().map(|&lit| Datum::int(lit))),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn not_in_str(reference: &str, str_literals: &[&str]) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Set(SetExpression::new(
            NotIn,
            Reference::new(reference),
            FnvHashSet::from_iter(str_literals.iter().map(Datum::string)),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn create_test_schema() -> Arc<Schema> {
        let table_schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "no_stats",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::required(
                    3,
                    "required",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "all_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    5,
                    "some_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    6,
                    "no_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    7,
                    "all_nans",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    8,
                    "some_nans",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    9,
                    "no_nans",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    10,
                    "all_nulls_double",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    11,
                    "all_nans_v1_stats",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    12,
                    "nan_and_null_only",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    13,
                    "no_nan_stats",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    14,
                    "some_empty",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();

        Arc::new(table_schema)
    }

    fn create_test_data_file() -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: "/test/path".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 10,
            file_size_in_bytes: 10,
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            nan_value_counts: Default::default(),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            partition_spec_id: 0,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn create_zero_records_data_file() -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: "/test/path".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 0,
            file_size_in_bytes: 10,
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            nan_value_counts: Default::default(),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            partition_spec_id: 0,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn get_test_file_1() -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: "/test/path".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 50,
            file_size_in_bytes: 10,

            value_counts: HashMap::from([
                (4, 50),
                (5, 50),
                (6, 50),
                (7, 50),
                (8, 50),
                (9, 50),
                (10, 50),
                (11, 50),
                (12, 50),
                (13, 50),
                (14, 50),
            ]),

            null_value_counts: HashMap::from([
                (4, 50),
                (5, 10),
                (6, 0),
                (10, 50),
                (11, 0),
                (12, 1),
                (14, 0),
            ]),

            nan_value_counts: HashMap::from([(7, 50), (8, 10), (9, 0)]),

            lower_bounds: HashMap::from([
                (1, Datum::int(INT_MIN_VALUE)),
                (11, Datum::float(f32::NAN)),
                (12, Datum::double(f64::NAN)),
                (14, Datum::string("")),
            ]),

            upper_bounds: HashMap::from([
                (1, Datum::int(INT_MAX_VALUE)),
                (11, Datum::float(f32::NAN)),
                (12, Datum::double(f64::NAN)),
                (14, Datum::string("房东整租霍营小区二层两居室")),
            ]),

            column_sizes: Default::default(),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            partition_spec_id: 0,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }
    fn get_test_file_2() -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: "file_2.avro".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 50,
            file_size_in_bytes: 10,

            value_counts: HashMap::from([(3, 20)]),

            null_value_counts: HashMap::from([(3, 2)]),

            nan_value_counts: HashMap::default(),

            lower_bounds: HashMap::from([(3, Datum::string("aa"))]),

            upper_bounds: HashMap::from([(3, Datum::string("dC"))]),

            column_sizes: Default::default(),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            partition_spec_id: 0,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn get_test_file_3() -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: "file_3.avro".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 50,
            file_size_in_bytes: 10,

            value_counts: HashMap::from([(3, 20)]),

            null_value_counts: HashMap::from([(3, 2)]),

            nan_value_counts: HashMap::default(),

            lower_bounds: HashMap::from([(3, Datum::string("1str1"))]),

            upper_bounds: HashMap::from([(3, Datum::string("3str3"))]),

            column_sizes: Default::default(),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            partition_spec_id: 0,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn get_test_file_4() -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: "file_4.avro".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 50,
            file_size_in_bytes: 10,

            value_counts: HashMap::from([(3, 20)]),

            null_value_counts: HashMap::from([(3, 2)]),

            nan_value_counts: HashMap::default(),

            lower_bounds: HashMap::from([(3, Datum::string("abc"))]),

            upper_bounds: HashMap::from([(3, Datum::string("イロハニホヘト"))]),

            column_sizes: Default::default(),
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            partition_spec_id: 0,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }
}
