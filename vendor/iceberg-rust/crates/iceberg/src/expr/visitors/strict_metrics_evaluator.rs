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
use crate::spec::{DataFile, Datum};
use crate::{Error, ErrorKind, Result};

#[allow(dead_code)]
const ROWS_MUST_MATCH: Result<bool> = Ok(true);
#[allow(dead_code)]
const ROWS_MIGHT_NOT_MATCH: Result<bool> = Ok(false);

#[allow(dead_code)]
/// Evaluates an `Expression` on a `DataFile` to test whether all rows in the file match.
///  
/// This evaluation is strict: it returns true if all rows in a file must match the expression.
/// For example, if a file's ts column has min X and max Y, this evaluator will return true for ts
/// &lt; Y+1 but not for ts &lt; Y-1.
///
/// Files are passed to `eval(DataFile)`, which returns true if all rows in the file
/// must contain matching rows and false if the file may contain rows that do not match.
pub(crate) struct StrictMetricsEvaluator<'a> {
    data_file: &'a DataFile,
}

impl<'a> StrictMetricsEvaluator<'a> {
    #[allow(dead_code)]
    fn new(data_file: &'a DataFile) -> Self {
        StrictMetricsEvaluator { data_file }
    }

    /// Evaluate this `StrictMetricsEvaluator`'s filter predicate against the
    /// provided [`DataFile`]'s metrics. Used by [`TableScan`] to
    /// see if this `DataFile` contains data that could match
    /// the scan's filter.
    #[allow(dead_code)]
    pub(crate) fn eval(filter: &'a BoundPredicate, data_file: &'a DataFile) -> crate::Result<bool> {
        if data_file.record_count == 0 {
            return ROWS_MUST_MATCH;
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

    fn may_contain_nan(&self, field_id: i32) -> bool {
        if let Some(&nan_count) = self.nan_count(field_id) {
            nan_count > 0
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

        if self.may_contain_null(field_id) || self.may_contain_nan(field_id) {
            return ROWS_MIGHT_NOT_MATCH;
        }

        let bound = if use_lower_bound {
            self.lower_bound(field_id)
        } else {
            self.upper_bound(field_id)
        };

        if let Some(bound) = bound
            && cmp_fn(bound, datum)
        {
            return ROWS_MUST_MATCH;
        }

        ROWS_MIGHT_NOT_MATCH
    }
}

impl BoundPredicateVisitor for StrictMetricsEvaluator<'_> {
    type T = bool;

    fn always_true(&mut self) -> crate::Result<bool> {
        ROWS_MUST_MATCH
    }

    fn always_false(&mut self) -> crate::Result<bool> {
        ROWS_MIGHT_NOT_MATCH
    }

    fn and(&mut self, lhs: bool, rhs: bool) -> crate::Result<bool> {
        Ok(lhs && rhs)
    }

    fn or(&mut self, lhs: bool, rhs: bool) -> crate::Result<bool> {
        Ok(lhs || rhs)
    }

    fn not(&mut self, _inner: bool) -> crate::Result<bool> {
        Err(Error::new(
            ErrorKind::DataInvalid,
            "NOT should be rewritten",
        ))
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) {
            return ROWS_MUST_MATCH;
        }

        ROWS_MIGHT_NOT_MATCH
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if let Some(&count) = self.null_count(field_id) {
            if count == 0 {
                return ROWS_MUST_MATCH;
            } else {
                return ROWS_MIGHT_NOT_MATCH;
            }
        }
        ROWS_MIGHT_NOT_MATCH
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        let contains_only = self.contains_nans_only(field_id);

        if contains_only {
            return ROWS_MUST_MATCH;
        }

        ROWS_MIGHT_NOT_MATCH
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if let Some(&nan_count) = self.nan_count(field_id)
            && nan_count == 0
        {
            return ROWS_MUST_MATCH;
        }

        if self.contains_nulls_only(field_id) {
            return ROWS_MUST_MATCH;
        }

        ROWS_MIGHT_NOT_MATCH
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::lt, false)
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::le, false)
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if let Some(lower) = self.lower_bound(field_id)
            && lower.is_nan()
        {
            return ROWS_MIGHT_NOT_MATCH;
        }

        self.visit_inequality(reference, datum, PartialOrd::gt, true)
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        self.visit_inequality(reference, datum, PartialOrd::ge, true)
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.may_contain_null(field_id) || self.may_contain_nan(field_id) {
            return ROWS_MIGHT_NOT_MATCH;
        }

        if let (Some(lower), Some(upper)) = (self.lower_bound(field_id), self.upper_bound(field_id))
        {
            // For an equality predicate to hold strictly, we must have:
            //     lower == literal.value == upper.
            if lower.literal() == datum.literal() && upper.literal() == datum.literal() {
                return ROWS_MUST_MATCH;
            } else {
                return ROWS_MIGHT_NOT_MATCH;
            }
        }

        ROWS_MIGHT_NOT_MATCH
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) || self.contains_nans_only(field_id) {
            return ROWS_MUST_MATCH;
        }

        if let Some(lower) = self.lower_bound(field_id) {
            if lower.is_nan() {
                return ROWS_MIGHT_NOT_MATCH;
            }
            if lower.literal() > datum.literal() {
                return ROWS_MUST_MATCH;
            }
        }

        if let Some(upper) = self.upper_bound(field_id) {
            if upper.is_nan() {
                return ROWS_MIGHT_NOT_MATCH;
            }
            if upper.literal() < datum.literal() {
                return ROWS_MUST_MATCH;
            }
        }

        ROWS_MIGHT_NOT_MATCH
    }

    fn starts_with(
        &mut self,
        _reference: &BoundReference,
        _datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        ROWS_MIGHT_NOT_MATCH
    }

    fn not_starts_with(
        &mut self,
        _reference: &BoundReference,
        _datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        ROWS_MIGHT_NOT_MATCH
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.may_contain_null(field_id) || self.may_contain_nan(field_id) {
            return ROWS_MIGHT_NOT_MATCH;
        }

        if let (Some(lower), Some(upper)) = (self.lower_bound(field_id), self.upper_bound(field_id))
        {
            if !literals.contains(lower) || !literals.contains(upper) || lower != upper {
                return ROWS_MIGHT_NOT_MATCH;
            }

            return ROWS_MUST_MATCH;
        }

        ROWS_MIGHT_NOT_MATCH
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field_id = reference.field().id;

        if self.contains_nulls_only(field_id) || self.contains_nans_only(field_id) {
            return ROWS_MUST_MATCH;
        }

        let mut filtered_literals = literals.clone();

        if let Some(lower) = self.lower_bound(field_id) {
            if lower.is_nan() {
                return ROWS_MIGHT_NOT_MATCH;
            }

            filtered_literals.retain(|val| lower <= val);
            if filtered_literals.is_empty() {
                return ROWS_MUST_MATCH;
            }
        }

        if let Some(upper) = self.upper_bound(field_id) {
            filtered_literals.retain(|val| *val <= *upper);
            if filtered_literals.is_empty() {
                return ROWS_MUST_MATCH;
            }
        }

        ROWS_MIGHT_NOT_MATCH
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
    use crate::expr::visitors::strict_metrics_evaluator::StrictMetricsEvaluator;
    use crate::expr::{
        BinaryExpression, Bind, BoundPredicate, Predicate, Reference, SetExpression,
        UnaryExpression,
    };
    use crate::spec::{
        DataContentType, DataFile, DataFileFormat, Datum, NestedField, PrimitiveType, Schema,
        Struct, Type,
    };

    const INT_MIN_VALUE: i32 = 30;
    const INT_MAX_VALUE: i32 = 79;

    // Helper: Create a test schema.
    fn create_test_schema() -> Arc<Schema> {
        let table_schema = Schema::builder()
            .with_fields(vec![
                // field id=1: "id" (Int)
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                // field id=2: "no_stats" (Int)
                Arc::new(NestedField::optional(
                    2,
                    "no_stats",
                    Type::Primitive(PrimitiveType::Int),
                )),
                // field id=3: "required" (String)
                Arc::new(NestedField::required(
                    3,
                    "required",
                    Type::Primitive(PrimitiveType::String),
                )),
                // field id=4: "all_nulls" (String)
                Arc::new(NestedField::optional(
                    4,
                    "all_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                // field id=5: "some_nulls" (String)
                Arc::new(NestedField::optional(
                    5,
                    "some_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                // field id=6: "no_nulls" (String)
                Arc::new(NestedField::optional(
                    6,
                    "no_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                // field id=7: "all_nans" (Double)
                Arc::new(NestedField::optional(
                    7,
                    "all_nans",
                    Type::Primitive(PrimitiveType::Double),
                )),
                // field id=8: "some_nans" (Float)
                Arc::new(NestedField::optional(
                    8,
                    "some_nans",
                    Type::Primitive(PrimitiveType::Float),
                )),
                // field id=9: "no_nans" (Float)
                Arc::new(NestedField::optional(
                    9,
                    "no_nans",
                    Type::Primitive(PrimitiveType::Float),
                )),
                // field id=10: "all_nulls_double" (Double)
                Arc::new(NestedField::optional(
                    10,
                    "all_nulls_double",
                    Type::Primitive(PrimitiveType::Double),
                )),
                // field id=11: "all_nans_v1_stats" (Float)
                Arc::new(NestedField::optional(
                    11,
                    "all_nans_v1_stats",
                    Type::Primitive(PrimitiveType::Float),
                )),
                // field id=12: "nan_and_null_only" (Double)
                Arc::new(NestedField::optional(
                    12,
                    "nan_and_null_only",
                    Type::Primitive(PrimitiveType::Double),
                )),
                // field id=13: "no_nan_stats" (Double)
                Arc::new(NestedField::optional(
                    13,
                    "no_nan_stats",
                    Type::Primitive(PrimitiveType::Double),
                )),
                // field id=14: "some_empty" (String)
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

    fn get_test_file_1() -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: "/test/path".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 50,
            file_size_in_bytes: 10,
            value_counts: HashMap::from([
                (1, 50),
                (4, 50),
                (5, 50),
                (6, 50),
                (7, 50),
                (8, 50),
                (9, 50),
                (10, 50),
                (11, 50),
                (12, 50),
                (14, 50),
            ]),
            null_value_counts: HashMap::from([
                (1, 0),
                (4, 50),
                (5, 10),
                (6, 0),
                (10, 50),
                (11, 0),
                (12, 1),
            ]),
            nan_value_counts: HashMap::from([(1, 0), (7, 50), (8, 10), (9, 0), (11, 50)]),
            lower_bounds: HashMap::from([
                (1, Datum::int(INT_MIN_VALUE)), // id lower bound = 30
                (11, Datum::float(f32::NAN)),
                (12, Datum::double(f64::NAN)),
                (14, Datum::string("")),
            ]),
            upper_bounds: HashMap::from([
                (1, Datum::int(INT_MAX_VALUE)), // id upper bound = 79
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

    fn create_zero_records_data_file() -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: "/test/zero".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 0,
            file_size_in_bytes: 10,
            column_sizes: HashMap::new(),
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

    fn get_test_file_eq() -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: "/test/path_eq".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: Struct::empty(),
            record_count: 10,
            file_size_in_bytes: 10,
            value_counts: HashMap::from([(1, 10)]),
            null_value_counts: HashMap::from([(1, 0)]),
            nan_value_counts: HashMap::from([(1, 0)]),
            lower_bounds: HashMap::from([(1, Datum::int(42))]),
            upper_bounds: HashMap::from([(1, Datum::int(42))]),
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

    // For string test files we reuse the ones from inclusive tests.
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

    // Helper functions to bind predicates with the test schema and then evaluate using StrictMetricsEvaluator.
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

    fn less_than(reference: &str, literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            LessThan,
            Reference::new(reference),
            Datum::string(literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn less_than_or_equal(reference: &str, literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            LessThanOrEq,
            Reference::new(reference),
            Datum::string(literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn greater_than(reference: &str, literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            GreaterThan,
            Reference::new(reference),
            Datum::string(literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn greater_than_or_equal(reference: &str, literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            GreaterThanOrEq,
            Reference::new(reference),
            Datum::string(literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn equal(reference: &str, literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            Eq,
            Reference::new(reference),
            Datum::string(literal),
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

    fn starts_with(reference: &str, literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            StartsWith,
            Reference::new(reference),
            Datum::string(literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn not_starts_with(reference: &str, literal: &str) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Binary(BinaryExpression::new(
            NotStartsWith,
            Reference::new(reference),
            Datum::string(literal),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn in_int(reference: &str, int_literals: &[i32]) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Set(SetExpression::new(
            In,
            Reference::new(reference),
            FnvHashSet::from_iter(int_literals.iter().copied().map(Datum::int)),
        ));
        filter.bind(schema.clone(), true).unwrap()
    }

    fn not_in_int(reference: &str, int_literals: &[i32]) -> BoundPredicate {
        let schema = create_test_schema();
        let filter = Predicate::Set(SetExpression::new(
            NotIn,
            Reference::new(reference),
            FnvHashSet::from_iter(int_literals.iter().copied().map(Datum::int)),
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

    #[test]
    fn test_data_file_no_partitions() {
        let schema = create_test_schema();
        let partition_filter = Predicate::AlwaysTrue.bind(schema.clone(), false).unwrap();

        let data_file = get_test_file_1();

        let result = StrictMetricsEvaluator::eval(&partition_filter, &data_file).unwrap();
        assert!(result, "Should read: AlwaysTrue predicate");
    }

    #[test]
    fn test_all_nulls() {
        let file = get_test_file_1();

        // "all_nulls" (field 4) is all null.
        let result = StrictMetricsEvaluator::eval(&not_null("all_nulls"), &file).unwrap();
        assert!(!result, "Should skip: notNull on all-null column");

        let result = StrictMetricsEvaluator::eval(&less_than("all_nulls", "a"), &file).unwrap();
        assert!(!result, "Should skip: lessThan on all-null column");

        let result =
            StrictMetricsEvaluator::eval(&less_than_or_equal("all_nulls", "a"), &file).unwrap();
        assert!(!result, "Should skip: lessThanOrEqual on all-null column");

        let result = StrictMetricsEvaluator::eval(&greater_than("all_nulls", "a"), &file).unwrap();
        assert!(!result, "Should skip: greaterThan on all-null column");

        let result =
            StrictMetricsEvaluator::eval(&greater_than_or_equal("all_nulls", "a"), &file).unwrap();
        assert!(
            !result,
            "Should skip: greaterThanOrEqual on all-null column"
        );

        let result = StrictMetricsEvaluator::eval(&equal("all_nulls", "a"), &file).unwrap();
        assert!(!result, "Should skip: equal on all-null column");

        let result = StrictMetricsEvaluator::eval(&starts_with("all_nulls", "a"), &file).unwrap();
        assert!(!result, "Strict eval: startsWith always returns false");

        let result =
            StrictMetricsEvaluator::eval(&not_starts_with("all_nulls", "a"), &file).unwrap();
        assert!(!result, "Strict eval: notStartsWith always returns false");

        // "some_nulls" (field 5) has some nulls.
        let result = StrictMetricsEvaluator::eval(&not_null("some_nulls"), &file).unwrap();
        assert!(!result, "Should skip: notNull on column with some nulls");

        // "no_nulls" (field 6) has no nulls.
        let result = StrictMetricsEvaluator::eval(&not_null("no_nulls"), &file).unwrap();
        assert!(result, "Should read: notNull on column with no nulls");
    }

    #[test]
    fn test_no_nulls() {
        let file = get_test_file_1();

        // "all_nulls" is all null so isNull returns MUST_MATCH.
        let result = StrictMetricsEvaluator::eval(&is_null("all_nulls"), &file).unwrap();
        assert!(result, "Should read: isNull on all-null column");

        // "some_nulls" is not all null.
        let result = StrictMetricsEvaluator::eval(&is_null("some_nulls"), &file).unwrap();
        assert!(
            !result,
            "Should skip: isNull on column with some non-null values"
        );

        // "no_nulls" has no nulls.
        let result = StrictMetricsEvaluator::eval(&is_null("no_nulls"), &file).unwrap();
        assert!(!result, "Should skip: isNull on column with no nulls");
    }

    #[test]
    fn test_is_nan() {
        let file = get_test_file_1();

        // "all_nans" (field 7) is all NaN.
        let result = StrictMetricsEvaluator::eval(&is_nan("all_nans"), &file).unwrap();
        assert!(result, "Should read: isNan on all-NaN column");

        // "some_nans" (field 8) has some NaN.
        let result = StrictMetricsEvaluator::eval(&is_nan("some_nans"), &file).unwrap();
        assert!(!result, "Should skip: isNan on column with some NaNs");

        // "no_nans" (field 9) has no NaN.
        let result = StrictMetricsEvaluator::eval(&is_nan("no_nans"), &file).unwrap();
        assert!(!result, "Should skip: isNan on column with no NaNs");

        // "all_nulls_double" (field 10) is all null.
        let result = StrictMetricsEvaluator::eval(&is_nan("all_nulls_double"), &file).unwrap();
        assert!(!result, "Should skip: isNan on all-null double column");

        // "no_nan_stats" (field 13) missing stats → cannot guarantee, so false.
        let result = StrictMetricsEvaluator::eval(&is_nan("no_nan_stats"), &file).unwrap();
        assert!(!result, "Should skip: isNan when stats are missing");

        // "all_nans_v1_stats" (field 11) is all NaN.
        let result = StrictMetricsEvaluator::eval(&is_nan("all_nans_v1_stats"), &file).unwrap();
        assert!(result, "Should read: isNan on all-NaN (v1 stats) column");

        // "nan_and_null_only" (field 12) → mixed, so false.
        let result = StrictMetricsEvaluator::eval(&is_nan("nan_and_null_only"), &file).unwrap();
        assert!(!result, "Should skip: isNan on nan-and-null-only column");
    }

    #[test]
    fn test_not_nan() {
        let file = get_test_file_1();

        // "all_nans" → notNan returns MIGHT_NOT_MATCH.
        let result = StrictMetricsEvaluator::eval(&not_nan("all_nans"), &file).unwrap();
        assert!(
            !result,
            "Should read: notNan on all-NaN column (strict: must match)"
        );

        // "some_nans" → returns false.
        let result = StrictMetricsEvaluator::eval(&not_nan("some_nans"), &file).unwrap();
        assert!(!result, "Should skip: notNan on column with some NaNs");

        // "no_nans" → notNan returns MUST_MATCH.
        let result = StrictMetricsEvaluator::eval(&not_nan("no_nans"), &file).unwrap();
        assert!(result, "Should read: notNan on column with no NaNs");

        // "all_nulls_double" → returns MUST_MATCH due to all nulls.
        let result = StrictMetricsEvaluator::eval(&not_nan("all_nulls_double"), &file).unwrap();
        assert!(result, "Should read: notNan on all-null double column");

        // "no_nan_stats" → missing stats so returns false.
        let result = StrictMetricsEvaluator::eval(&not_nan("no_nan_stats"), &file).unwrap();
        assert!(!result, "Should skip: notNan when stats are missing");

        // "all_nans_v1_stats" → returns false.
        let result = StrictMetricsEvaluator::eval(&not_nan("all_nans_v1_stats"), &file).unwrap();
        assert!(!result, "Should read: notNan on all-NaN (v1 stats) column");

        // "nan_and_null_only" → returns false.
        let result = StrictMetricsEvaluator::eval(&not_nan("nan_and_null_only"), &file).unwrap();
        assert!(!result, "Should skip: notNan on nan-and-null-only column");
    }

    #[test]
    #[should_panic]
    fn test_missing_column() {
        let _ = StrictMetricsEvaluator::eval(&less_than("missing", "a"), &get_test_file_1());
    }

    #[test]
    fn test_zero_record_file() {
        let file = create_zero_records_data_file();

        let expressions = [
            less_than_int("no_stats", 5),
            less_than_or_equal_int("no_stats", 30),
            equal_int("no_stats", 70),
            greater_than_int("no_stats", 78),
            greater_than_or_equal_int("no_stats", 90),
            not_equal_int("no_stats", 101),
            is_null("no_stats"),
            not_null("no_stats"),
        ];

        for expr in expressions {
            let result = StrictMetricsEvaluator::eval(&expr, &file).unwrap();
            // For zero-record files, strict eval returns MUST_MATCH.
            assert!(
                result,
                "Strict eval: Should read zero-record file for expression {expr:?}"
            );
        }
    }

    #[test]
    fn test_not() {
        let file = get_test_file_1();

        let result =
            StrictMetricsEvaluator::eval(&not_less_than_int("id", INT_MIN_VALUE - 25), &file);
        assert!(result.is_err());
    }

    #[test]
    fn test_and() {
        let schema = create_test_schema();

        // (id < (INT_MIN_VALUE - 25)) AND (id >= (INT_MAX_VALUE + 1))
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
        let bound = filter.bind(schema.clone(), true).unwrap();
        let result = StrictMetricsEvaluator::eval(&bound, &get_test_file_1()).unwrap();
        assert!(!result, "Strict eval: and(false, false) should be false");

        // (id > (INT_MIN_VALUE - 1)) AND (id <= (INT_MAX_VALUE + 1))
        let filter = Predicate::Binary(BinaryExpression::new(
            GreaterThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 1),
        ))
        .and(Predicate::Binary(BinaryExpression::new(
            LessThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MAX_VALUE + 1),
        )));
        let bound = filter.bind(schema.clone(), true).unwrap();
        let result = StrictMetricsEvaluator::eval(&bound, &get_test_file_1()).unwrap();
        assert!(result, "Strict eval: and(true, true) should be true");
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
        let bound = filter.bind(schema.clone(), true).unwrap();
        let result = StrictMetricsEvaluator::eval(&bound, &get_test_file_1()).unwrap();
        assert!(result, "Strict eval: or(false, true) should be true");

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
        let bound = filter.bind(schema.clone(), true).unwrap();
        let result = StrictMetricsEvaluator::eval(&bound, &get_test_file_1()).unwrap();
        assert!(!result, "Strict eval: or(false, false) should be false");
    }

    #[test]
    fn test_integer_lt() {
        let file = get_test_file_1();

        let result =
            StrictMetricsEvaluator::eval(&less_than_int("id", INT_MIN_VALUE - 25), &file).unwrap();
        assert!(
            !result,
            "Strict eval: id < {} should be false",
            INT_MIN_VALUE - 25
        );

        let result =
            StrictMetricsEvaluator::eval(&less_than_int("id", INT_MIN_VALUE), &file).unwrap();
        assert!(!result, "Strict eval: id < {INT_MIN_VALUE} should be false");

        let result =
            StrictMetricsEvaluator::eval(&less_than_int("id", INT_MIN_VALUE + 1), &file).unwrap();
        assert!(
            !result,
            "Strict eval: id < {} should be false",
            INT_MIN_VALUE + 1
        );

        let result =
            StrictMetricsEvaluator::eval(&less_than_int("id", INT_MAX_VALUE), &file).unwrap();
        assert!(!result, "Strict eval: id < {INT_MAX_VALUE} should be false");

        let result =
            StrictMetricsEvaluator::eval(&less_than_int("id", INT_MAX_VALUE + 1), &file).unwrap();
        assert!(
            result,
            "Strict eval: id < {} should be true",
            INT_MAX_VALUE + 1
        );
    }

    #[test]
    fn test_integer_lt_eq() {
        let file = get_test_file_1();

        let result =
            StrictMetricsEvaluator::eval(&less_than_or_equal_int("id", INT_MIN_VALUE - 25), &file)
                .unwrap();
        assert!(
            !result,
            "Strict eval: id <= {} should be false",
            INT_MIN_VALUE - 25
        );

        let result =
            StrictMetricsEvaluator::eval(&less_than_or_equal_int("id", INT_MIN_VALUE - 1), &file)
                .unwrap();
        assert!(
            !result,
            "Strict eval: id <= {} should be false",
            INT_MIN_VALUE - 1
        );

        let result =
            StrictMetricsEvaluator::eval(&less_than_or_equal_int("id", INT_MIN_VALUE), &file)
                .unwrap();
        assert!(
            !result,
            "Strict eval: id <= {INT_MIN_VALUE} should be false"
        );

        let result =
            StrictMetricsEvaluator::eval(&less_than_or_equal_int("id", INT_MAX_VALUE), &file)
                .unwrap();
        assert!(result, "Strict eval: id <= {INT_MAX_VALUE} should be true");

        let result =
            StrictMetricsEvaluator::eval(&less_than_or_equal_int("id", INT_MAX_VALUE + 1), &file)
                .unwrap();
        assert!(
            result,
            "Strict eval: id <= {} should be true",
            INT_MAX_VALUE + 1
        );
    }

    #[test]
    fn test_integer_gt() {
        let file = get_test_file_1();

        let result =
            StrictMetricsEvaluator::eval(&greater_than_int("id", INT_MAX_VALUE + 6), &file)
                .unwrap();
        assert!(
            !result,
            "Strict eval: id > {} should be false",
            INT_MAX_VALUE + 6
        );

        let result =
            StrictMetricsEvaluator::eval(&greater_than_int("id", INT_MAX_VALUE), &file).unwrap();
        assert!(!result, "Strict eval: id > {INT_MAX_VALUE} should be false");

        let result =
            StrictMetricsEvaluator::eval(&greater_than_int("id", INT_MIN_VALUE), &file).unwrap();
        assert!(!result, "Strict eval: id > {INT_MIN_VALUE} should be false");

        let result =
            StrictMetricsEvaluator::eval(&greater_than_int("id", INT_MIN_VALUE - 1), &file)
                .unwrap();
        assert!(
            result,
            "Strict eval: id > {} should be true",
            INT_MIN_VALUE - 1
        );

        let result =
            StrictMetricsEvaluator::eval(&greater_than_int("id", INT_MIN_VALUE - 4), &file)
                .unwrap();
        assert!(
            result,
            "Strict eval: id > {} should be true",
            INT_MIN_VALUE - 4
        );
    }

    #[test]
    fn test_integer_gt_eq() {
        let file = get_test_file_1();

        let result = StrictMetricsEvaluator::eval(
            &greater_than_or_equal_int("id", INT_MAX_VALUE + 6),
            &file,
        )
        .unwrap();
        assert!(
            !result,
            "Strict eval: id >= {} should be false",
            INT_MAX_VALUE + 6
        );

        let result = StrictMetricsEvaluator::eval(
            &greater_than_or_equal_int("id", INT_MAX_VALUE + 1),
            &file,
        )
        .unwrap();
        assert!(
            !result,
            "Strict eval: id >= {} should be false",
            INT_MAX_VALUE + 1
        );

        let result =
            StrictMetricsEvaluator::eval(&greater_than_or_equal_int("id", INT_MAX_VALUE), &file)
                .unwrap();
        assert!(
            !result,
            "Strict eval: id >= {INT_MAX_VALUE} should be false"
        );

        let result =
            StrictMetricsEvaluator::eval(&greater_than_or_equal_int("id", INT_MIN_VALUE), &file)
                .unwrap();
        assert!(result, "Strict eval: id >= {INT_MIN_VALUE} should be true");

        let result = StrictMetricsEvaluator::eval(
            &greater_than_or_equal_int("id", INT_MIN_VALUE - 1),
            &file,
        )
        .unwrap();
        assert!(
            result,
            "Strict eval: id >= {} should be true",
            INT_MIN_VALUE - 1
        );
    }

    #[test]
    fn test_integer_eq() {
        let file = get_test_file_1();

        let result = StrictMetricsEvaluator::eval(&equal_int("id", INT_MIN_VALUE), &file).unwrap();
        assert!(
            !result,
            "Strict eval: equal should be false if bounds are not identical"
        );

        let eq_file = get_test_file_eq();
        let result = StrictMetricsEvaluator::eval(&equal_int("id", 42), &eq_file).unwrap();
        assert!(
            result,
            "Strict eval: equal should be true when lower == upper == literal"
        );

        let result = StrictMetricsEvaluator::eval(&equal_int("id", 41), &eq_file).unwrap();
        assert!(
            !result,
            "Strict eval: equal should be false for non-matching literal"
        );
    }

    #[test]
    fn test_integer_not_eq() {
        let file = get_test_file_1();

        let result =
            StrictMetricsEvaluator::eval(&not_equal_int("id", INT_MIN_VALUE - 25), &file).unwrap();
        assert!(
            result,
            "Strict eval: notEqual should be true when lower bound > literal"
        );

        let result =
            StrictMetricsEvaluator::eval(&not_equal_int("id", INT_MIN_VALUE - 1), &file).unwrap();
        assert!(
            result,
            "Strict eval: notEqual should be true when lower bound > literal"
        );

        let result =
            StrictMetricsEvaluator::eval(&not_equal_int("id", INT_MIN_VALUE), &file).unwrap();
        assert!(
            !result,
            "Strict eval: notEqual should be false when literal equals lower bound (but upper is different)"
        );

        let result =
            StrictMetricsEvaluator::eval(&not_equal_int("id", INT_MAX_VALUE - 4), &file).unwrap();
        assert!(
            !result,
            "Strict eval: notEqual should be false when literal is between bounds"
        );

        let result =
            StrictMetricsEvaluator::eval(&not_equal_int("id", INT_MAX_VALUE), &file).unwrap();
        assert!(
            !result,
            "Strict eval: notEqual should be false when literal equals upper bound"
        );

        let result =
            StrictMetricsEvaluator::eval(&not_equal_int("id", INT_MAX_VALUE + 1), &file).unwrap();
        assert!(
            result,
            "Strict eval: notEqual should be true when upper bound < literal"
        );

        let result =
            StrictMetricsEvaluator::eval(&not_equal_int("id", INT_MAX_VALUE + 6), &file).unwrap();
        assert!(
            result,
            "Strict eval: notEqual should be true when literal is well above upper bound"
        );

        let eq_file = get_test_file_eq();
        let result = StrictMetricsEvaluator::eval(&not_equal_int("id", 42), &eq_file).unwrap();
        assert!(
            !result,
            "Strict eval: notEqual should be false when literal equals the only value"
        );

        let result = StrictMetricsEvaluator::eval(&not_equal_int("id", 41), &eq_file).unwrap();
        assert!(
            result,
            "Strict eval: notEqual should be true when literal does not equal the only value"
        );
    }

    #[test]
    #[should_panic]
    fn test_case_sensitive_integer_not_eq_rewritten() {
        let _ = StrictMetricsEvaluator::eval(&equal_int_not("ID", 5), &get_test_file_1()).unwrap();
    }

    #[test]
    fn test_string_starts_with() {
        let file1 = get_test_file_1();
        let file2 = get_test_file_2();

        let result = StrictMetricsEvaluator::eval(&starts_with("required", "a"), &file1).unwrap();
        assert!(
            !result,
            "strict eval: startsWith always false (no metrics support)"
        );
        let result = StrictMetricsEvaluator::eval(&starts_with("required", "a"), &file2).unwrap();
        assert!(!result, "strict eval: startsWith always false");
    }

    #[test]
    fn test_string_not_starts_with() {
        let file1 = get_test_file_1();
        let file2 = get_test_file_2();

        let result =
            StrictMetricsEvaluator::eval(&not_starts_with("required", "a"), &file1).unwrap();
        assert!(!result, "Strict eval: notStartsWith always false");
        let result =
            StrictMetricsEvaluator::eval(&not_starts_with("required", "a"), &file2).unwrap();
        assert!(!result, "Strict eval: notStartsWith always false");
    }

    #[test]
    fn test_integer_in() {
        let file = get_test_file_1();

        let result = StrictMetricsEvaluator::eval(
            &in_int("id", &[INT_MIN_VALUE - 25, INT_MIN_VALUE - 24]),
            &file,
        )
        .unwrap();
        assert!(
            !result,
            "Strict eval: inInt on file1 returns false because bounds are not equal"
        );

        let result =
            StrictMetricsEvaluator::eval(&in_int("id", &[INT_MIN_VALUE - 1, INT_MIN_VALUE]), &file)
                .unwrap();
        assert!(
            !result,
            "Strict eval: inInt on file1 returns false when only one bound is in set"
        );

        // For file with equality stats.
        let eq_file = get_test_file_eq();
        let result = StrictMetricsEvaluator::eval(&in_int("id", &[42]), &eq_file).unwrap();
        assert!(
            result,
            "Strict eval: inInt should be true when both bounds equal literal in set"
        );

        let result =
            StrictMetricsEvaluator::eval(&in_int("id", &[INT_MAX_VALUE, INT_MAX_VALUE + 1]), &file)
                .unwrap();
        assert!(
            !result,
            "Strict eval: inInt on file1 returns false due to unequal bounds"
        );
    }

    #[test]
    fn test_integer_not_in() {
        let file = get_test_file_1();

        let result = StrictMetricsEvaluator::eval(
            &not_in_int("id", &[INT_MIN_VALUE - 25, INT_MIN_VALUE - 24]),
            &file,
        )
        .unwrap();
        assert!(
            result,
            "Strict eval: notInInt should be true when all provided literals are outside bounds"
        );

        let result = StrictMetricsEvaluator::eval(
            &not_in_int("id", &[INT_MIN_VALUE - 2, INT_MIN_VALUE - 1]),
            &file,
        )
        .unwrap();
        assert!(
            result,
            "Strict eval: notInInt should be true when literals are below lower bound"
        );

        let result = StrictMetricsEvaluator::eval(
            &not_in_int("id", &[INT_MIN_VALUE - 1, INT_MIN_VALUE]),
            &file,
        )
        .unwrap();
        assert!(
            !result,
            "Strict eval: notInInt should be false when at least one literal falls within bounds"
        );

        let result = StrictMetricsEvaluator::eval(
            &not_in_int("id", &[INT_MAX_VALUE - 4, INT_MAX_VALUE - 3]),
            &file,
        )
        .unwrap();
        assert!(
            !result,
            "Strict eval: notInInt should be false when some literals are within bounds"
        );

        let result = StrictMetricsEvaluator::eval(
            &not_in_int("id", &[INT_MAX_VALUE, INT_MAX_VALUE + 1]),
            &file,
        )
        .unwrap();
        assert!(
            !result,
            "Strict eval: not_in_int should be false when one literal is within bounds"
        );

        let result = StrictMetricsEvaluator::eval(
            &not_in_int("id", &[INT_MAX_VALUE + 1, INT_MAX_VALUE + 2]),
            &file,
        )
        .unwrap();
        assert!(
            result,
            "Strict eval: notInInt should be true when all literals are above upper bound"
        );

        let result = StrictMetricsEvaluator::eval(
            &not_in_int("id", &[INT_MAX_VALUE + 6, INT_MAX_VALUE + 7]),
            &file,
        )
        .unwrap();
        assert!(
            result,
            "Strict eval: notInInt should be true when literals are well above upper bound"
        );

        let result =
            StrictMetricsEvaluator::eval(&not_in_str("all_nulls", &["abc", "def"]), &file).unwrap();
        assert!(
            result,
            "Strict eval: notInStr on all-null column should be true"
        );

        let result =
            StrictMetricsEvaluator::eval(&not_in_str("some_nulls", &["abc", "def"]), &file)
                .unwrap();
        assert!(
            !result,
            "Strict eval: notInStr on column start with nan should be false"
        );
    }
}
