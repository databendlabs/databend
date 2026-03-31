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
use serde_bytes::ByteBuf;

use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
use crate::expr::{BoundPredicate, BoundReference};
use crate::spec::{Datum, FieldSummary, ManifestFile, PrimitiveLiteral, Type};
use crate::{Error, ErrorKind, Result};

/// Builder for [`ManifestEvaluator`] with optional NOT rewriting capability.
#[derive(Debug)]
pub(crate) struct ManifestEvaluatorBuilder {
    partition_filter: BoundPredicate,
    rewrite_not: bool,
}

impl ManifestEvaluatorBuilder {
    /// Creates a new `ManifestEvaluatorBuilder` with the given partition filter.
    pub(crate) fn new(partition_filter: BoundPredicate) -> Self {
        Self {
            partition_filter,
            rewrite_not: false,
        }
    }

    /// Enables NOT rewriting optimization for the partition filter.
    /// When enabled, the builder will apply NOT elimination to simplify the predicate
    /// before creating the evaluator.
    #[allow(unused)]
    pub(crate) fn with_rewrite_not(mut self, rewrite_not: bool) -> Self {
        self.rewrite_not = rewrite_not;
        self
    }

    /// Builds the `ManifestEvaluator` with the configured options.
    pub(crate) fn build(self) -> ManifestEvaluator {
        let partition_filter = if self.rewrite_not {
            self.partition_filter.rewrite_not()
        } else {
            self.partition_filter
        };

        ManifestEvaluator { partition_filter }
    }
}

/// Evaluates a [`ManifestFile`] to see if the partition summaries
/// match a provided [`BoundPredicate`].
///
/// Used by [`TableScan`] to prune the list of [`ManifestFile`]s
/// in which data might be found that matches the TableScan's filter.
#[derive(Debug)]
pub(crate) struct ManifestEvaluator {
    partition_filter: BoundPredicate,
}

impl ManifestEvaluator {
    /// Creates a new `ManifestEvaluatorBuilder` for building a `ManifestEvaluator`.
    pub(crate) fn builder(partition_filter: BoundPredicate) -> ManifestEvaluatorBuilder {
        ManifestEvaluatorBuilder::new(partition_filter)
    }

    /// Evaluate this `ManifestEvaluator`'s filter predicate against the
    /// provided [`ManifestFile`]'s partitions. Used by [`TableScan`] to
    /// see if this `ManifestFile` could possibly contain data that matches
    /// the scan's filter.
    pub(crate) fn eval(&self, manifest_file: &ManifestFile) -> Result<bool> {
        match &manifest_file.partitions {
            Some(p) if !p.is_empty() => {
                let mut evaluator = ManifestFilterVisitor::new(p);
                visit(&mut evaluator, &self.partition_filter)
            }
            _ => Ok(true),
        }
    }
}

struct ManifestFilterVisitor<'a> {
    partitions: &'a Vec<FieldSummary>,
}

impl<'a> ManifestFilterVisitor<'a> {
    fn new(partitions: &'a Vec<FieldSummary>) -> Self {
        ManifestFilterVisitor { partitions }
    }
}

const ROWS_MIGHT_MATCH: Result<bool> = Ok(true);
const ROWS_CANNOT_MATCH: Result<bool> = Ok(false);
const IN_PREDICATE_LIMIT: usize = 200;

impl BoundPredicateVisitor for ManifestFilterVisitor<'_> {
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

    fn not(&mut self, _: bool) -> crate::Result<bool> {
        Err(Error::new(
            ErrorKind::Unexpected,
            "not operator is not supported in partition filter",
        ))
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        Ok(self.field_summary_for_reference(reference).contains_null)
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field = self.field_summary_for_reference(reference);

        // contains_null encodes whether at least one partition value is null,
        // lowerBound is null if all partition values are null
        if ManifestFilterVisitor::are_all_null(field, &reference.field().field_type) {
            ROWS_CANNOT_MATCH
        } else {
            ROWS_MIGHT_MATCH
        }
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field = self.field_summary_for_reference(reference);
        if let Some(contains_nan) = field.contains_nan
            && !contains_nan
        {
            return ROWS_CANNOT_MATCH;
        }

        if ManifestFilterVisitor::are_all_null(field, &reference.field().field_type) {
            return ROWS_CANNOT_MATCH;
        }

        ROWS_MIGHT_MATCH
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field = self.field_summary_for_reference(reference);
        if let Some(contains_nan) = field.contains_nan {
            // check if all values are nan
            if contains_nan && !field.contains_null && field.lower_bound.is_none() {
                return ROWS_CANNOT_MATCH;
            }
        }
        ROWS_MIGHT_MATCH
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field = self.field_summary_for_reference(reference);

        match &field.lower_bound {
            Some(bound_bytes) => {
                let bound = ManifestFilterVisitor::bytes_to_datum(
                    bound_bytes,
                    *reference.field().field_type.clone(),
                );
                if datum <= &bound {
                    ROWS_CANNOT_MATCH
                } else {
                    ROWS_MIGHT_MATCH
                }
            }
            None => ROWS_CANNOT_MATCH,
        }
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field = self.field_summary_for_reference(reference);
        match &field.lower_bound {
            Some(bound_bytes) => {
                let bound = ManifestFilterVisitor::bytes_to_datum(
                    bound_bytes,
                    *reference.field().field_type.clone(),
                );
                if datum < &bound {
                    ROWS_CANNOT_MATCH
                } else {
                    ROWS_MIGHT_MATCH
                }
            }
            None => ROWS_CANNOT_MATCH,
        }
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field = self.field_summary_for_reference(reference);
        match &field.upper_bound {
            Some(bound_bytes) => {
                let bound = ManifestFilterVisitor::bytes_to_datum(
                    bound_bytes,
                    *reference.field().field_type.clone(),
                );
                if datum >= &bound {
                    ROWS_CANNOT_MATCH
                } else {
                    ROWS_MIGHT_MATCH
                }
            }
            None => ROWS_CANNOT_MATCH,
        }
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field = self.field_summary_for_reference(reference);
        match &field.upper_bound {
            Some(bound_bytes) => {
                let bound = ManifestFilterVisitor::bytes_to_datum(
                    bound_bytes,
                    *reference.field().field_type.clone(),
                );
                if datum > &bound {
                    ROWS_CANNOT_MATCH
                } else {
                    ROWS_MIGHT_MATCH
                }
            }
            None => ROWS_CANNOT_MATCH,
        }
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field = self.field_summary_for_reference(reference);

        if field.lower_bound.is_none() || field.upper_bound.is_none() {
            return ROWS_CANNOT_MATCH;
        }

        if let Some(lower_bound_bytes) = &field.lower_bound {
            let lower_bound = ManifestFilterVisitor::bytes_to_datum(
                lower_bound_bytes,
                *reference.field().field_type.clone(),
            );
            if &lower_bound > datum {
                return ROWS_CANNOT_MATCH;
            }
        }

        if let Some(upper_bound_bytes) = &field.upper_bound {
            let upper_bound = ManifestFilterVisitor::bytes_to_datum(
                upper_bound_bytes,
                *reference.field().field_type.clone(),
            );
            if &upper_bound < datum {
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
        // because the bounds are not necessarily a min or max value, this cannot be answered using
        // them. notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
        ROWS_MIGHT_MATCH
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        datum: &Datum,
        _predicate: &BoundPredicate,
    ) -> crate::Result<bool> {
        let field = self.field_summary_for_reference(reference);

        if field.lower_bound.is_none() || field.upper_bound.is_none() {
            return ROWS_CANNOT_MATCH;
        }

        let prefix = ManifestFilterVisitor::datum_as_str(
            datum,
            "Cannot perform starts_with on non-string value",
        )?;
        let prefix_len = prefix.len();

        if let Some(lower_bound) = &field.lower_bound {
            let min_len = lower_bound.len().min(prefix_len);
            if prefix.as_bytes().lt(&lower_bound[..min_len]) {
                return ROWS_CANNOT_MATCH;
            }
        }

        if let Some(upper_bound) = &field.upper_bound {
            let min_len = upper_bound.len().min(prefix_len);
            if prefix.as_bytes().gt(&upper_bound[..min_len]) {
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
        let field = self.field_summary_for_reference(reference);

        if field.contains_null || field.lower_bound.is_none() || field.upper_bound.is_none() {
            return ROWS_MIGHT_MATCH;
        }

        let prefix = ManifestFilterVisitor::datum_as_str(
            datum,
            "Cannot perform not_starts_with on non-string value",
        )?;
        let prefix_len = prefix.len();

        // not_starts_with will match unless all values must start with the prefix. This happens when
        // the lower and upper bounds both start with the prefix.
        if let Some(lower_bound) = &field.lower_bound {
            // if lower is shorter than the prefix then lower doesn't start with the prefix
            if prefix_len > lower_bound.len() {
                return ROWS_MIGHT_MATCH;
            }

            if prefix.as_bytes().eq(&lower_bound[..prefix_len])
                && let Some(upper_bound) = &field.upper_bound
            {
                // if upper is shorter than the prefix then upper can't start with the prefix
                if prefix_len > upper_bound.len() {
                    return ROWS_MIGHT_MATCH;
                }

                if prefix.as_bytes().eq(&upper_bound[..prefix_len]) {
                    return ROWS_CANNOT_MATCH;
                }
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
        let field = self.field_summary_for_reference(reference);
        if field.lower_bound.is_none() {
            return ROWS_CANNOT_MATCH;
        }

        if literals.len() > IN_PREDICATE_LIMIT {
            return ROWS_MIGHT_MATCH;
        }

        if let Some(lower_bound) = &field.lower_bound {
            let lower_bound = ManifestFilterVisitor::bytes_to_datum(
                lower_bound,
                *reference.field().clone().field_type,
            );
            if literals.iter().all(|datum| &lower_bound > datum) {
                return ROWS_CANNOT_MATCH;
            }
        }

        if let Some(upper_bound) = &field.upper_bound {
            let upper_bound = ManifestFilterVisitor::bytes_to_datum(
                upper_bound,
                *reference.field().clone().field_type,
            );
            if literals.iter().all(|datum| &upper_bound < datum) {
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
        // because the bounds are not necessarily a min or max value, this cannot be answered using
        // them. notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in col.
        ROWS_MIGHT_MATCH
    }
}

impl ManifestFilterVisitor<'_> {
    fn field_summary_for_reference(&self, reference: &BoundReference) -> &FieldSummary {
        let pos = reference.accessor().position();
        &self.partitions[pos]
    }

    fn are_all_null(field: &FieldSummary, r#type: &Type) -> bool {
        // contains_null encodes whether at least one partition value is null,
        // lowerBound is null if all partition values are null
        let mut all_null: bool = field.contains_null && field.lower_bound.is_none();

        if all_null && r#type.is_floating_type() {
            // floating point types may include NaN values, which we check separately.
            // In case bounds don't include NaN value, contains_nan needs to be checked against.
            all_null = match field.contains_nan {
                Some(val) => !val,
                None => false,
            }
        }

        all_null
    }

    fn datum_as_str<'a>(bound: &'a Datum, err_msg: &str) -> crate::Result<&'a String> {
        let PrimitiveLiteral::String(bound) = bound.literal() else {
            return Err(Error::new(ErrorKind::Unexpected, err_msg));
        };
        Ok(bound)
    }

    fn bytes_to_datum(bytes: &ByteBuf, t: Type) -> Datum {
        let p = t.as_primitive_type().unwrap();
        Datum::try_from_bytes(bytes, p.clone()).unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Not;
    use std::sync::Arc;

    use fnv::FnvHashSet;

    use crate::Result;
    use crate::expr::visitors::manifest_evaluator::ManifestEvaluator;
    use crate::expr::{
        BinaryExpression, Bind, Predicate, PredicateOperator, Reference, SetExpression,
        UnaryExpression,
    };
    use crate::spec::{
        Datum, FieldSummary, ManifestContentType, ManifestFile, NestedField, PrimitiveType, Schema,
        SchemaRef, Type,
    };

    const INT_MIN_VALUE: i32 = 30;
    const INT_MAX_VALUE: i32 = 79;

    const STRING_MIN_VALUE: &str = "a";
    const STRING_MAX_VALUE: &str = "z";

    fn create_schema() -> Result<SchemaRef> {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "all_nulls_missing_nan",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "some_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "no_nulls",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    5,
                    "float",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    6,
                    "all_nulls_double",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    7,
                    "all_nulls_no_nans",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    8,
                    "all_nans",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    9,
                    "both_nan_and_null",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    10,
                    "no_nan_or_null",
                    Type::Primitive(PrimitiveType::Double),
                )),
                Arc::new(NestedField::optional(
                    11,
                    "all_nulls_missing_nan_float",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    12,
                    "all_same_value_or_null",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    13,
                    "no_nulls_same_value_a",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()?;

        Ok(Arc::new(schema))
    }

    fn create_partitions() -> Vec<FieldSummary> {
        vec![
            // id
            FieldSummary {
                contains_null: false,
                contains_nan: None,
                lower_bound: Some(Datum::int(INT_MIN_VALUE).to_bytes().unwrap()),
                upper_bound: Some(Datum::int(INT_MAX_VALUE).to_bytes().unwrap()),
            },
            // all_nulls_missing_nan
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: None,
                upper_bound: None,
            },
            // some_nulls
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: Some(Datum::string(STRING_MIN_VALUE).to_bytes().unwrap()),
                upper_bound: Some(Datum::string(STRING_MAX_VALUE).to_bytes().unwrap()),
            },
            // no_nulls
            FieldSummary {
                contains_null: false,
                contains_nan: None,
                lower_bound: Some(Datum::string(STRING_MIN_VALUE).to_bytes().unwrap()),
                upper_bound: Some(Datum::string(STRING_MAX_VALUE).to_bytes().unwrap()),
            },
            // float
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: Some(Datum::float(0.0).to_bytes().unwrap()),
                upper_bound: Some(Datum::float(20.0).to_bytes().unwrap()),
            },
            // all_nulls_double
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: None,
                upper_bound: None,
            },
            // all_nulls_no_nans
            FieldSummary {
                contains_null: true,
                contains_nan: Some(false),
                lower_bound: None,
                upper_bound: None,
            },
            // all_nans
            FieldSummary {
                contains_null: false,
                contains_nan: Some(true),
                lower_bound: None,
                upper_bound: None,
            },
            // both_nan_and_null
            FieldSummary {
                contains_null: true,
                contains_nan: Some(true),
                lower_bound: None,
                upper_bound: None,
            },
            // no_nan_or_null
            FieldSummary {
                contains_null: false,
                contains_nan: Some(false),
                lower_bound: Some(Datum::float(0.0).to_bytes().unwrap()),
                upper_bound: Some(Datum::float(20.0).to_bytes().unwrap()),
            },
            // all_nulls_missing_nan_float
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: None,
                upper_bound: None,
            },
            // all_same_value_or_null
            FieldSummary {
                contains_null: true,
                contains_nan: None,
                lower_bound: Some(Datum::string(STRING_MIN_VALUE).to_bytes().unwrap()),
                upper_bound: Some(Datum::string(STRING_MIN_VALUE).to_bytes().unwrap()),
            },
            // no_nulls_same_value_a
            FieldSummary {
                contains_null: false,
                contains_nan: None,
                lower_bound: Some(Datum::string(STRING_MIN_VALUE).to_bytes().unwrap()),
                upper_bound: Some(Datum::string(STRING_MIN_VALUE).to_bytes().unwrap()),
            },
        ]
    }

    fn create_manifest_file(partitions: Vec<FieldSummary>) -> ManifestFile {
        ManifestFile {
            manifest_path: "/test/path".to_string(),
            manifest_length: 0,
            partition_spec_id: 1,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 0,
            added_files_count: None,
            existing_files_count: None,
            deleted_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions: Some(partitions),
            key_metadata: None,
            first_row_id: None,
        }
    }

    #[test]
    fn test_always_true() -> Result<()> {
        let case_sensitive = false;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::AlwaysTrue.bind(schema.clone(), case_sensitive)?;

        assert!(
            ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?
        );

        Ok(())
    }

    #[test]
    fn test_always_false() -> Result<()> {
        let case_sensitive = false;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::AlwaysFalse.bind(schema.clone(), case_sensitive)?;

        assert!(
            !ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?
        );

        Ok(())
    }

    #[test]
    fn test_all_nulls() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        // all_nulls_missing_nan
        let all_nulls_missing_nan_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            Reference::new("all_nulls_missing_nan"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(all_nulls_missing_nan_filter)
                .build()
                .eval(&manifest_file)?,
            "Should skip: all nulls column with non-floating type contains all null"
        );

        // all_nulls_missing_nan_float
        let all_nulls_missing_nan_float_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            Reference::new("all_nulls_missing_nan_float"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(all_nulls_missing_nan_float_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: no NaN information may indicate presence of NaN value"
        );

        // some_nulls
        let some_nulls_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            Reference::new("some_nulls"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(some_nulls_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: column with some nulls contains a non-null value"
        );

        // no_nulls
        let no_nulls_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            Reference::new("no_nulls"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        assert!(
            ManifestEvaluator::builder(no_nulls_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: non-null column contains a non-null value"
        );

        Ok(())
    }

    #[test]
    fn test_no_nulls() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        // all_nulls_missing_nan
        let all_nulls_missing_nan_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("all_nulls_missing_nan"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(all_nulls_missing_nan_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: at least one null value in all null column"
        );

        // some_nulls
        let some_nulls_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("some_nulls"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(some_nulls_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: column with some nulls contains a null value"
        );

        // no_nulls
        let no_nulls_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("no_nulls"),
        ))
        .bind(schema.clone(), case_sensitive)?;

        assert!(
            !ManifestEvaluator::builder(no_nulls_filter)
                .build()
                .eval(&manifest_file)?,
            "Should skip: non-null column contains no null values"
        );

        // both_nan_and_null
        let both_nan_and_null_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("both_nan_and_null"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(both_nan_and_null_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: both_nan_and_null column contains no null values"
        );

        Ok(())
    }

    #[test]
    fn test_is_nan() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        // float
        let float_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("float"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(float_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: no information on if there are nan value in float column"
        );

        // all_nulls_double
        let all_nulls_double_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("all_nulls_double"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(all_nulls_double_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: no NaN information may indicate presence of NaN value"
        );

        // all_nulls_missing_nan_float
        let all_nulls_missing_nan_float_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("all_nulls_missing_nan_float"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(all_nulls_missing_nan_float_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: no NaN information may indicate presence of NaN value"
        );

        // all_nulls_no_nans
        let all_nulls_no_nans_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("all_nulls_no_nans"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(all_nulls_no_nans_filter)
                .build()
                .eval(&manifest_file)?,
            "Should skip: no nan column doesn't contain nan value"
        );

        // all_nans
        let all_nans_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("all_nans"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(all_nans_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: all_nans column contains nan value"
        );

        // both_nan_and_null
        let both_nan_and_null_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("both_nan_and_null"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(both_nan_and_null_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: both_nan_and_null column contains nan value"
        );

        // no_nan_or_null
        let no_nan_or_null_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("no_nan_or_null"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(no_nan_or_null_filter)
                .build()
                .eval(&manifest_file)?,
            "Should skip: no_nan_or_null column doesn't contain nan value"
        );

        Ok(())
    }

    #[test]
    fn test_not_nan() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        // float
        let float_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("float"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(float_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: no information on if there are nan value in float column"
        );

        // all_nulls_double
        let all_nulls_double_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("all_nulls_double"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(all_nulls_double_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: all null column contains non nan value"
        );

        // all_nulls_no_nans
        let all_nulls_no_nans_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("all_nulls_no_nans"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(all_nulls_no_nans_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: no_nans column contains non nan value"
        );

        // all_nans
        let all_nans_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("all_nans"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(all_nans_filter)
                .build()
                .eval(&manifest_file)?,
            "Should skip: all nans column doesn't contain non nan value"
        );

        // both_nan_and_null
        let both_nan_and_null_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("both_nan_and_null"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(both_nan_and_null_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: both_nan_and_null nans column contains non nan value"
        );

        // no_nan_or_null
        let no_nan_or_null_filter = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("no_nan_or_null"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(no_nan_or_null_filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: no_nan_or_null column contains non nan value"
        );

        Ok(())
    }

    #[test]
    fn test_and() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .and(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 30),
        )))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: no information on if there are nan value in float column"
        );

        Ok(())
    }

    #[test]
    fn test_or() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .or(Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MAX_VALUE + 1),
        )))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should skip: or(false, false)"
        );

        Ok(())
    }

    #[test]
    fn test_not() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .not()
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)
                .is_err(),
        );
        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .not()
        .rewrite_not()
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: not(false)"
        );

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .not()
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)
                .is_err()
        );

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .not()
        .rewrite_not()
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should skip: not(true)"
        );

        Ok(())
    }

    #[test]
    fn test_less_than() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should not read: id range below lower bound (5 < 30)"
        );

        Ok(())
    }

    #[test]
    fn test_less_than_or_eq() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should not read: id range below lower bound (5 < 30)"
        );

        Ok(())
    }

    #[test]
    fn test_greater_than() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThan,
            Reference::new("id"),
            Datum::int(INT_MAX_VALUE + 6),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should not read: id range above upper bound (85 < 79)"
        );

        Ok(())
    }

    #[test]
    fn test_greater_than_or_eq() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MAX_VALUE + 6),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should not read: id range above upper bound (85 < 79)"
        );

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Reference::new("id"),
            Datum::int(INT_MAX_VALUE),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: one possible id"
        );

        Ok(())
    }

    #[test]
    fn test_eq() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::Eq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should not read: id below lower bound"
        );

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::Eq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: id equal to lower bound"
        );

        Ok(())
    }

    #[test]
    fn test_not_eq() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotEq,
            Reference::new("id"),
            Datum::int(INT_MIN_VALUE - 25),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: id below lower bound"
        );

        Ok(())
    }

    #[test]
    fn test_in() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Set(SetExpression::new(
            PredicateOperator::In,
            Reference::new("id"),
            FnvHashSet::from_iter(vec![
                Datum::int(INT_MIN_VALUE - 25),
                Datum::int(INT_MIN_VALUE - 24),
            ]),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should not read: id below lower bound (5 < 30, 6 < 30)"
        );

        let filter = Predicate::Set(SetExpression::new(
            PredicateOperator::In,
            Reference::new("id"),
            FnvHashSet::from_iter(vec![
                Datum::int(INT_MIN_VALUE - 1),
                Datum::int(INT_MIN_VALUE),
            ]),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: id equal to lower bound (30 == 30)"
        );

        Ok(())
    }

    #[test]
    fn test_not_in() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Set(SetExpression::new(
            PredicateOperator::NotIn,
            Reference::new("id"),
            FnvHashSet::from_iter(vec![
                Datum::int(INT_MIN_VALUE - 25),
                Datum::int(INT_MIN_VALUE - 24),
            ]),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: id below lower bound (5 < 30, 6 < 30)"
        );

        Ok(())
    }

    #[test]
    fn test_starts_with() -> Result<()> {
        let case_sensitive = false;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::StartsWith,
            Reference::new("some_nulls"),
            Datum::string("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: range matches"
        );

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::StartsWith,
            Reference::new("some_nulls"),
            Datum::string("zzzz"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should skip: range doesn't match"
        );

        Ok(())
    }

    #[test]
    fn test_not_starts_with() -> Result<()> {
        let case_sensitive = false;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotStartsWith,
            Reference::new("some_nulls"),
            Datum::string("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should read: range matches"
        );

        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotStartsWith,
            Reference::new("no_nulls_same_value_a"),
            Datum::string("a"),
        ))
        .bind(schema.clone(), case_sensitive)?;
        assert!(
            !ManifestEvaluator::builder(filter)
                .build()
                .eval(&manifest_file)?,
            "Should not read: all values start with the prefix"
        );

        Ok(())
    }

    #[test]
    fn test_manifest_evaluator_builder_with_rewrite() -> Result<()> {
        let case_sensitive = true;
        let schema = create_schema()?;
        let partitions = create_partitions();
        let manifest_file = create_manifest_file(partitions);

        // Create a predicate with NOT that should be rewritten
        // NOT(id < 25) should become (id >= 25)
        let filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("id"),
            Datum::int(25), // This is less than our range [30, 79]
        ))
        .not()
        .bind(schema.clone(), case_sensitive)?;

        // Test without rewrite - should fail because NOT is not supported
        let evaluator = ManifestEvaluator::builder(filter.clone())
            .with_rewrite_not(false)
            .build();
        assert!(
            evaluator.eval(&manifest_file).is_err(),
            "Should error: NOT is not supported without rewrite"
        );

        // Test with rewrite enabled - should succeed
        let evaluator = ManifestEvaluator::builder(filter)
            .with_rewrite_not(true)
            .build();
        let result = evaluator.eval(&manifest_file)?;
        assert!(
            result,
            "Should read: NOT(id < 25) becomes (id >= 25), which matches our range [30, 79]"
        );

        // Test default behavior (no rewrite) with a simple predicate
        let simple_filter = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThan,
            Reference::new("id"),
            Datum::int(20),
        ))
        .bind(schema, case_sensitive)?;

        let evaluator = ManifestEvaluator::builder(simple_filter).build();
        assert!(
            evaluator.eval(&manifest_file)?,
            "Should read: simple predicate without NOT works by default"
        );

        Ok(())
    }
}
