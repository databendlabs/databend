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

//! Term definition.

use std::fmt::{Display, Formatter};

use fnv::FnvHashSet;
use serde::{Deserialize, Serialize};

use crate::expr::accessor::{StructAccessor, StructAccessorRef};
use crate::expr::{
    BinaryExpression, Bind, Predicate, PredicateOperator, SetExpression, UnaryExpression,
};
use crate::spec::{Datum, NestedField, NestedFieldRef, SchemaRef};
use crate::{Error, ErrorKind};

/// Unbound term before binding to a schema.
pub type Term = Reference;

/// A named reference in an unbound expression.
/// For example, `a` in `a > 10`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reference {
    name: String,
}

impl Reference {
    /// Create a new unbound reference.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Return the name of this reference.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Reference {
    /// Creates an less than expression. For example, `a < 10`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").less_than(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a < 10");
    /// ```
    pub fn less_than(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            self,
            datum,
        ))
    }

    /// Creates an less than or equal to expression. For example, `a <= 10`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").less_than_or_equal_to(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a <= 10");
    /// ```
    pub fn less_than_or_equal_to(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThanOrEq,
            self,
            datum,
        ))
    }

    /// Creates an greater than expression. For example, `a > 10`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").greater_than(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a > 10");
    /// ```
    pub fn greater_than(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThan,
            self,
            datum,
        ))
    }

    /// Creates a greater-than-or-equal-to than expression. For example, `a >= 10`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").greater_than_or_equal_to(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a >= 10");
    /// ```
    pub fn greater_than_or_equal_to(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            self,
            datum,
        ))
    }

    /// Creates an equal-to expression. For example, `a = 10`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").equal_to(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a = 10");
    /// ```
    pub fn equal_to(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(PredicateOperator::Eq, self, datum))
    }

    /// Creates a not equal-to expression. For example, `a!= 10`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").not_equal_to(Datum::long(10));
    ///
    /// assert_eq!(&format!("{expr}"), "a != 10");
    /// ```
    pub fn not_equal_to(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(PredicateOperator::NotEq, self, datum))
    }

    /// Creates a start-with expression. For example, `a STARTS WITH "foo"`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").starts_with(Datum::string("foo"));
    ///
    /// assert_eq!(&format!("{expr}"), r#"a STARTS WITH "foo""#);
    /// ```
    pub fn starts_with(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::StartsWith,
            self,
            datum,
        ))
    }

    /// Creates a not start-with expression. For example, `a NOT STARTS WITH 'foo'`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    ///
    /// let expr = Reference::new("a").not_starts_with(Datum::string("foo"));
    ///
    /// assert_eq!(&format!("{expr}"), r#"a NOT STARTS WITH "foo""#);
    /// ```
    pub fn not_starts_with(self, datum: Datum) -> Predicate {
        Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotStartsWith,
            self,
            datum,
        ))
    }

    /// Creates an is-nan expression. For example, `a IS NAN`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").is_nan();
    ///
    /// assert_eq!(&format!("{expr}"), "a IS NAN");
    /// ```
    pub fn is_nan(self) -> Predicate {
        Predicate::Unary(UnaryExpression::new(PredicateOperator::IsNan, self))
    }

    /// Creates an is-not-nan expression. For example, `a IS NOT NAN`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").is_not_nan();
    ///
    /// assert_eq!(&format!("{expr}"), "a IS NOT NAN");
    /// ```
    pub fn is_not_nan(self) -> Predicate {
        Predicate::Unary(UnaryExpression::new(PredicateOperator::NotNan, self))
    }

    /// Creates an is-null expression. For example, `a IS NULL`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").is_null();
    ///
    /// assert_eq!(&format!("{expr}"), "a IS NULL");
    /// ```
    pub fn is_null(self) -> Predicate {
        Predicate::Unary(UnaryExpression::new(PredicateOperator::IsNull, self))
    }

    /// Creates an is-not-null expression. For example, `a IS NOT NULL`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").is_not_null();
    ///
    /// assert_eq!(&format!("{expr}"), "a IS NOT NULL");
    /// ```
    pub fn is_not_null(self) -> Predicate {
        Predicate::Unary(UnaryExpression::new(PredicateOperator::NotNull, self))
    }

    /// Creates an is-in expression. For example, `a IN (5, 6)`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use fnv::FnvHashSet;
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").is_in([Datum::long(5), Datum::long(6)]);
    ///
    /// let as_string = format!("{expr}");
    /// assert!(&as_string == "a IN (5, 6)" || &as_string == "a IN (6, 5)");
    /// ```
    pub fn is_in(self, literals: impl IntoIterator<Item = Datum>) -> Predicate {
        Predicate::Set(SetExpression::new(
            PredicateOperator::In,
            self,
            FnvHashSet::from_iter(literals),
        ))
    }

    /// Creates an is-not-in expression. For example, `a NOT IN (5, 6)`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use fnv::FnvHashSet;
    /// use iceberg::expr::Reference;
    /// use iceberg::spec::Datum;
    /// let expr = Reference::new("a").is_not_in([Datum::long(5), Datum::long(6)]);
    ///
    /// let as_string = format!("{expr}");
    /// assert!(&as_string == "a NOT IN (5, 6)" || &as_string == "a NOT IN (6, 5)");
    /// ```
    pub fn is_not_in(self, literals: impl IntoIterator<Item = Datum>) -> Predicate {
        Predicate::Set(SetExpression::new(
            PredicateOperator::NotIn,
            self,
            FnvHashSet::from_iter(literals),
        ))
    }
}

impl Display for Reference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Bind for Reference {
    type Bound = BoundReference;

    fn bind(&self, schema: SchemaRef, case_sensitive: bool) -> crate::Result<Self::Bound> {
        let field = if case_sensitive {
            schema.field_by_name(&self.name)
        } else {
            schema.field_by_name_case_insensitive(&self.name)
        };

        let field = field.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Field {} not found in schema", self.name),
            )
        })?;

        let accessor = schema.accessor_by_field_id(field.id).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Accessor for Field {} not found", self.name),
            )
        })?;

        Ok(BoundReference::new(
            self.name.clone(),
            field.clone(),
            accessor.clone(),
        ))
    }
}

/// A named reference in a bound expression after binding to a schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundReference {
    // This maybe different from [`name`] filed in [`NestedField`] since this contains full path.
    // For example, if the field is `a.b.c`, then `field.name` is `c`, but `original_name` is `a.b.c`.
    column_name: String,
    field: NestedFieldRef,
    accessor: StructAccessorRef,
}

impl BoundReference {
    /// Creates a new bound reference.
    pub fn new(
        name: impl Into<String>,
        field: NestedFieldRef,
        accessor: StructAccessorRef,
    ) -> Self {
        Self {
            column_name: name.into(),
            field,
            accessor,
        }
    }

    /// Return the field of this reference.
    pub fn field(&self) -> &NestedField {
        &self.field
    }

    /// Get this BoundReference's Accessor
    pub fn accessor(&self) -> &StructAccessor {
        &self.accessor
    }
}

impl Display for BoundReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.column_name)
    }
}

/// Bound term after binding to a schema.
pub type BoundTerm = BoundReference;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::expr::accessor::StructAccessor;
    use crate::expr::{Bind, BoundReference, Reference};
    use crate::spec::{NestedField, PrimitiveType, Schema, SchemaRef, Type};

    fn table_schema_simple() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![2])
                .with_fields(vec![
                    NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_bind_reference() {
        let schema = table_schema_simple();
        let reference = Reference::new("bar").bind(schema, true).unwrap();

        let accessor_ref = Arc::new(StructAccessor::new(1, PrimitiveType::Int));
        let expected_ref = BoundReference::new(
            "bar",
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            accessor_ref.clone(),
        );

        assert_eq!(expected_ref, reference);
    }

    #[test]
    fn test_bind_reference_case_insensitive() {
        let schema = table_schema_simple();
        let reference = Reference::new("BAR").bind(schema, false).unwrap();

        let accessor_ref = Arc::new(StructAccessor::new(1, PrimitiveType::Int));
        let expected_ref = BoundReference::new(
            "BAR",
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            accessor_ref.clone(),
        );

        assert_eq!(expected_ref, reference);
    }

    #[test]
    fn test_bind_reference_failure() {
        let schema = table_schema_simple();
        let result = Reference::new("bar_not_eix").bind(schema, true);

        assert!(result.is_err());
    }

    #[test]
    fn test_bind_reference_case_insensitive_failure() {
        let schema = table_schema_simple();
        let result = Reference::new("bar_non_exist").bind(schema, false);
        assert!(result.is_err());
    }
}
