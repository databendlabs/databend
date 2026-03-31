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

//! Transforms in iceberg.

use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use fnv::FnvHashSet;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{Datum, PrimitiveLiteral};
use crate::ErrorKind;
use crate::error::{Error, Result};
use crate::expr::{
    BinaryExpression, BoundPredicate, BoundReference, Predicate, PredicateOperator, Reference,
    SetExpression, UnaryExpression,
};
use crate::spec::Literal;
use crate::spec::datatypes::{PrimitiveType, Type};
use crate::transform::{BoxedTransformFunction, create_transform_function};

/// Transform is used to transform predicates to partition predicates,
/// in addition to transforming data values.
///
/// Deriving partition predicates from column predicates on the table data
/// is used to separate the logical queries from physical storage: the
/// partitioning can change and the correct partition filters are always
/// derived from column predicates.
///
/// This simplifies queries because users don’t have to supply both logical
/// predicates and partition predicates.
///
/// All transforms must return `null` for a `null` input value.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Transform {
    /// Source value, unmodified
    ///
    /// - Source type could be any type.
    /// - Return type is the same with source type.
    Identity,
    /// Hash of value, mod `N`.
    ///
    /// Bucket partition transforms use a 32-bit hash of the source value.
    /// The 32-bit hash implementation is the 32-bit Murmur3 hash, x86
    /// variant, seeded with 0.
    ///
    /// Transforms are parameterized by a number of buckets, N. The hash mod
    /// N must produce a positive value by first discarding the sign bit of
    /// the hash value. In pseudo-code, the function is:
    ///
    /// ```text
    /// def bucket_N(x) = (murmur3_x86_32_hash(x) & Integer.MAX_VALUE) % N
    /// ```
    ///
    /// - Source type could be `int`, `long`, `decimal`, `date`, `time`,
    ///   `timestamp`, `timestamptz`, `string`, `uuid`, `fixed`, `binary`.
    /// - Return type is `int`.
    Bucket(u32),
    /// Value truncated to width `W`
    ///
    /// For `int`:
    ///
    /// - `v - (v % W)` remainders must be positive
    /// - example: W=10: 1 ￫ 0, -1 ￫ -10
    /// - note: The remainder, v % W, must be positive.
    ///
    /// For `long`:
    ///
    /// - `v - (v % W)` remainders must be positive
    /// - example: W=10: 1 ￫ 0, -1 ￫ -10
    /// - note: The remainder, v % W, must be positive.
    ///
    /// For `decimal`:
    ///
    /// - `scaled_W = decimal(W, scale(v)) v - (v % scaled_W)`
    /// - example: W=50, s=2: 10.65 ￫ 10.50
    ///
    /// For `string`:
    ///
    /// - Substring of length L: `v.substring(0, L)`
    /// - example: L=3: iceberg ￫ ice
    /// - note: Strings are truncated to a valid UTF-8 string with no more
    ///   than L code points.
    ///
    /// - Source type could be `int`, `long`, `decimal`, `string`
    /// - Return type is the same with source type.
    Truncate(u32),
    /// Extract a date or timestamp year, as years from 1970
    ///
    /// - Source type could be `date`, `timestamp`, `timestamptz`
    /// - Return type is `int`
    Year,
    /// Extract a date or timestamp month, as months from 1970-01-01
    ///
    /// - Source type could be `date`, `timestamp`, `timestamptz`
    /// - Return type is `int`
    Month,
    /// Extract a date or timestamp day, as days from 1970-01-01
    ///
    /// - Source type could be `date`, `timestamp`, `timestamptz`
    /// - Return type is `int`
    Day,
    /// Extract a timestamp hour, as hours from 1970-01-01 00:00:00
    ///
    /// - Source type could be `timestamp`, `timestamptz`
    /// - Return type is `int`
    Hour,
    /// Always produces `null`
    ///
    /// The void transform may be used to replace the transform in an
    /// existing partition field so that the field is effectively dropped in
    /// v1 tables.
    ///
    /// - Source type could be any type..
    /// - Return type is Source type.
    Void,
    /// Used to represent some customized transform that can't be recognized or supported now.
    Unknown,
}

impl Transform {
    /// Returns a human-readable String representation of a transformed value.
    pub fn to_human_string(&self, field_type: &Type, value: Option<&Literal>) -> String {
        let Some(value) = value else {
            return "null".to_string();
        };

        if let Some(value) = value.as_primitive_literal() {
            let field_type = field_type.as_primitive_type().unwrap();
            let datum = Datum::new(field_type.clone(), value);

            match self {
                Self::Void => "null".to_string(),
                _ => datum.to_human_string(),
            }
        } else {
            "null".to_string()
        }
    }

    /// Get the return type of transform given the input type.
    /// Returns `None` if it can't be transformed.
    pub fn result_type(&self, input_type: &Type) -> Result<Type> {
        match self {
            Transform::Identity => {
                if matches!(input_type, Type::Primitive(_)) {
                    Ok(input_type.clone())
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("{input_type} is not a valid input type of identity transform",),
                    ))
                }
            }
            Transform::Void => Ok(input_type.clone()),
            Transform::Unknown => Ok(Type::Primitive(PrimitiveType::String)),
            Transform::Bucket(_) => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Int
                        | PrimitiveType::Long
                        | PrimitiveType::Decimal { .. }
                        | PrimitiveType::Date
                        | PrimitiveType::Time
                        | PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs
                        | PrimitiveType::String
                        | PrimitiveType::Uuid
                        | PrimitiveType::Fixed(_)
                        | PrimitiveType::Binary => Ok(Type::Primitive(PrimitiveType::Int)),
                        _ => Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("{input_type} is not a valid input type of bucket transform",),
                        )),
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("{input_type} is not a valid input type of bucket transform",),
                    ))
                }
            }
            Transform::Truncate(_) => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Int
                        | PrimitiveType::Long
                        | PrimitiveType::String
                        | PrimitiveType::Binary
                        | PrimitiveType::Decimal { .. } => Ok(input_type.clone()),
                        _ => Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("{input_type} is not a valid input type of truncate transform",),
                        )),
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("{input_type} is not a valid input type of truncate transform",),
                    ))
                }
            }
            Transform::Year | Transform::Month => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs
                        | PrimitiveType::Date => Ok(Type::Primitive(PrimitiveType::Int)),
                        _ => Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("{input_type} is not a valid input type of {self} transform",),
                        )),
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("{input_type} is not a valid input type of {self} transform",),
                    ))
                }
            }
            Transform::Day => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs
                        | PrimitiveType::Date => Ok(Type::Primitive(PrimitiveType::Date)),
                        _ => Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("{input_type} is not a valid input type of {self} transform",),
                        )),
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("{input_type} is not a valid input type of {self} transform",),
                    ))
                }
            }
            Transform::Hour => {
                if let Type::Primitive(p) = input_type {
                    match p {
                        PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs => Ok(Type::Primitive(PrimitiveType::Int)),
                        _ => Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("{input_type} is not a valid input type of {self} transform",),
                        )),
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("{input_type} is not a valid input type of {self} transform",),
                    ))
                }
            }
        }
    }

    /// Whether the transform preserves the order of values.
    pub fn preserves_order(&self) -> bool {
        !matches!(
            self,
            Transform::Void | Transform::Bucket(_) | Transform::Unknown
        )
    }

    /// Return the unique transform name to check if similar transforms for the same source field
    /// are added multiple times in partition spec builder.
    pub fn dedup_name(&self) -> String {
        match self {
            Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
                "time".to_string()
            }
            _ => format!("{self}"),
        }
    }

    /// Whether ordering by this transform's result satisfies the ordering of another transform's
    /// result.
    ///
    /// For example, sorting by day(ts) will produce an ordering that is also by month(ts) or
    ///  year(ts). However, sorting by day(ts) will not satisfy the order of hour(ts) or identity(ts).
    pub fn satisfies_order_of(&self, other: &Self) -> bool {
        match self {
            Transform::Identity => other.preserves_order(),
            Transform::Hour => matches!(
                other,
                Transform::Hour | Transform::Day | Transform::Month | Transform::Year
            ),
            Transform::Day => matches!(other, Transform::Day | Transform::Month | Transform::Year),
            Transform::Month => matches!(other, Transform::Month | Transform::Year),
            _ => self == other,
        }
    }

    /// Strictly projects a given predicate according to the transformation
    /// specified by the `Transform` instance.
    ///
    /// This method ensures that the projected predicate is strictly aligned
    /// with the transformation logic, providing a more precise filtering
    /// mechanism for transformed data.
    ///
    /// # Example
    /// Suppose, we have row filter `a = 10`, and a partition spec
    /// `bucket(a, 37) as bs`, if one row matches `a = 10`, then its partition
    /// value should match `bucket(10, 37) as bs`, and we project `a = 10` to
    /// `bs = bucket(10, 37)`
    pub fn strict_project(
        &self,
        name: &str,
        predicate: &BoundPredicate,
    ) -> Result<Option<Predicate>> {
        let func = create_transform_function(self)?;

        match self {
            Transform::Identity => match predicate {
                BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                BoundPredicate::Binary(expr) => Ok(Some(Predicate::Binary(BinaryExpression::new(
                    expr.op(),
                    Reference::new(name),
                    expr.literal().to_owned(),
                )))),
                BoundPredicate::Set(expr) => Ok(Some(Predicate::Set(SetExpression::new(
                    expr.op(),
                    Reference::new(name),
                    expr.literals().to_owned(),
                )))),
                _ => Ok(None),
            },
            Transform::Bucket(_) => match predicate {
                BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                BoundPredicate::Binary(expr) => {
                    self.project_binary_expr(name, PredicateOperator::NotEq, expr, &func)
                }
                BoundPredicate::Set(expr) => {
                    self.project_set_expr(expr, PredicateOperator::NotIn, name, &func)
                }
                _ => Ok(None),
            },
            Transform::Truncate(width) => match predicate {
                BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                BoundPredicate::Binary(expr) => {
                    if matches!(
                        expr.term().field().field_type.as_primitive_type(),
                        Some(&PrimitiveType::Int)
                            | Some(&PrimitiveType::Long)
                            | Some(&PrimitiveType::Decimal { .. })
                    ) {
                        self.truncate_number_strict(name, expr, &func)
                    } else if expr.op() == PredicateOperator::StartsWith {
                        let len = match expr.literal().literal() {
                            PrimitiveLiteral::String(s) => s.len(),
                            PrimitiveLiteral::Binary(b) => b.len(),
                            _ => {
                                return Err(Error::new(
                                    ErrorKind::DataInvalid,
                                    format!(
                                        "Expected a string or binary literal, got: {:?}",
                                        expr.literal()
                                    ),
                                ));
                            }
                        };
                        match len.cmp(&(*width as usize)) {
                            Ordering::Less => Ok(Some(Predicate::Binary(BinaryExpression::new(
                                PredicateOperator::StartsWith,
                                Reference::new(name),
                                expr.literal().to_owned(),
                            )))),
                            Ordering::Equal => Ok(Some(Predicate::Binary(BinaryExpression::new(
                                PredicateOperator::Eq,
                                Reference::new(name),
                                expr.literal().to_owned(),
                            )))),
                            Ordering::Greater => Ok(None),
                        }
                    } else if expr.op() == PredicateOperator::NotStartsWith {
                        let len = match expr.literal().literal() {
                            PrimitiveLiteral::String(s) => s.len(),
                            PrimitiveLiteral::Binary(b) => b.len(),
                            _ => {
                                return Err(Error::new(
                                    ErrorKind::DataInvalid,
                                    format!(
                                        "Expected a string or binary literal, got: {:?}",
                                        expr.literal()
                                    ),
                                ));
                            }
                        };
                        match len.cmp(&(*width as usize)) {
                            Ordering::Less => Ok(Some(Predicate::Binary(BinaryExpression::new(
                                PredicateOperator::NotStartsWith,
                                Reference::new(name),
                                expr.literal().to_owned(),
                            )))),
                            Ordering::Equal => Ok(Some(Predicate::Binary(BinaryExpression::new(
                                PredicateOperator::NotEq,
                                Reference::new(name),
                                expr.literal().to_owned(),
                            )))),
                            Ordering::Greater => {
                                Ok(Some(Predicate::Binary(BinaryExpression::new(
                                    expr.op(),
                                    Reference::new(name),
                                    func.transform_literal_result(expr.literal())?,
                                ))))
                            }
                        }
                    } else {
                        self.truncate_array_strict(name, expr, &func)
                    }
                }
                BoundPredicate::Set(expr) => {
                    self.project_set_expr(expr, PredicateOperator::NotIn, name, &func)
                }
                _ => Ok(None),
            },
            Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
                match predicate {
                    BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                    BoundPredicate::Binary(expr) => self.truncate_number_strict(name, expr, &func),
                    BoundPredicate::Set(expr) => {
                        self.project_set_expr(expr, PredicateOperator::NotIn, name, &func)
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// Projects a given predicate according to the transformation
    /// specified by the `Transform` instance.
    ///
    /// This allows predicates to be effectively applied to data
    /// that has undergone transformation, enabling efficient querying
    /// and filtering based on the original, untransformed data.
    ///
    /// # Example
    /// Suppose, we have row filter `a = 10`, and a partition spec
    /// `bucket(a, 37) as bs`, if one row matches `a = 10`, then its partition
    /// value should match `bucket(10, 37) as bs`, and we project `a = 10` to
    /// `bs = bucket(10, 37)`
    pub fn project(&self, name: &str, predicate: &BoundPredicate) -> Result<Option<Predicate>> {
        let func = create_transform_function(self)?;

        match self {
            Transform::Identity => match predicate {
                BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                BoundPredicate::Binary(expr) => Ok(Some(Predicate::Binary(BinaryExpression::new(
                    expr.op(),
                    Reference::new(name),
                    expr.literal().to_owned(),
                )))),
                BoundPredicate::Set(expr) => Ok(Some(Predicate::Set(SetExpression::new(
                    expr.op(),
                    Reference::new(name),
                    expr.literals().to_owned(),
                )))),
                _ => Ok(None),
            },
            Transform::Bucket(_) => match predicate {
                BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                BoundPredicate::Binary(expr) => {
                    self.project_binary_expr(name, PredicateOperator::Eq, expr, &func)
                }
                BoundPredicate::Set(expr) => {
                    self.project_set_expr(expr, PredicateOperator::In, name, &func)
                }
                _ => Ok(None),
            },
            Transform::Truncate(width) => match predicate {
                BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                BoundPredicate::Binary(expr) => {
                    self.project_binary_with_adjusted_boundary(name, expr, &func, Some(*width))
                }
                BoundPredicate::Set(expr) => {
                    self.project_set_expr(expr, PredicateOperator::In, name, &func)
                }
                _ => Ok(None),
            },
            Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
                match predicate {
                    BoundPredicate::Unary(expr) => Self::project_unary(expr.op(), name),
                    BoundPredicate::Binary(expr) => {
                        self.project_binary_with_adjusted_boundary(name, expr, &func, None)
                    }
                    BoundPredicate::Set(expr) => {
                        self.project_set_expr(expr, PredicateOperator::In, name, &func)
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// Check if `Transform` is applicable on datum's `PrimitiveType`
    fn can_transform(&self, datum: &Datum) -> bool {
        let input_type = datum.data_type().clone();
        self.result_type(&Type::Primitive(input_type)).is_ok()
    }

    /// Creates a unary predicate from a given operator and a reference name.
    fn project_unary(op: PredicateOperator, name: &str) -> Result<Option<Predicate>> {
        Ok(Some(Predicate::Unary(UnaryExpression::new(
            op,
            Reference::new(name),
        ))))
    }

    /// Attempts to create a binary predicate based on a binary expression,
    /// if applicable.
    ///
    /// This method evaluates a given binary expression and, if the operation
    /// is the given operator and the literal can be transformed, constructs a
    /// `Predicate::Binary`variant representing the binary operation.
    fn project_binary_expr(
        &self,
        name: &str,
        op: PredicateOperator,
        expr: &BinaryExpression<BoundReference>,
        func: &BoxedTransformFunction,
    ) -> Result<Option<Predicate>> {
        if expr.op() != op || !self.can_transform(expr.literal()) {
            return Ok(None);
        }

        Ok(Some(Predicate::Binary(BinaryExpression::new(
            expr.op(),
            Reference::new(name),
            func.transform_literal_result(expr.literal())?,
        ))))
    }

    /// Projects a binary expression to a predicate with an adjusted boundary.
    ///
    /// Checks if the literal within the given binary expression is
    /// transformable. If transformable, it proceeds to potentially adjust
    /// the boundary of the expression based on the comparison operator (`op`).
    /// The potential adjustments involve incrementing or decrementing the
    /// literal value and changing the `PredicateOperator` itself to its
    /// inclusive variant.
    fn project_binary_with_adjusted_boundary(
        &self,
        name: &str,
        expr: &BinaryExpression<BoundReference>,
        func: &BoxedTransformFunction,
        width: Option<u32>,
    ) -> Result<Option<Predicate>> {
        if !self.can_transform(expr.literal()) {
            return Ok(None);
        }

        let op = &expr.op();
        let datum = &expr.literal();

        if let Some(boundary) = Self::adjust_boundary(op, datum)? {
            let transformed_projection = func.transform_literal_result(&boundary)?;

            let adjusted_projection =
                self.adjust_time_projection(op, datum, &transformed_projection);

            let adjusted_operator = Self::adjust_operator(op, datum, width);

            if let Some(op) = adjusted_operator {
                let predicate = match adjusted_projection {
                    None => Predicate::Binary(BinaryExpression::new(
                        op,
                        Reference::new(name),
                        transformed_projection,
                    )),
                    Some(AdjustedProjection::Single(d)) => {
                        Predicate::Binary(BinaryExpression::new(op, Reference::new(name), d))
                    }
                    Some(AdjustedProjection::Set(d)) => Predicate::Set(SetExpression::new(
                        PredicateOperator::In,
                        Reference::new(name),
                        d,
                    )),
                };
                return Ok(Some(predicate));
            }
        };

        Ok(None)
    }

    /// Projects a set expression to a predicate,
    /// applying a transformation to each literal in the set.
    fn project_set_expr(
        &self,
        expr: &SetExpression<BoundReference>,
        op: PredicateOperator,
        name: &str,
        func: &BoxedTransformFunction,
    ) -> Result<Option<Predicate>> {
        if expr.op() != op || expr.literals().iter().any(|d| !self.can_transform(d)) {
            return Ok(None);
        }

        let mut new_set = FnvHashSet::default();

        for lit in expr.literals() {
            let datum = func.transform_literal_result(lit)?;

            if let Some(AdjustedProjection::Single(d)) =
                self.adjust_time_projection(&op, lit, &datum)
            {
                new_set.insert(d);
            };

            new_set.insert(datum);
        }

        Ok(Some(Predicate::Set(SetExpression::new(
            expr.op(),
            Reference::new(name),
            new_set,
        ))))
    }

    /// Adjusts the boundary value for comparison operations
    /// based on the specified `PredicateOperator` and `Datum`.
    ///
    /// This function modifies the boundary value for certain comparison
    /// operators (`LessThan`, `GreaterThan`) by incrementing or decrementing
    /// the literal value within the given `Datum`. For operators that do not
    /// imply a boundary shift (`Eq`, `LessThanOrEq`, `GreaterThanOrEq`,
    /// `StartsWith`, `NotStartsWith`), the original datum is returned
    /// unmodified.
    fn adjust_boundary(op: &PredicateOperator, datum: &Datum) -> Result<Option<Datum>> {
        let adjusted_boundary = match op {
            PredicateOperator::LessThan => match (datum.data_type(), datum.literal()) {
                (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Some(Datum::int(v - 1)),
                (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Some(Datum::long(v - 1)),
                (PrimitiveType::Decimal { .. }, PrimitiveLiteral::Int128(v)) => {
                    Some(Datum::decimal(v - 1)?)
                }
                (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Some(Datum::date(v - 1)),
                (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                    Some(Datum::timestamp_micros(v - 1))
                }
                _ => Some(datum.to_owned()),
            },
            PredicateOperator::GreaterThan => match (datum.data_type(), datum.literal()) {
                (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Some(Datum::int(v + 1)),
                (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Some(Datum::long(v + 1)),
                (PrimitiveType::Decimal { .. }, PrimitiveLiteral::Int128(v)) => {
                    Some(Datum::decimal(v + 1)?)
                }
                (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Some(Datum::date(v + 1)),
                (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                    Some(Datum::timestamp_micros(v + 1))
                }
                _ => Some(datum.to_owned()),
            },
            PredicateOperator::Eq
            | PredicateOperator::LessThanOrEq
            | PredicateOperator::GreaterThanOrEq
            | PredicateOperator::StartsWith
            | PredicateOperator::NotStartsWith => Some(datum.to_owned()),
            _ => None,
        };

        Ok(adjusted_boundary)
    }

    /// Adjusts the comparison operator based on the specified datum and an
    /// optional width constraint.
    ///
    /// This function modifies the comparison operator for `LessThan` and
    /// `GreaterThan` cases to their inclusive counterparts (`LessThanOrEq`,
    /// `GreaterThanOrEq`) unconditionally. For `StartsWith` and
    /// `NotStartsWith` operators acting on string literals, the operator may
    /// be adjusted to `Eq` or `NotEq` if the string length matches the
    /// specified width, indicating a precise match rather than a prefix
    /// condition.
    fn adjust_operator(
        op: &PredicateOperator,
        datum: &Datum,
        width: Option<u32>,
    ) -> Option<PredicateOperator> {
        match op {
            PredicateOperator::LessThan => Some(PredicateOperator::LessThanOrEq),
            PredicateOperator::GreaterThan => Some(PredicateOperator::GreaterThanOrEq),
            PredicateOperator::StartsWith => match datum.literal() {
                PrimitiveLiteral::String(s) => {
                    if let Some(w) = width
                        && s.len() == w as usize
                    {
                        return Some(PredicateOperator::Eq);
                    };
                    Some(*op)
                }
                _ => Some(*op),
            },
            PredicateOperator::NotStartsWith => match datum.literal() {
                PrimitiveLiteral::String(s) => {
                    if let Some(w) = width {
                        let w = w as usize;

                        if s.len() == w {
                            return Some(PredicateOperator::NotEq);
                        }

                        if s.len() < w {
                            return Some(*op);
                        }

                        return None;
                    };
                    Some(*op)
                }
                _ => Some(*op),
            },
            _ => Some(*op),
        }
    }

    /// Adjust projection for temporal transforms, align with Java
    /// implementation: https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/transforms/ProjectionUtil.java#L275
    fn adjust_time_projection(
        &self,
        op: &PredicateOperator,
        original: &Datum,
        transformed: &Datum,
    ) -> Option<AdjustedProjection> {
        let should_adjust = match self {
            Transform::Day => matches!(original.data_type(), PrimitiveType::Timestamp),
            Transform::Year | Transform::Month => true,
            _ => false,
        };

        if should_adjust && let &PrimitiveLiteral::Int(v) = transformed.literal() {
            match op {
                PredicateOperator::LessThan
                | PredicateOperator::LessThanOrEq
                | PredicateOperator::In => {
                    if v < 0 {
                        // # TODO
                        // An ugly hack to fix. Refine the increment and decrement logic later.
                        match self {
                            Transform::Day => {
                                return Some(AdjustedProjection::Single(Datum::date(v + 1)));
                            }
                            _ => {
                                return Some(AdjustedProjection::Single(Datum::int(v + 1)));
                            }
                        }
                    };
                }
                PredicateOperator::Eq => {
                    if v < 0 {
                        let new_set = FnvHashSet::from_iter(vec![
                            transformed.to_owned(),
                            // # TODO
                            // An ugly hack to fix. Refine the increment and decrement logic later.
                            {
                                match self {
                                    Transform::Day => Datum::date(v + 1),
                                    _ => Datum::int(v + 1),
                                }
                            },
                        ]);
                        return Some(AdjustedProjection::Set(new_set));
                    }
                }
                _ => {
                    return None;
                }
            }
        };
        None
    }

    // Increment for Int, Long, Decimal, Date, Timestamp
    // Ignore other types
    #[inline]
    fn try_increment_number(datum: &Datum) -> Result<Datum> {
        match (datum.data_type(), datum.literal()) {
            (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Ok(Datum::int(v + 1)),
            (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Ok(Datum::long(v + 1)),
            (PrimitiveType::Decimal { .. }, PrimitiveLiteral::Int128(v)) => Datum::decimal(v + 1),
            (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Ok(Datum::date(v + 1)),
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamp_micros(v + 1))
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamp_nanos(v + 1))
            }
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamptz_micros(v + 1))
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamptz_nanos(v + 1))
            }
            (PrimitiveType::Int, _)
            | (PrimitiveType::Long, _)
            | (PrimitiveType::Decimal { .. }, _)
            | (PrimitiveType::Date, _)
            | (PrimitiveType::Timestamp, _) => Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Unsupported literal increment for type: {:?}",
                    datum.data_type()
                ),
            )),
            _ => Ok(datum.to_owned()),
        }
    }

    // Decrement for Int, Long, Decimal, Date, Timestamp
    // Ignore other types
    #[inline]
    fn try_decrement_number(datum: &Datum) -> Result<Datum> {
        match (datum.data_type(), datum.literal()) {
            (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Ok(Datum::int(v - 1)),
            (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Ok(Datum::long(v - 1)),
            (PrimitiveType::Decimal { .. }, PrimitiveLiteral::Int128(v)) => Datum::decimal(v - 1),
            (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Ok(Datum::date(v - 1)),
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamp_micros(v - 1))
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamp_nanos(v - 1))
            }
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamptz_micros(v - 1))
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                Ok(Datum::timestamptz_nanos(v - 1))
            }
            (PrimitiveType::Int, _)
            | (PrimitiveType::Long, _)
            | (PrimitiveType::Decimal { .. }, _)
            | (PrimitiveType::Date, _)
            | (PrimitiveType::Timestamp, _) => Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Unsupported literal decrement for type: {:?}",
                    datum.data_type()
                ),
            )),
            _ => Ok(datum.to_owned()),
        }
    }

    fn truncate_number_strict(
        &self,
        name: &str,
        expr: &BinaryExpression<BoundReference>,
        func: &BoxedTransformFunction,
    ) -> Result<Option<Predicate>> {
        let boundary = expr.literal();

        if !matches!(
            boundary.data_type(),
            &PrimitiveType::Int
                | &PrimitiveType::Long
                | &PrimitiveType::Decimal { .. }
                | &PrimitiveType::Date
                | &PrimitiveType::Timestamp
                | &PrimitiveType::Timestamptz
                | &PrimitiveType::TimestampNs
                | &PrimitiveType::TimestamptzNs
        ) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Expected a numeric literal, got: {boundary:?}"),
            ));
        }

        let predicate = match expr.op() {
            PredicateOperator::LessThan => Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::LessThan,
                Reference::new(name),
                func.transform_literal_result(boundary)?,
            ))),
            PredicateOperator::LessThanOrEq => Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::LessThan,
                Reference::new(name),
                func.transform_literal_result(&Self::try_increment_number(boundary)?)?,
            ))),
            PredicateOperator::GreaterThan => Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::GreaterThan,
                Reference::new(name),
                func.transform_literal_result(boundary)?,
            ))),
            PredicateOperator::GreaterThanOrEq => Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::GreaterThan,
                Reference::new(name),
                func.transform_literal_result(&Self::try_decrement_number(boundary)?)?,
            ))),
            PredicateOperator::NotEq => Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::NotEq,
                Reference::new(name),
                func.transform_literal_result(boundary)?,
            ))),
            _ => None,
        };

        Ok(predicate)
    }

    fn truncate_array_strict(
        &self,
        name: &str,
        expr: &BinaryExpression<BoundReference>,
        func: &BoxedTransformFunction,
    ) -> Result<Option<Predicate>> {
        let boundary = expr.literal();

        match expr.op() {
            PredicateOperator::LessThan | PredicateOperator::LessThanOrEq => {
                Ok(Some(Predicate::Binary(BinaryExpression::new(
                    PredicateOperator::LessThan,
                    Reference::new(name),
                    func.transform_literal_result(boundary)?,
                ))))
            }
            PredicateOperator::GreaterThan | PredicateOperator::GreaterThanOrEq => {
                Ok(Some(Predicate::Binary(BinaryExpression::new(
                    PredicateOperator::GreaterThan,
                    Reference::new(name),
                    func.transform_literal_result(boundary)?,
                ))))
            }
            PredicateOperator::NotEq => Ok(Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::NotEq,
                Reference::new(name),
                func.transform_literal_result(boundary)?,
            )))),
            _ => Ok(None),
        }
    }
}

impl Display for Transform {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Transform::Identity => write!(f, "identity"),
            Transform::Year => write!(f, "year"),
            Transform::Month => write!(f, "month"),
            Transform::Day => write!(f, "day"),
            Transform::Hour => write!(f, "hour"),
            Transform::Void => write!(f, "void"),
            Transform::Bucket(length) => write!(f, "bucket[{length}]"),
            Transform::Truncate(width) => write!(f, "truncate[{width}]"),
            Transform::Unknown => write!(f, "unknown"),
        }
    }
}

impl FromStr for Transform {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let t = match s {
            "identity" => Transform::Identity,
            "year" => Transform::Year,
            "month" => Transform::Month,
            "day" => Transform::Day,
            "hour" => Transform::Hour,
            "void" => Transform::Void,
            "unknown" => Transform::Unknown,
            v if v.starts_with("bucket") => {
                let length = v
                    .strip_prefix("bucket")
                    .expect("transform must starts with `bucket`")
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse()
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("transform bucket type {v:?} is invalid"),
                        )
                        .with_source(err)
                    })?;

                Transform::Bucket(length)
            }
            v if v.starts_with("truncate") => {
                let width = v
                    .strip_prefix("truncate")
                    .expect("transform must starts with `truncate`")
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse()
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("transform truncate type {v:?} is invalid"),
                        )
                        .with_source(err)
                    })?;

                Transform::Truncate(width)
            }
            v => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("transform {v:?} is invalid"),
                ));
            }
        };

        Ok(t)
    }
}

impl Serialize for Transform {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(format!("{self}").as_str())
    }
}

impl<'de> Deserialize<'de> for Transform {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(<D::Error as serde::de::Error>::custom)
    }
}

/// An enum representing the result of the adjusted projection.
/// Either being a single adjusted datum or a set.
#[derive(Debug)]
enum AdjustedProjection {
    Single(Datum),
    Set(FnvHashSet<Datum>),
}
