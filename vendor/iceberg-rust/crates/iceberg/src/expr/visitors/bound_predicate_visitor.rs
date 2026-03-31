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

use crate::Result;
use crate::expr::{BoundPredicate, BoundReference, PredicateOperator};
use crate::spec::Datum;

/// A visitor for [`BoundPredicate`]s. Visits in post-order.
pub trait BoundPredicateVisitor {
    /// The return type of this visitor
    type T;

    /// Called after an `AlwaysTrue` predicate is visited
    fn always_true(&mut self) -> Result<Self::T>;

    /// Called after an `AlwaysFalse` predicate is visited
    fn always_false(&mut self) -> Result<Self::T>;

    /// Called after an `And` predicate is visited
    fn and(&mut self, lhs: Self::T, rhs: Self::T) -> Result<Self::T>;

    /// Called after an `Or` predicate is visited
    fn or(&mut self, lhs: Self::T, rhs: Self::T) -> Result<Self::T>;

    /// Called after a `Not` predicate is visited
    fn not(&mut self, inner: Self::T) -> Result<Self::T>;

    /// Called after a predicate with an `IsNull` operator is visited
    fn is_null(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;

    /// Called after a predicate with a `NotNull` operator is visited
    fn not_null(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;

    /// Called after a predicate with an `IsNan` operator is visited
    fn is_nan(&mut self, reference: &BoundReference, predicate: &BoundPredicate)
    -> Result<Self::T>;

    /// Called after a predicate with a `NotNan` operator is visited
    fn not_nan(
        &mut self,
        reference: &BoundReference,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;

    /// Called after a predicate with a `LessThan` operator is visited
    fn less_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;

    /// Called after a predicate with a `LessThanOrEq` operator is visited
    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;

    /// Called after a predicate with a `GreaterThan` operator is visited
    fn greater_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;

    /// Called after a predicate with a `GreaterThanOrEq` operator is visited
    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;

    /// Called after a predicate with an `Eq` operator is visited
    fn eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;

    /// Called after a predicate with a `NotEq` operator is visited
    fn not_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;

    /// Called after a predicate with a `StartsWith` operator is visited
    fn starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;

    /// Called after a predicate with a `NotStartsWith` operator is visited
    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;

    /// Called after a predicate with an `In` operator is visited
    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;

    /// Called after a predicate with a `NotIn` operator is visited
    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        predicate: &BoundPredicate,
    ) -> Result<Self::T>;
}

/// Visits a [`BoundPredicate`] with the provided visitor,
/// in post-order
pub(crate) fn visit<V: BoundPredicateVisitor>(
    visitor: &mut V,
    predicate: &BoundPredicate,
) -> Result<V::T> {
    match predicate {
        BoundPredicate::AlwaysTrue => visitor.always_true(),
        BoundPredicate::AlwaysFalse => visitor.always_false(),
        BoundPredicate::And(expr) => {
            let [left_pred, right_pred] = expr.inputs();

            let left_result = visit(visitor, left_pred)?;
            let right_result = visit(visitor, right_pred)?;

            visitor.and(left_result, right_result)
        }
        BoundPredicate::Or(expr) => {
            let [left_pred, right_pred] = expr.inputs();

            let left_result = visit(visitor, left_pred)?;
            let right_result = visit(visitor, right_pred)?;

            visitor.or(left_result, right_result)
        }
        BoundPredicate::Not(expr) => {
            let [inner_pred] = expr.inputs();

            let inner_result = visit(visitor, inner_pred)?;

            visitor.not(inner_result)
        }
        BoundPredicate::Unary(expr) => match expr.op() {
            PredicateOperator::IsNull => visitor.is_null(expr.term(), predicate),
            PredicateOperator::NotNull => visitor.not_null(expr.term(), predicate),
            PredicateOperator::IsNan => visitor.is_nan(expr.term(), predicate),
            PredicateOperator::NotNan => visitor.not_nan(expr.term(), predicate),
            op => {
                panic!("Unexpected op for unary predicate: {}", &op)
            }
        },
        BoundPredicate::Binary(expr) => {
            let reference = expr.term();
            let literal = expr.literal();
            match expr.op() {
                PredicateOperator::LessThan => visitor.less_than(reference, literal, predicate),
                PredicateOperator::LessThanOrEq => {
                    visitor.less_than_or_eq(reference, literal, predicate)
                }
                PredicateOperator::GreaterThan => {
                    visitor.greater_than(reference, literal, predicate)
                }
                PredicateOperator::GreaterThanOrEq => {
                    visitor.greater_than_or_eq(reference, literal, predicate)
                }
                PredicateOperator::Eq => visitor.eq(reference, literal, predicate),
                PredicateOperator::NotEq => visitor.not_eq(reference, literal, predicate),
                PredicateOperator::StartsWith => visitor.starts_with(reference, literal, predicate),
                PredicateOperator::NotStartsWith => {
                    visitor.not_starts_with(reference, literal, predicate)
                }
                op => {
                    panic!("Unexpected op for binary predicate: {}", &op)
                }
            }
        }
        BoundPredicate::Set(expr) => {
            let reference = expr.term();
            let literals = expr.literals();
            match expr.op() {
                PredicateOperator::In => visitor.r#in(reference, literals, predicate),
                PredicateOperator::NotIn => visitor.not_in(reference, literals, predicate),
                op => {
                    panic!("Unexpected op for set predicate: {}", &op)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Not;
    use std::sync::Arc;

    use fnv::FnvHashSet;

    use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
    use crate::expr::{
        BinaryExpression, Bind, BoundPredicate, BoundReference, Predicate, PredicateOperator,
        Reference, SetExpression, UnaryExpression,
    };
    use crate::spec::{Datum, NestedField, PrimitiveType, Schema, SchemaRef, Type};

    struct TestEvaluator {}
    impl BoundPredicateVisitor for TestEvaluator {
        type T = bool;

        fn always_true(&mut self) -> crate::Result<Self::T> {
            Ok(true)
        }

        fn always_false(&mut self) -> crate::Result<Self::T> {
            Ok(false)
        }

        fn and(&mut self, lhs: Self::T, rhs: Self::T) -> crate::Result<Self::T> {
            Ok(lhs && rhs)
        }

        fn or(&mut self, lhs: Self::T, rhs: Self::T) -> crate::Result<Self::T> {
            Ok(lhs || rhs)
        }

        fn not(&mut self, inner: Self::T) -> crate::Result<Self::T> {
            Ok(!inner)
        }

        fn is_null(
            &mut self,
            _reference: &BoundReference,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(true)
        }

        fn not_null(
            &mut self,
            _reference: &BoundReference,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn is_nan(
            &mut self,
            _reference: &BoundReference,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(true)
        }

        fn not_nan(
            &mut self,
            _reference: &BoundReference,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn less_than(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(true)
        }

        fn less_than_or_eq(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn greater_than(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(true)
        }

        fn greater_than_or_eq(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn eq(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(true)
        }

        fn not_eq(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn starts_with(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(true)
        }

        fn not_starts_with(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn r#in(
            &mut self,
            _reference: &BoundReference,
            _literals: &FnvHashSet<Datum>,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(true)
        }

        fn not_in(
            &mut self,
            _reference: &BoundReference,
            _literals: &FnvHashSet<Datum>,
            _predicate: &BoundPredicate,
        ) -> crate::Result<bool> {
            Ok(false)
        }
    }

    fn create_test_schema() -> SchemaRef {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::required(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::Float),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "c",
                    Type::Primitive(PrimitiveType::Float),
                )),
            ])
            .build()
            .unwrap();

        let schema_arc = Arc::new(schema);
        schema_arc.clone()
    }

    #[test]
    fn test_always_true() {
        let predicate = Predicate::AlwaysTrue;
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_always_false() {
        let predicate = Predicate::AlwaysFalse;
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());
    }

    #[test]
    fn test_logical_and() {
        let predicate = Predicate::AlwaysTrue.and(Predicate::AlwaysFalse);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());

        let predicate = Predicate::AlwaysFalse.and(Predicate::AlwaysFalse);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());

        let predicate = Predicate::AlwaysTrue.and(Predicate::AlwaysTrue);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_logical_or() {
        let predicate = Predicate::AlwaysTrue.or(Predicate::AlwaysFalse);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());

        let predicate = Predicate::AlwaysFalse.or(Predicate::AlwaysFalse);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());

        let predicate = Predicate::AlwaysTrue.or(Predicate::AlwaysTrue);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_not() {
        let predicate = Predicate::AlwaysFalse.not();
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());

        let predicate = Predicate::AlwaysTrue.not();
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());
    }

    #[test]
    fn test_is_null() {
        let predicate = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNull,
            Reference::new("c"),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_not_null() {
        let predicate = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNull,
            Reference::new("a"),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_is_nan() {
        let predicate = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::IsNan,
            Reference::new("b"),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_not_nan() {
        let predicate = Predicate::Unary(UnaryExpression::new(
            PredicateOperator::NotNan,
            Reference::new("b"),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());
    }

    #[test]
    fn test_less_than() {
        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThan,
            Reference::new("a"),
            Datum::int(10),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_less_than_or_eq() {
        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::LessThanOrEq,
            Reference::new("a"),
            Datum::int(10),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());
    }

    #[test]
    fn test_greater_than() {
        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThan,
            Reference::new("a"),
            Datum::int(10),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_greater_than_or_eq() {
        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::GreaterThanOrEq,
            Reference::new("a"),
            Datum::int(10),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());
    }

    #[test]
    fn test_eq() {
        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::Eq,
            Reference::new("a"),
            Datum::int(10),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_not_eq() {
        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotEq,
            Reference::new("a"),
            Datum::int(10),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());
    }

    #[test]
    fn test_starts_with() {
        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::StartsWith,
            Reference::new("a"),
            Datum::int(10),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_not_starts_with() {
        let predicate = Predicate::Binary(BinaryExpression::new(
            PredicateOperator::NotStartsWith,
            Reference::new("a"),
            Datum::int(10),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());
    }

    #[test]
    fn test_in() {
        let predicate = Predicate::Set(SetExpression::new(
            PredicateOperator::In,
            Reference::new("a"),
            FnvHashSet::from_iter(vec![Datum::int(1)]),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(result.unwrap());
    }

    #[test]
    fn test_not_in() {
        let predicate = Predicate::Set(SetExpression::new(
            PredicateOperator::NotIn,
            Reference::new("a"),
            FnvHashSet::from_iter(vec![Datum::int(1)]),
        ));
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = visit(&mut test_evaluator, &bound_predicate);

        assert!(!result.unwrap());
    }
}
