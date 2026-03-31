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

//! Transform function used to compute partition values.

use std::fmt::Debug;

use arrow_array::ArrayRef;

use crate::spec::{Datum, Transform};
use crate::{Error, ErrorKind, Result};

mod bucket;
mod identity;
mod temporal;
mod truncate;
mod void;

/// TransformFunction is a trait that defines the interface for all transform functions.
pub trait TransformFunction: Send + Sync + Debug {
    /// transform will take an input array and transform it into a new array.
    /// The implementation of this function will need to check and downcast the input to specific
    /// type.
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef>;
    /// transform_literal will take an input literal and transform it into a new literal.
    fn transform_literal(&self, input: &Datum) -> Result<Option<Datum>>;
    /// A thin wrapper around `transform_literal`
    /// to return an error even when it's `None`.
    fn transform_literal_result(&self, input: &Datum) -> Result<Datum> {
        self.transform_literal(input)?.ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Returns 'None' for literal {input}"),
            )
        })
    }
}

/// BoxedTransformFunction is a boxed trait object of TransformFunction.
pub type BoxedTransformFunction = Box<dyn TransformFunction>;

/// create_transform_function creates a boxed trait object of TransformFunction from a Transform.
pub fn create_transform_function(transform: &Transform) -> Result<BoxedTransformFunction> {
    match transform {
        Transform::Identity => Ok(Box::new(identity::Identity {})),
        Transform::Void => Ok(Box::new(void::Void {})),
        Transform::Year => Ok(Box::new(temporal::Year {})),
        Transform::Month => Ok(Box::new(temporal::Month {})),
        Transform::Day => Ok(Box::new(temporal::Day {})),
        Transform::Hour => Ok(Box::new(temporal::Hour {})),
        Transform::Bucket(mod_n) => Ok(Box::new(bucket::Bucket::new(*mod_n))),
        Transform::Truncate(width) => Ok(Box::new(truncate::Truncate::new(*width))),
        Transform::Unknown => Err(crate::error::Error::new(
            crate::ErrorKind::FeatureUnsupported,
            "Transform Unknown is not implemented",
        )),
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::Result;
    use crate::expr::accessor::StructAccessor;
    use crate::expr::{
        BinaryExpression, BoundPredicate, BoundReference, PredicateOperator, SetExpression,
    };
    use crate::spec::{Datum, NestedField, NestedFieldRef, PrimitiveType, Transform, Type};

    /// A utitily struct, test fixture
    /// used for testing the projection on `Transform`
    pub(crate) struct TestProjectionFixture {
        transform: Transform,
        name: String,
        field: NestedFieldRef,
    }

    impl TestProjectionFixture {
        pub(crate) fn new(
            transform: Transform,
            name: impl Into<String>,
            field: NestedField,
        ) -> Self {
            TestProjectionFixture {
                transform,
                name: name.into(),
                field: Arc::new(field),
            }
        }
        pub(crate) fn binary_predicate(
            &self,
            op: PredicateOperator,
            literal: Datum,
        ) -> BoundPredicate {
            BoundPredicate::Binary(BinaryExpression::new(
                op,
                BoundReference::new(
                    self.name.clone(),
                    self.field.clone(),
                    Arc::new(StructAccessor::new(1, PrimitiveType::Boolean)),
                ),
                literal,
            ))
        }
        pub(crate) fn set_predicate(
            &self,
            op: PredicateOperator,
            literals: Vec<Datum>,
        ) -> BoundPredicate {
            BoundPredicate::Set(SetExpression::new(
                op,
                BoundReference::new(
                    self.name.clone(),
                    self.field.clone(),
                    Arc::new(StructAccessor::new(1, PrimitiveType::Boolean)),
                ),
                HashSet::from_iter(literals),
            ))
        }
        pub(crate) fn assert_projection(
            &self,
            predicate: &BoundPredicate,
            expected: Option<&str>,
        ) -> Result<()> {
            let result = self.transform.project(&self.name, predicate)?;
            match expected {
                Some(exp) => assert_eq!(format!("{}", result.unwrap()), exp),
                None => assert!(result.is_none()),
            }
            Ok(())
        }
    }

    /// A utility struct, test fixture
    /// used for testing the transform on `Transform`
    pub(crate) struct TestTransformFixture {
        pub display: String,
        pub json: String,
        pub dedup_name: String,
        pub preserves_order: bool,
        pub satisfies_order_of: Vec<(Transform, bool)>,
        pub trans_types: Vec<(Type, Option<Type>)>,
    }

    impl TestTransformFixture {
        #[track_caller]
        pub(crate) fn assert_transform(&self, trans: Transform) {
            assert_eq!(self.display, format!("{trans}"));
            assert_eq!(self.json, serde_json::to_string(&trans).unwrap());
            assert_eq!(trans, serde_json::from_str(self.json.as_str()).unwrap());
            assert_eq!(self.dedup_name, trans.dedup_name());
            assert_eq!(self.preserves_order, trans.preserves_order());

            for (other_trans, satisfies_order_of) in &self.satisfies_order_of {
                assert_eq!(
                    satisfies_order_of,
                    &trans.satisfies_order_of(other_trans),
                    "Failed to check satisfies order {trans}, {other_trans}, {satisfies_order_of}"
                );
            }

            for (i, (input_type, result_type)) in self.trans_types.iter().enumerate() {
                let actual = trans.result_type(input_type).ok();
                assert_eq!(
                    result_type, &actual,
                    "type mismatch at index {i}, input: {input_type}, expected: {result_type:?}, actual: {actual:?}"
                );
            }
        }
    }
}
