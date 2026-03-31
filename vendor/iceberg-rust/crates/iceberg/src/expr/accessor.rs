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

use std::sync::Arc;

use serde_derive::{Deserialize, Serialize};

use crate::spec::{Datum, Literal, PrimitiveType, Struct};
use crate::{Error, ErrorKind, Result};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct StructAccessor {
    position: usize,
    r#type: PrimitiveType,
    inner: Option<Box<StructAccessor>>,
}

pub(crate) type StructAccessorRef = Arc<StructAccessor>;

impl StructAccessor {
    pub(crate) fn new(position: usize, r#type: PrimitiveType) -> Self {
        StructAccessor {
            position,
            r#type,
            inner: None,
        }
    }

    pub(crate) fn wrap(position: usize, inner: Box<StructAccessor>) -> Self {
        StructAccessor {
            position,
            r#type: inner.r#type().clone(),
            inner: Some(inner),
        }
    }

    pub(crate) fn position(&self) -> usize {
        self.position
    }

    pub(crate) fn r#type(&self) -> &PrimitiveType {
        &self.r#type
    }

    pub(crate) fn get<'a>(&'a self, container: &'a Struct) -> Result<Option<Datum>> {
        match &self.inner {
            None => match &container[self.position] {
                None => Ok(None),
                Some(Literal::Primitive(literal)) => {
                    Ok(Some(Datum::new(self.r#type().clone(), literal.clone())))
                }
                Some(_) => Err(Error::new(
                    ErrorKind::Unexpected,
                    "Expected Literal to be Primitive",
                )),
            },
            Some(inner) => {
                if let Some(Literal::Struct(wrapped)) = &container[self.position] {
                    inner.get(wrapped)
                } else {
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        "Nested accessor should only be wrapping a Struct",
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::expr::accessor::StructAccessor;
    use crate::spec::{Datum, Literal, PrimitiveType, Struct};

    #[test]
    fn test_single_level_accessor() {
        let accessor = StructAccessor::new(1, PrimitiveType::Boolean);

        assert_eq!(accessor.r#type(), &PrimitiveType::Boolean);
        assert_eq!(accessor.position(), 1);

        let test_struct =
            Struct::from_iter(vec![Some(Literal::bool(false)), Some(Literal::bool(true))]);

        assert_eq!(accessor.get(&test_struct).unwrap(), Some(Datum::bool(true)));
    }

    #[test]
    fn test_single_level_accessor_null() {
        let accessor = StructAccessor::new(1, PrimitiveType::Boolean);

        assert_eq!(accessor.r#type(), &PrimitiveType::Boolean);
        assert_eq!(accessor.position(), 1);

        let test_struct = Struct::from_iter(vec![Some(Literal::bool(false)), None]);

        assert_eq!(accessor.get(&test_struct).unwrap(), None);
    }

    #[test]
    fn test_nested_accessor() {
        let nested_accessor = StructAccessor::new(1, PrimitiveType::Boolean);
        let accessor = StructAccessor::wrap(2, Box::new(nested_accessor));

        assert_eq!(accessor.r#type(), &PrimitiveType::Boolean);
        //assert_eq!(accessor.position(), 1);

        let nested_test_struct =
            Struct::from_iter(vec![Some(Literal::bool(false)), Some(Literal::bool(true))]);

        let test_struct = Struct::from_iter(vec![
            Some(Literal::bool(false)),
            Some(Literal::bool(false)),
            Some(Literal::Struct(nested_test_struct)),
        ]);

        assert_eq!(accessor.get(&test_struct).unwrap(), Some(Datum::bool(true)));
    }

    #[test]
    fn test_nested_accessor_null() {
        let nested_accessor = StructAccessor::new(0, PrimitiveType::Boolean);
        let accessor = StructAccessor::wrap(2, Box::new(nested_accessor));

        assert_eq!(accessor.r#type(), &PrimitiveType::Boolean);
        //assert_eq!(accessor.position(), 1);

        let nested_test_struct = Struct::from_iter(vec![None, Some(Literal::bool(true))]);

        let test_struct = Struct::from_iter(vec![
            Some(Literal::bool(false)),
            Some(Literal::bool(false)),
            Some(Literal::Struct(nested_test_struct)),
        ]);

        assert_eq!(accessor.get(&test_struct).unwrap(), None);
    }
}
