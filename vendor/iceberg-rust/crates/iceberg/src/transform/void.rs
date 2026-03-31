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

use arrow_array::{ArrayRef, new_null_array};

use super::TransformFunction;
use crate::Result;

#[derive(Debug)]
pub struct Void {}

impl TransformFunction for Void {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        Ok(new_null_array(input.data_type(), input.len()))
    }

    fn transform_literal(&self, _input: &crate::spec::Datum) -> Result<Option<crate::spec::Datum>> {
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use crate::spec::PrimitiveType::{
        Binary, Date, Decimal, Fixed, Int, Long, String as StringType, Time, Timestamp,
        TimestampNs, Timestamptz, TimestamptzNs, Uuid,
    };
    use crate::spec::Type::{Primitive, Struct};
    use crate::spec::{NestedField, StructType, Transform};
    use crate::transform::test::TestTransformFixture;

    #[test]
    fn test_void_transform() {
        let trans = Transform::Void;

        let fixture = TestTransformFixture {
            display: "void".to_string(),
            json: r#""void""#.to_string(),
            dedup_name: "void".to_string(),
            preserves_order: false,
            satisfies_order_of: vec![
                (Transform::Year, false),
                (Transform::Month, false),
                (Transform::Day, false),
                (Transform::Hour, false),
                (Transform::Void, true),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), Some(Primitive(Binary))),
                (Primitive(Date), Some(Primitive(Date))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    Some(Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    })),
                ),
                (Primitive(Fixed(8)), Some(Primitive(Fixed(8)))),
                (Primitive(Int), Some(Primitive(Int))),
                (Primitive(Long), Some(Primitive(Long))),
                (Primitive(StringType), Some(Primitive(StringType))),
                (Primitive(Uuid), Some(Primitive(Uuid))),
                (Primitive(Time), Some(Primitive(Time))),
                (Primitive(Timestamp), Some(Primitive(Timestamp))),
                (Primitive(Timestamptz), Some(Primitive(Timestamptz))),
                (Primitive(TimestampNs), Some(Primitive(TimestampNs))),
                (Primitive(TimestamptzNs), Some(Primitive(TimestamptzNs))),
                (
                    Struct(StructType::new(vec![
                        NestedField::optional(1, "a", Primitive(Timestamp)).into(),
                    ])),
                    Some(Struct(StructType::new(vec![
                        NestedField::optional(1, "a", Primitive(Timestamp)).into(),
                    ]))),
                ),
            ],
        };

        fixture.assert_transform(trans);
    }

    #[test]
    fn test_known_transform() {
        let trans = Transform::Unknown;

        let fixture = TestTransformFixture {
            display: "unknown".to_string(),
            json: r#""unknown""#.to_string(),
            dedup_name: "unknown".to_string(),
            preserves_order: false,
            satisfies_order_of: vec![
                (Transform::Year, false),
                (Transform::Month, false),
                (Transform::Day, false),
                (Transform::Hour, false),
                (Transform::Void, false),
                (Transform::Identity, false),
                (Transform::Unknown, true),
            ],
            trans_types: vec![
                (Primitive(Binary), Some(Primitive(StringType))),
                (Primitive(Date), Some(Primitive(StringType))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    Some(Primitive(StringType)),
                ),
                (Primitive(Fixed(8)), Some(Primitive(StringType))),
                (Primitive(Int), Some(Primitive(StringType))),
                (Primitive(Long), Some(Primitive(StringType))),
                (Primitive(StringType), Some(Primitive(StringType))),
                (Primitive(Uuid), Some(Primitive(StringType))),
                (Primitive(Time), Some(Primitive(StringType))),
                (Primitive(Timestamp), Some(Primitive(StringType))),
                (Primitive(Timestamptz), Some(Primitive(StringType))),
                (
                    Struct(StructType::new(vec![
                        NestedField::optional(1, "a", Primitive(Timestamp)).into(),
                    ])),
                    Some(Primitive(StringType)),
                ),
            ],
        };

        fixture.assert_transform(trans);
    }
}
