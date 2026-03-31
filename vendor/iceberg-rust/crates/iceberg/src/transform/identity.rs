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

use arrow_array::ArrayRef;

use super::TransformFunction;
use crate::Result;

/// Return identity array.
#[derive(Debug)]
pub struct Identity {}

impl TransformFunction for Identity {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        Ok(input)
    }

    fn transform_literal(&self, input: &crate::spec::Datum) -> Result<Option<crate::spec::Datum>> {
        Ok(Some(input.clone()))
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
    fn test_identity_transform() {
        let trans = Transform::Identity;

        let fixture = TestTransformFixture {
            display: "identity".to_string(),
            json: r#""identity""#.to_string(),
            dedup_name: "identity".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Truncate(4), true),
                (Transform::Truncate(2), true),
                (Transform::Bucket(4), false),
                (Transform::Void, false),
                (Transform::Day, true),
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
                    None,
                ),
            ],
        };

        fixture.assert_transform(trans);
    }
}
