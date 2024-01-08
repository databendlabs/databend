//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::io::Cursor;

use databend_common_io::prelude::bincode_deserialize_from_slice;
use databend_common_io::prelude::bincode_serialize_into_buf;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct BasicOld {
    a: u32,
    b: u32,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Basic {
    a: u32,
    b: u32,
    new_string: Option<String>,
    #[serde(default = "default_new_int")]
    new_int: u32,
}

fn default_new_int() -> u32 {
    100
}

#[cfg(with_pot)]
#[test]
fn test_pot_backward_compat() {
    let old = BasicOld { a: 1, b: 2 };
    let bytes = pot::to_vec(&old).unwrap();
    let new: Basic = pot::from_slice(&bytes).unwrap();

    assert_eq!(new.a, 1);
    assert_eq!(new.b, 2);
    assert_eq!(new.new_string, None);
    assert_eq!(new.new_int, 100);
}

#[test]
fn test_msgpack_backward_compat() {
    let old_format = BasicOld { a: 1, b: 2 };

    // msgpack without schema, NOT backward compatible
    let bytes = rmp_serde::to_vec(&old_format).unwrap();
    let old: Result<BasicOld, _> = rmp_serde::from_slice(&bytes);
    assert!(old.is_ok());
    let new: Result<Basic, _> = rmp_serde::from_slice(&bytes);
    assert!(new.is_err());

    // named messagepack is ok
    let bytes = rmp_serde::to_vec_named(&old_format).unwrap();
    let new: Basic = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(new.a, 1);
    assert_eq!(new.b, 2);
    assert_eq!(new.new_string, None);
    assert_eq!(new.new_int, 100);
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
enum OldEnum {
    A(String),
    B(u32),
}

// enum that extended with extra variant
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
enum NewEnumAppendField {
    A(String),
    B(u32),
    C(String),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
enum NewEnumInsertFieldInTheMiddle {
    A(String),
    C(String),
    B(u32),
}

#[test]
fn test_bincode_backward_compat_enum() {
    let old_format = OldEnum::B(100);

    let mut buffer = Cursor::new(Vec::new());
    bincode_serialize_into_buf(&mut buffer, &old_format).unwrap();
    let bytes = buffer.get_ref().as_slice();

    let _: OldEnum = bincode_deserialize_from_slice(bytes).unwrap();

    // enum extended with new field is ok
    let new: NewEnumAppendField = bincode_deserialize_from_slice(bytes).unwrap();
    assert_eq!(new, NewEnumAppendField::B(100));

    // enum, insert with new field in the middle, is NOT ok
    let new: Result<NewEnumInsertFieldInTheMiddle, _> = bincode_deserialize_from_slice(bytes);
    assert!(new.is_err())
}

#[test]
fn test_msgpack_backward_compat_enum() {
    // msgpack with schema
    let old_format = OldEnum::B(100);
    let bytes = rmp_serde::to_vec_named(&old_format).unwrap();
    let _old: OldEnum = rmp_serde::from_slice(&bytes).unwrap();

    // enum extended with new field is ok
    let new: NewEnumAppendField = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(new, NewEnumAppendField::B(100));

    // enum, insert with new field in the middle, is ok
    let new: NewEnumInsertFieldInTheMiddle = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(new, NewEnumInsertFieldInTheMiddle::B(100));

    // nested struct, backward compat

    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
    struct OldStruct {
        enum_field: OldEnum,
    }

    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
    struct NewStruct {
        enum_field: NewEnumInsertFieldInTheMiddle,
        new_string: Option<String>,
        #[serde(default = "default_new_int")]
        new_int: u32,
    }

    // enum backward compat test case: out of order enum evolution

    let olds = OldStruct {
        enum_field: OldEnum::B(100),
    };

    let bytes = rmp_serde::to_vec_named(&olds).unwrap();
    let _old: OldStruct = rmp_serde::from_slice(&bytes).unwrap();

    let new: NewStruct = rmp_serde::from_slice(&bytes).unwrap();

    assert_eq!(new.enum_field, NewEnumInsertFieldInTheMiddle::B(100));
    assert_eq!(new.new_string, None);
    assert_eq!(new.new_int, default_new_int());
}

/// A compat tests for scalar with old version into scalar of current version
#[test]
fn test_bincode_backward_compat_scalar() {
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::Scalar;
    let old_format = vec![
        Scalar::Null,
        Scalar::String(vec![0, 1, 2]),
        Scalar::Bitmap(vec![1, 2, 3]),
        Scalar::Variant(vec![2, 3, 4]),
        Scalar::Number(NumberScalar::Int16(100)),
        Scalar::Boolean(false),
    ];

    // data generated from old version v1.2.262-nightly
    // let mut buffer = vec![];
    // bincode_serialize_into_buf(&mut buffer, &old_format).unwrap();
    // println!("{:?}", buffer);
    let data = vec![
        6, 0, 8, 3, 0, 1, 2, 11, 3, 1, 2, 3, 13, 3, 2, 3, 4, 3, 5, 200, 7, 0,
    ];
    let new_format: Vec<Scalar> = bincode_deserialize_from_slice(&data).unwrap();

    assert_eq!(old_format, new_format);
}
