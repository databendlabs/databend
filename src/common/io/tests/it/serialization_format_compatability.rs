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

use std::assert_matches::assert_matches;
use std::io::Cursor;

use databend_common_io::prelude::bincode_deserialize_from_slice;
use databend_common_io::prelude::bincode_serialize_into_buf;
use serde::Deserialize;
use serde::Serialize;

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

#[test]
fn test_msg_pack_enum_reorder() {
    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    enum ScalarOrigin {
        Null,
        Int(i8),
        String(Vec<u8>),
        Float(f32),
        Binary(Vec<u8>),
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    enum ScalarReordered {
        Null,
        Int(i8),
        Binary(Vec<u8>),
        String(Vec<u8>),
        Float(f32),
    }

    let value = vec![ScalarOrigin::Null, ScalarOrigin::String(vec![1, 2, 3])];
    let bytes = rmp_serde::to_vec_named(&value).unwrap();
    let deserialized: Vec<ScalarReordered> = rmp_serde::from_slice(&bytes).unwrap();

    assert_matches!(deserialized[0], ScalarReordered::Null);
    assert_matches!(&deserialized[1], ScalarReordered::String(x) if x == &vec![1, 2, 3]);
}

#[test]
fn test_serde_other_alias() {
    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    enum Scalar1 {
        Null,
        Int(i8),
        String(Vec<u8>),
        Float(f32),
        Variant(Vec<u8>),
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    enum Scalar2 {
        Null,
        Int(i8),
        Binary(Vec<u8>),
        String(Vec<u8>),
        Float(f32),
        Variant(Vec<u8>),
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    enum Scalar3 {
        Null,
        Int(i8),
        #[serde(alias = "String", alias = "Binary")]
        String(Vec<u8>),
        #[serde(other)]
        Other,
    }

    // test v1-v3 with rmp
    let mut value1 = vec![
        Scalar1::Null,
        Scalar1::Int(42),
        Scalar1::String(vec![3, 4, 5]),
    ];
    let bytes = rmp_serde::to_vec_named(&value1).unwrap();
    let deserialized: Vec<Scalar3> = rmp_serde::from_slice(&bytes).unwrap();
    let e3 = vec![
        Scalar3::Null,
        Scalar3::Int(42),
        Scalar3::String(vec![3, 4, 5]),
    ];
    assert_eq!(deserialized, e3);

    // test v1-v3 with bincode
    let mut writer = vec![];
    bincode_serialize_into_buf(&mut writer, &value1).unwrap();
    let deserialized: Vec<Scalar3> = bincode_deserialize_from_slice(&writer).unwrap();
    assert_eq!(deserialized, e3);

    // test other
    value1.push(Scalar1::Float(3.2));
    let bytes = rmp_serde::to_vec_named(&value1).unwrap();
    let deserialized: Result<Vec<Scalar3>, _> = rmp_serde::from_slice(&bytes);
    assert!(deserialized.is_err());

    // test v2-v3 with rpm and binary to string
    let value2 = vec![
        Scalar2::Null,
        Scalar2::Binary(vec![1, 2, 3]),
        Scalar2::Int(42),
        Scalar2::String(vec![3, 4, 5]),
    ];
    let bytes = rmp_serde::to_vec_named(&value2).unwrap();
    let deserialized: Vec<Scalar3> = rmp_serde::from_slice(&bytes).unwrap();
    let e3 = vec![
        Scalar3::Null,
        Scalar3::String(vec![1, 2, 3]),
        Scalar3::Int(42),
        Scalar3::String(vec![3, 4, 5]),
    ];
    assert_eq!(deserialized, e3);
}
