// Copyright 2020-2022 Jorge C. Leitão
// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_arrow::arrow::array::BooleanArray;
use databend_common_arrow::arrow::array::StructArray;
use databend_common_arrow::arrow::array::Utf8Array;
use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::datatypes::Field;
use databend_common_arrow::arrow::scalar::MapScalar;
use databend_common_arrow::arrow::scalar::Scalar;

#[allow(clippy::eq_op)]
#[test]
fn equal() {
    let kv_dt = DataType::Struct(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Boolean, true),
    ]);
    let kv_array1 = StructArray::try_new(
        kv_dt.clone(),
        vec![
            Utf8Array::<i32>::from([Some("k1"), Some("k2")]).boxed(),
            BooleanArray::from_slice([true, false]).boxed(),
        ],
        None,
    )
    .unwrap();
    let kv_array2 = StructArray::try_new(
        kv_dt.clone(),
        vec![
            Utf8Array::<i32>::from([Some("k1"), Some("k3")]).boxed(),
            BooleanArray::from_slice([true, true]).boxed(),
        ],
        None,
    )
    .unwrap();

    let dt = DataType::Map(Box::new(Field::new("entries", kv_dt, true)), false);
    let a = MapScalar::new(dt.clone(), Some(Box::new(kv_array1)));
    let b = MapScalar::new(dt.clone(), None);
    assert_eq!(a, a);
    assert_eq!(b, b);
    assert!(a != b);
    let b = MapScalar::new(dt, Some(Box::new(kv_array2)));
    assert!(a != b);
    assert_eq!(b, b);
}

#[test]
fn basics() {
    let kv_dt = DataType::Struct(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Boolean, true),
    ]);
    let kv_array = StructArray::try_new(
        kv_dt.clone(),
        vec![
            Utf8Array::<i32>::from([Some("k1"), Some("k2")]).boxed(),
            BooleanArray::from_slice([true, false]).boxed(),
        ],
        None,
    )
    .unwrap();

    let dt = DataType::Map(Box::new(Field::new("entries", kv_dt, true)), false);
    let a = MapScalar::new(dt.clone(), Some(Box::new(kv_array.clone())));

    assert_eq!(kv_array, a.values().as_ref());
    assert_eq!(a.data_type(), &dt);
    assert!(a.is_valid());

    let _: &dyn std::any::Any = a.as_any();
}
