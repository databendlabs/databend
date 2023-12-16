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

use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::scalar::Scalar;
use databend_common_arrow::arrow::scalar::Utf8Scalar;

#[allow(clippy::eq_op)]
#[test]
fn equal() {
    let a = Utf8Scalar::<i32>::from(Some("a"));
    let b = Utf8Scalar::<i32>::from(None::<&str>);
    assert_eq!(a, a);
    assert_eq!(b, b);
    assert!(a != b);
    let b = Utf8Scalar::<i32>::from(Some("b"));
    assert!(a != b);
    assert_eq!(b, b);
}

#[test]
fn basics() {
    let a = Utf8Scalar::<i32>::from(Some("a"));

    assert_eq!(a.value(), Some("a"));
    assert_eq!(a.data_type(), &DataType::Utf8);
    assert!(a.is_valid());

    let a = Utf8Scalar::<i64>::from(None::<&str>);

    assert_eq!(a.data_type(), &DataType::LargeUtf8);
    assert!(!a.is_valid());

    let _: &dyn std::any::Any = a.as_any();
}
