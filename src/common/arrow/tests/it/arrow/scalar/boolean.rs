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
use databend_common_arrow::arrow::scalar::BooleanScalar;
use databend_common_arrow::arrow::scalar::Scalar;

#[allow(clippy::eq_op)]
#[test]
fn equal() {
    let a = BooleanScalar::from(Some(true));
    let b = BooleanScalar::from(None);
    assert_eq!(a, a);
    assert_eq!(b, b);
    assert!(a != b);
    let b = BooleanScalar::from(Some(false));
    assert!(a != b);
    assert_eq!(b, b);
}

#[test]
fn basics() {
    let a = BooleanScalar::new(Some(true));

    assert_eq!(a.value(), Some(true));
    assert_eq!(a.data_type(), &DataType::Boolean);
    assert!(a.is_valid());

    let _: &dyn std::any::Any = a.as_any();
}
