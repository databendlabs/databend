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
use databend_common_arrow::arrow::datatypes::Field;
use databend_common_arrow::arrow::scalar::BooleanScalar;
use databend_common_arrow::arrow::scalar::Scalar;
use databend_common_arrow::arrow::scalar::StructScalar;

#[allow(clippy::eq_op)]
#[test]
fn equal() {
    let dt = DataType::Struct(vec![Field::new("a", DataType::Boolean, true)]);
    let a = StructScalar::new(
        dt.clone(),
        Some(vec![
            Box::new(BooleanScalar::from(Some(true))) as Box<dyn Scalar>
        ]),
    );
    let b = StructScalar::new(dt.clone(), None);
    assert_eq!(a, a);
    assert_eq!(b, b);
    assert!(a != b);
    let b = StructScalar::new(
        dt,
        Some(vec![
            Box::new(BooleanScalar::from(Some(false))) as Box<dyn Scalar>
        ]),
    );
    assert!(a != b);
    assert_eq!(b, b);
}

#[test]
fn basics() {
    let dt = DataType::Struct(vec![Field::new("a", DataType::Boolean, true)]);

    let values = vec![Box::new(BooleanScalar::from(Some(true))) as Box<dyn Scalar>];

    let a = StructScalar::new(dt.clone(), Some(values.clone()));

    assert_eq!(a.values(), &values);
    assert_eq!(a.data_type(), &dt);
    assert!(a.is_valid());

    let _: &dyn std::any::Any = a.as_any();
}
