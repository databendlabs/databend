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

use databend_common_arrow::arrow::array::*;
use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::datatypes::Field;

#[test]
fn push() {
    let c1 = Box::new(MutablePrimitiveArray::<i32>::new()) as Box<dyn MutableArray>;
    let values = vec![c1];
    let data_type = DataType::Struct(vec![Field::new("f1", DataType::Int32, true)]);
    let mut a = MutableStructArray::new(data_type, values);

    a.value::<MutablePrimitiveArray<i32>>(0)
        .unwrap()
        .push(Some(1));
    a.push(true);
    a.value::<MutablePrimitiveArray<i32>>(0).unwrap().push(None);
    a.push(false);
    a.value::<MutablePrimitiveArray<i32>>(0)
        .unwrap()
        .push(Some(2));
    a.push(true);

    assert_eq!(a.len(), 3);
    assert!(a.is_valid(0));
    assert!(!a.is_valid(1));
    assert!(a.is_valid(2));

    assert_eq!(
        a.value::<MutablePrimitiveArray<i32>>(0).unwrap().values(),
        &Vec::from([1, 0, 2])
    );
}
