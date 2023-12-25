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
fn basics() {
    let dt = DataType::Struct(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let data_type = DataType::Map(Box::new(Field::new("a", dt.clone(), true)), false);

    let field = StructArray::new(
        dt.clone(),
        vec![
            Box::new(Utf8Array::<i32>::from_slice(["a", "aa", "aaa"])) as _,
            Box::new(Utf8Array::<i32>::from_slice(["b", "bb", "bbb"])),
        ],
        None,
    );

    let array = MapArray::new(
        data_type,
        vec![0, 1, 2].try_into().unwrap(),
        Box::new(field),
        None,
    );

    assert_eq!(
        array.value(0),
        Box::new(StructArray::new(
            dt.clone(),
            vec![
                Box::new(Utf8Array::<i32>::from_slice(["a"])) as _,
                Box::new(Utf8Array::<i32>::from_slice(["b"])),
            ],
            None,
        )) as Box<dyn Array>
    );

    let sliced = array.sliced(1, 1);
    assert_eq!(
        sliced.value(0),
        Box::new(StructArray::new(
            dt,
            vec![
                Box::new(Utf8Array::<i32>::from_slice(["aa"])) as _,
                Box::new(Utf8Array::<i32>::from_slice(["bb"])),
            ],
            None,
        )) as Box<dyn Array>
    );
}
