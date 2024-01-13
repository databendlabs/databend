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
use databend_common_arrow::arrow::datatypes::*;
use databend_common_arrow::arrow::scalar::new_scalar;

#[test]
fn test_simple_iter() {
    let boolean = BooleanArray::from_slice([false, false, true, true]).boxed();
    let int = Int32Array::from_slice([42, 28, 19, 31]).boxed();

    let fields = vec![
        Field::new("b", DataType::Boolean, false),
        Field::new("c", DataType::Int32, false),
    ];

    let array = StructArray::new(
        DataType::Struct(fields),
        vec![boolean.clone(), int.clone()],
        None,
    );

    for (i, item) in array.iter().enumerate() {
        let expected = Some(vec![
            new_scalar(boolean.as_ref(), i),
            new_scalar(int.as_ref(), i),
        ]);
        assert_eq!(expected, item);
    }
}
