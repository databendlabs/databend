// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::DictionaryArray;
use arrow_array::Int32Array;
use arrow_array::StringArray;
use databend_common_expression::Column;
use databend_common_expression::types::DataType;

#[test]
fn test_from_arrow_rs() {
    let array = Arc::new(StringArray::from(vec![
        Some("foo"),
        Some("bar"),
        None,
        Some("baz"),
    ])) as ArrayRef;

    let array_k = Int32Array::from(vec![0, 3, 2, 2]);
    let array_v = Arc::new(StringArray::from(vec![
        Some("foo"),
        Some("bar"),
        None,
        Some("baz"),
    ])) as ArrayRef;

    let array_dict = Arc::new(DictionaryArray::new(array_k, array_v)) as _;

    for arr in [array, array_dict] {
        let column = Column::from_arrow_rs(arr.clone(), &DataType::String.wrap_nullable()).unwrap();
        let c = column.into_arrow_rs();
        assert_eq!(c.len(), arr.len());
    }
}
