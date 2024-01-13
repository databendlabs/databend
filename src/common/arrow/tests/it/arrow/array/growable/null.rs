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

use databend_common_arrow::arrow::array::growable::Growable;
use databend_common_arrow::arrow::array::growable::GrowableNull;
use databend_common_arrow::arrow::array::NullArray;
use databend_common_arrow::arrow::datatypes::DataType;

#[test]
fn null() {
    let mut mutable = GrowableNull::default();

    mutable.extend(0, 1, 2);
    mutable.extend(1, 0, 1);
    assert_eq!(mutable.len(), 3);

    let result: NullArray = mutable.into();

    let expected = NullArray::new(DataType::Null, 3);
    assert_eq!(result, expected);
}
