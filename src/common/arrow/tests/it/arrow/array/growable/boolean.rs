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
use databend_common_arrow::arrow::array::growable::GrowableBoolean;
use databend_common_arrow::arrow::array::BooleanArray;

#[test]
fn test_bool() {
    let array = BooleanArray::from(vec![Some(false), Some(true), None, Some(false)]);

    let mut a = GrowableBoolean::new(vec![&array], false, 0);

    a.extend(0, 1, 2);
    assert_eq!(a.len(), 2);

    let result: BooleanArray = a.into();

    let expected = BooleanArray::from(vec![Some(true), None]);
    assert_eq!(result, expected);
}
