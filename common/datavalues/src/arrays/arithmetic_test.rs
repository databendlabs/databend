// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;

use crate::prelude::*;

#[test]
fn test_add() -> Result<()> {
    // array=[null, 1, 2, null, 4]
    let array1 = DFUInt16Array::new_from_opt_slice(&[Some(5), Some(5), Some(10)]);
    // array=[5, null, 7, 8, null]
    let array2 = DFUInt16Array::new_from_opt_slice(&[Some(5), None, None]);

    let array = (&array1 + &array2).unwrap();
    let values = array.collect_values();
    assert_eq!(values, vec![Some(10), None, None]);
    Ok(())
}

#[test]
fn test_mul() -> Result<()> {
    // array=[null, 1, 2, null, 4]
    let array1 = DFUInt16Array::new_from_opt_slice(&[Some(2), Some(5), Some(10)]);
    // array=[5, null, 7, 8, null]
    let array2 = DFUInt16Array::new_from_opt_slice(&[Some(7u16), None, None]);
    let array = (&array1 * &array2).unwrap();
    assert_eq!(array.collect_values(), vec![Some(14), None, None]);
    Ok(())
}
