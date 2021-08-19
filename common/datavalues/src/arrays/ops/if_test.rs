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

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::NullArray;
use common_exception::Result;

use crate::prelude::*;

#[test]
fn test_array_if() -> Result<()> {
    let conds: Vec<DFBooleanArray> = vec![
        DFBooleanArray::new_from_slice(&vec![true, false, true]),
        DFBooleanArray::new_from_slice(&vec![true]),
    ];

    // DFNumericType.
    let lhs = DFUInt16Array::new_from_slice(&[1u16]);
    let rhs = DFUInt16Array::new_from_slice(&[2u16, 4u16, 6u16]);
    let res = lhs.if_then_else(&rhs, &conds[0])?;
    assert_eq!(3, res.len());
    assert_eq!(1u16, res.as_ref().value(0));
    assert_eq!(4u16, res.as_ref().value(1));
    assert_eq!(1u16, res.as_ref().value(2));

    let lhs = DFUInt16Array::new_from_iter(1u16..4u16);
    let res = lhs.if_then_else(&rhs, &conds[0])?;
    assert_eq!(3, res.len());
    assert_eq!(1u16, res.as_ref().value(0));
    assert_eq!(4u16, res.as_ref().value(1));
    assert_eq!(3u16, res.as_ref().value(2));

    // DFBooleanArray.
    let lhs = DFBooleanArray::new_from_slice(&[true]);
    let rhs = DFBooleanArray::new_from_slice(&[false, false, true]);
    let res = lhs.if_then_else(&rhs, &conds[1])?;
    assert_eq!(1, res.len());
    assert_eq!(true, res.as_ref().value(0));

    let res = rhs.if_then_else(&lhs, &conds[0])?;
    assert_eq!(3, res.len());
    assert_eq!(false, res.as_ref().value(0));
    assert_eq!(true, res.as_ref().value(1));
    assert_eq!(true, res.as_ref().value(2));

    // DFUtf8Array.
    let lhs = DFUtf8Array::new_from_slice(&["a"]);
    let rhs = DFUtf8Array::new_from_slice(&["b"]);
    let res = lhs.if_then_else(&rhs, &conds[0])?;
    assert_eq!(3, res.len());
    assert_eq!("a", res.as_ref().value(0));
    assert_eq!("b", res.as_ref().value(1));
    assert_eq!("a", res.as_ref().value(2));

    // DFNullArray.
    let lhs = Arc::new(NullArray::new_null(2)) as ArrayRef;
    let lhs: DFNullArray = lhs.into();
    let rhs = Arc::new(NullArray::new_null(1)) as ArrayRef;
    let rhs: DFNullArray = rhs.into();
    let res = lhs.if_then_else(&rhs, &conds[0])?;
    assert_eq!(2, res.len());

    Ok(())
}
