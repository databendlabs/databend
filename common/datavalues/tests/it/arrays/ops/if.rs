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

use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::NullArray;
use common_arrow::arrow::array::UInt64Array;
use common_arrow::arrow::compute::comparison::compare_scalar;
use common_arrow::arrow::compute::comparison::Operator;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::scalar::PrimitiveScalar;
use common_datavalues::prelude::*;
use common_exception::Result;

#[test]
fn test_array_if() -> Result<()> {
    let conds: Vec<DFBooleanArray> = vec![
        DFBooleanArray::new_from_slice(&[true, false, true]),
        DFBooleanArray::new_from_slice(&[true]),
    ];

    // DFPrimitiveType.
    let lhs = DFUInt16Array::new_from_slice(&[1u16]);
    let rhs = DFUInt16Array::new_from_slice(&[2u16, 4u16, 6u16]);
    let res = lhs.if_then_else(&rhs, &conds[0])?;
    assert_eq!(3, res.len());
    assert_eq!(1u16, res.inner().value(0));
    assert_eq!(4u16, res.inner().value(1));
    assert_eq!(1u16, res.inner().value(2));

    let lhs = DFUInt16Array::new_from_iter(1u16..4u16);
    let res = lhs.if_then_else(&rhs, &conds[0])?;
    assert_eq!(3, res.len());
    assert_eq!(1u16, res.inner().value(0));
    assert_eq!(4u16, res.inner().value(1));
    assert_eq!(3u16, res.inner().value(2));

    // DFBooleanArray.
    let lhs = DFBooleanArray::new_from_slice(&[true]);
    let rhs = DFBooleanArray::new_from_slice(&[false, false, true]);
    let res = lhs.if_then_else(&rhs, &conds[1])?;
    assert_eq!(1, res.len());
    assert!(res.inner().value(0));

    let res = rhs.if_then_else(&lhs, &conds[0])?;
    assert_eq!(3, res.len());
    assert!(!res.inner().value(0));
    assert!(res.inner().value(1));
    assert!(res.inner().value(2));

    // DFStringArray.
    let lhs = DFStringArray::new_from_slice(&["a"]);
    let rhs = DFStringArray::new_from_slice(&["b"]);
    let res = lhs.if_then_else(&rhs, &conds[0])?;
    assert_eq!(3, res.len());
    assert_eq!(b"a", res.inner().value(0));
    assert_eq!(b"b", res.inner().value(1));
    assert_eq!(b"a", res.inner().value(2));

    // DFNullArray.
    let lhs = NullArray::new_null(ArrowType::Null, 2);
    let lhs: DFNullArray = lhs.into();
    let rhs = NullArray::new_null(ArrowType::Null, 1);
    let rhs: DFNullArray = rhs.into();
    let res = lhs.if_then_else(&rhs, &conds[0])?;
    assert_eq!(2, res.len());

    Ok(())
}

#[test]
fn test_gt_u64_scalar() {
    use common_arrow::arrow::datatypes::DataType as ArrowDataType;
    let a = UInt64Array::from_slice(vec![0, 1, 2]);
    let c = compare_scalar(
        &a,
        &PrimitiveScalar::new(ArrowDataType::UInt64, Some(1u64)),
        Operator::Gt,
    )
    .unwrap();
    assert_eq!(BooleanArray::from_slice(vec![false, false, true]), c);
}
