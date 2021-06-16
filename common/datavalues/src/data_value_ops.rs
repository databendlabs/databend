// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::marker::PhantomData;
use std::ops::Add;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::DFNumericType;
use crate::DataColumnarValue;
use crate::*;

// now work
pub trait ColumnAdd {
    fn add<L, R>(&self, rhs: &Self) -> Result<DataColumnarValue>
    where
        L: DFNumericType,
        R: DFNumericType;
}

impl ColumnAdd for DataColumnarValue {
    fn add<L, R>(&self, right: &Self) -> Result<DataColumnarValue>
    where
        L: DFNumericType,
        R: DFNumericType,
    {
        Ok(DataColumnarValue::Constant(
            DataValue::UInt32(Some(3u32)),
            3,
        ))
    }
}

trait IDataType {
    const TYPE_SIZE: usize;
    const IS_FLOAT: bool;
    type DataType: IDataType;
}

struct Condition<Left: IDataType, Right: IDataType, const Condition: bool> {
    left: PhantomData<Left>,
    right: PhantomData<Right>,
}

impl<Left: IDataType, Right: IDataType> IDataType for Condition<Left, Right, true> {
    const TYPE_SIZE: usize = Left::TYPE_SIZE;
    const IS_FLOAT: bool = Left::IS_FLOAT;
    type DataType = Left::DataType;
}

impl<Left: IDataType, Right: IDataType> IDataType for Condition<Left, Right, false> {
    const TYPE_SIZE: usize = Right::TYPE_SIZE;
    const IS_FLOAT: bool = Right::IS_FLOAT;
    type DataType = Right::DataType;
}

struct MaxSizeType<Left: IDataType, Right: IDataType> {
    left: PhantomData<Left>,
    right: PhantomData<Right>,
}

impl<Left: IDataType, Right: IDataType> IDataType for MaxSizeType<Left, Right>
where Condition<Left, Right, { Left::TYPE_SIZE >= Right::TYPE_SIZE }>: IDataType
{
    const TYPE_SIZE: usize =
        <Condition<Left, Right, { Left::TYPE_SIZE >= Right::TYPE_SIZE }> as IDataType>::TYPE_SIZE;

    const IS_FLOAT: bool =
        <Condition<Left, Right, { Left::TYPE_SIZE >= Right::TYPE_SIZE }> as IDataType>::IS_FLOAT;

    type DataType =
        <Condition<Left, Right, { Left::TYPE_SIZE >= Right::TYPE_SIZE }> as IDataType>::DataType;
}

impl<Left: IDataType, Right: IDataType> IDataType for MaxSizeType<Left, Right>
where Condition<Left, Right, { Left::TYPE_SIZE < Right::TYPE_SIZE }>: IDataType
{
    const TYPE_SIZE: usize =
        <Condition<Left, Right, { Left::TYPE_SIZE < Right::TYPE_SIZE }> as IDataType>::TYPE_SIZE;

    const IS_FLOAT: bool =
        <Condition<Left, Right, { Left::TYPE_SIZE < Right::TYPE_SIZE }> as IDataType>::IS_FLOAT;

    type DataType =
        <Condition<Left, Right, { Left::TYPE_SIZE < Right::TYPE_SIZE }> as IDataType>::DataType;
}

// impl IDataType for UInt8Type {
//     const TYPE_SIZE: usize = 1;
//     const IS_FLOAT: bool = false;
//     type DataType = Self;
// }

// struct AddType<const TYPE_SIZE: usize, const IS_FLOAT: bool> {}

// impl IDataType for AddType<1, false> {
//     const TYPE_SIZE: usize = 1;
//     const IS_FLOAT: bool = false;
//     type DataType = UInt8Type;
// }

// pub trait Arithmetic<L, R> {
//     fn add<L, R>(left: &DataColumnarValue, right: &DataColumnarValue) -> Result<DataColumnarValue>
//     where
//         L: DFNumericType,
//         R: DFNumericType,
//         E::Native: Add<Output = T::Native>,
//     {
//         Err(ErrorCode::UnImplement(
//             "Ops add is not implemented for {:?} add {:?}",
//             left,
//             right,
//         ))
//     }
// }

// /// Perform `left + right` operation on two arrays. If either left or right value is null
// /// then the result is also null.
// pub fn add<T>(
//     left: &PrimitiveArray<T>,
//     right: &PrimitiveArray<T>,
// ) -> Result<PrimitiveArray<T>>
// where
//     T: ArrowNumericType,
//     T::Native: Add<Output = T::Native>
//         + Sub<Output = T::Native>
//         + Mul<Output = T::Native>
//         + Div<Output = T::Native>
//         + Zero,
// {
//     #[cfg(feature = "simd")]
//     return simd_math_op(&left, &right, |a, b| a + b, |a, b| a + b);
//     #[cfg(not(feature = "simd"))]
//     return math_op(left, right, |a, b| a + b);
// }

// #[cfg(feature = "simd")]
// return simd_math_op(&left, &right, |a, b| a + b, |a, b| a + b);
// #[cfg(not(feature = "simd"))]
// return math_op(left, right, |a, b| a + b);
