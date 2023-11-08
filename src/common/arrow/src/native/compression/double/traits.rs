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

use std::hash::Hash;
use std::ops::BitXor;
use std::ops::Shl;
use std::ops::ShlAssign;
use std::ops::Shr;
use std::ops::ShrAssign;

use num::Float;
use ordered_float::OrderedFloat;

use crate::arrow::types::NativeType;
use crate::native::util::AsBytes;

pub trait DoubleType: AsBytes + Copy + Clone + NativeType + Float {
    type OrderType: std::fmt::Debug
        + std::fmt::Display
        + Eq
        + Hash
        + PartialOrd
        + Hash
        + Copy
        + Clone;

    type BitType: Eq
        + NativeType
        + Hash
        + PartialOrd
        + Hash
        + AsBytes
        + BitXor<Output = Self::BitType>
        + ShlAssign
        + Shl<usize, Output = Self::BitType>
        + Shr<usize, Output = Self::BitType>
        + ShrAssign;

    fn as_order(&self) -> Self::OrderType;

    fn from_order(order: Self::OrderType) -> Self;

    fn as_bits(&self) -> Self::BitType;
    fn from_bits_val(bits: Self::BitType) -> Self;

    fn leading_zeros(bit_value: &Self::BitType) -> u32;
    fn trailing_zeros(bit_value: &Self::BitType) -> u32;
}

macro_rules! double_type {
    ($type:ty, $order_type: ty,  $bit_type: ty) => {
        impl DoubleType for $type {
            type OrderType = $order_type;
            type BitType = $bit_type;

            fn as_order(&self) -> Self::OrderType {
                OrderedFloat(*self)
            }

            fn from_order(order: Self::OrderType) -> Self {
                order.0
            }

            fn as_bits(&self) -> Self::BitType {
                self.to_bits()
            }

            fn from_bits_val(bits: Self::BitType) -> Self {
                Self::from_bits(bits)
            }

            fn leading_zeros(bit_value: &Self::BitType) -> u32 {
                bit_value.leading_zeros()
            }

            fn trailing_zeros(bit_value: &Self::BitType) -> u32 {
                bit_value.trailing_zeros()
            }
        }
    };
}

type F32 = OrderedFloat<f32>;
type F64 = OrderedFloat<f64>;

double_type!(f32, F32, u32);
double_type!(f64, F64, u64);
