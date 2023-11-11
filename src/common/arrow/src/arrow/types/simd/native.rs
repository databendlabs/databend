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

use std::convert::TryInto;

use super::*;
use crate::arrow::types::BitChunkIter;

native_simd!(u8x64, u8, 64, u64);
native_simd!(u16x32, u16, 32, u32);
native_simd!(u32x16, u32, 16, u16);
native_simd!(u64x8, u64, 8, u8);
native_simd!(i8x64, i8, 64, u64);
native_simd!(i16x32, i16, 32, u32);
native_simd!(i32x16, i32, 16, u16);
native_simd!(i64x8, i64, 8, u8);
native_simd!(f16x32, f16, 32, u32);
native_simd!(f32x16, f32, 16, u16);
native_simd!(f64x8, f64, 8, u8);
