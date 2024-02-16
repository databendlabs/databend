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

//! Sealed traits and implementations to handle all _physical types_ used in this crate.
//!
//! Most physical types used in this crate are native Rust types, such as `i32`.
//! The trait [`NativeType`] describes the interfaces required by this crate to be conformant
//! with Arrow.
//!
//! Every implementation of [`NativeType`] has an associated variant in [`PrimitiveType`],
//! available via [`NativeType::PRIMITIVE`].
//! Combined, these allow structs generic over [`NativeType`] to be trait objects downcastable
//! to concrete implementations based on the matched [`NativeType::PRIMITIVE`] variant.
//!
//! Another important trait in this module is [`Offset`], the subset of [`NativeType`] that can
//! be used in Arrow offsets (`i32` and `i64`).
//!
//! Another important trait in this module is [`BitChunk`], describing types that can be used to
//! represent chunks of bits (e.g. 8 bits via `u8`, 16 via `u16`), and [`BitChunkIter`],
//! that can be used to iterate over bitmaps in [`BitChunk`]s according to
//! Arrow's definition of bitmaps.
//!
//! Finally, this module contains traits used to compile code based on [`NativeType`] optimized
//! for SIMD, at [`mod@simd`].

mod bit_chunk;
pub use bit_chunk::BitChunk;
pub use bit_chunk::BitChunkIter;
pub use bit_chunk::BitChunkOnes;
mod index;
pub mod simd;
pub use index::*;
mod native;
pub use native::*;
mod offset;
pub use offset::*;
#[cfg(feature = "serde_types")]
use serde_derive::Deserialize;
#[cfg(feature = "serde_types")]
use serde_derive::Serialize;

/// The set of all implementations of the sealed trait [`NativeType`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde_types", derive(Serialize, Deserialize))]
pub enum PrimitiveType {
    /// A signed 8-bit integer.
    Int8,
    /// A signed 16-bit integer.
    Int16,
    /// A signed 32-bit integer.
    Int32,
    /// A signed 64-bit integer.
    Int64,
    /// A signed 128-bit integer.
    Int128,
    /// A signed 256-bit integer.
    Int256,
    /// An unsigned 8-bit integer.
    UInt8,
    /// An unsigned 16-bit integer.
    UInt16,
    /// An unsigned 32-bit integer.
    UInt32,
    /// An unsigned 64-bit integer.
    UInt64,
    /// An unsigned 128-bit integer.
    UInt128,
    /// A 16-bit floating point number.
    Float16,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
    /// Two i32 representing days and ms
    DaysMs,
    /// months_days_ns(i32, i32, i64)
    MonthDayNano,
}

mod private {
    use crate::arrow::array::View;

    pub trait Sealed {}

    impl Sealed for u8 {}
    impl Sealed for u16 {}
    impl Sealed for u32 {}
    impl Sealed for u64 {}
    impl Sealed for i8 {}
    impl Sealed for i16 {}
    impl Sealed for i32 {}
    impl Sealed for i64 {}
    impl Sealed for i128 {}
    impl Sealed for u128 {}
    impl Sealed for super::i256 {}
    impl Sealed for super::f16 {}
    impl Sealed for f32 {}
    impl Sealed for f64 {}
    impl Sealed for super::days_ms {}
    impl Sealed for super::months_days_ns {}
    impl Sealed for View {}
}
