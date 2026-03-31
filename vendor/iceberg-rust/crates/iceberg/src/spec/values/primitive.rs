// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Primitive literal types

use ordered_float::OrderedFloat;

/// Values present in iceberg type
#[derive(Clone, Debug, PartialOrd, PartialEq, Hash, Eq)]
pub enum PrimitiveLiteral {
    /// 0x00 for false, non-zero byte for true
    Boolean(bool),
    /// Stored as 4-byte little-endian
    Int(i32),
    /// Stored as 8-byte little-endian
    Long(i64),
    /// Stored as 4-byte little-endian
    Float(OrderedFloat<f32>),
    /// Stored as 8-byte little-endian
    Double(OrderedFloat<f64>),
    /// UTF-8 bytes (without length)
    String(String),
    /// Binary value (without length)
    Binary(Vec<u8>),
    /// Stored as 16-byte little-endian
    Int128(i128),
    /// Stored as 16-byte little-endian
    UInt128(u128),
    /// When a number is larger than it can hold
    AboveMax,
    /// When a number is smaller than it can hold
    BelowMin,
}

impl PrimitiveLiteral {
    /// Returns true if the Literal represents a primitive type
    /// that can be a NaN, and that it's value is NaN
    pub fn is_nan(&self) -> bool {
        match self {
            PrimitiveLiteral::Double(val) => val.is_nan(),
            PrimitiveLiteral::Float(val) => val.is_nan(),
            _ => false,
        }
    }
}
