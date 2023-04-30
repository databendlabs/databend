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

use crate::types::decimal::DecimalSize;
use crate::types::DataType;
use crate::types::DecimalDataType;
use crate::types::NumberDataType;

pub enum NumberClass {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Decimal128,
    Decimal256,
    Float32,
    Float64,
}

/// used for function register
/// do not change the order
pub const ALL_NUMBER_CLASSES: &[NumberClass] = &[
    NumberClass::UInt8,
    NumberClass::Int8,
    NumberClass::UInt16,
    NumberClass::Int16,
    NumberClass::UInt32,
    NumberClass::Int32,
    NumberClass::UInt64,
    NumberClass::Int64,
    NumberClass::Decimal128,
    NumberClass::Decimal256,
    NumberClass::Float32,
    NumberClass::Float64,
];

impl NumberClass {
    pub fn get_repr_type(&self) -> DataType {
        match self {
            NumberClass::UInt8 => DataType::Number(NumberDataType::UInt8),
            NumberClass::UInt16 => DataType::Number(NumberDataType::UInt16),
            NumberClass::UInt32 => DataType::Number(NumberDataType::UInt32),
            NumberClass::UInt64 => DataType::Number(NumberDataType::UInt64),
            NumberClass::Int8 => DataType::Number(NumberDataType::Int8),
            NumberClass::Int16 => DataType::Number(NumberDataType::Int16),
            NumberClass::Int32 => DataType::Number(NumberDataType::Int32),
            NumberClass::Int64 => DataType::Number(NumberDataType::Int64),
            NumberClass::Decimal128 => {
                DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                    precision: 38,
                    scale: 0,
                }))
            }
            NumberClass::Decimal256 => {
                DataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
                    precision: 38,
                    scale: 0,
                }))
            }
            NumberClass::Float32 => DataType::Number(NumberDataType::Float32),
            NumberClass::Float64 => DataType::Number(NumberDataType::Float64),
        }
    }

    pub fn get_number_type(&self) -> Option<NumberDataType> {
        match self.get_repr_type() {
            DataType::Number(n) => Some(n),
            _ => None,
        }
    }
}
