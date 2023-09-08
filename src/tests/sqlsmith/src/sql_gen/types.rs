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

use common_expression::types::decimal::DecimalSize;
use common_expression::types::DataType;
use common_expression::types::DecimalDataType;
use common_expression::types::NumberDataType;
use rand::Rng;

use crate::sql_gen::SqlGenerator;

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_data_type(&mut self) -> DataType {
        if self.rng.gen_bool(0.8) {
            self.gen_simple_data_type()
        } else {
            self.gen_nested_data_type()
        }
    }

    pub(crate) fn gen_simple_data_type(&mut self) -> DataType {
        match self.rng.gen_range(0..=9) {
            0 => DataType::Null,
            1 => DataType::Boolean,
            2 => DataType::String,
            3 => match self.rng.gen_range(0..=9) {
                0 => DataType::Number(NumberDataType::UInt8),
                1 => DataType::Number(NumberDataType::UInt16),
                2 => DataType::Number(NumberDataType::UInt32),
                3 => DataType::Number(NumberDataType::UInt64),
                4 => DataType::Number(NumberDataType::Int8),
                5 => DataType::Number(NumberDataType::Int16),
                6 => DataType::Number(NumberDataType::Int32),
                7 => DataType::Number(NumberDataType::Int64),
                8 => DataType::Number(NumberDataType::Float32),
                9 => DataType::Number(NumberDataType::Float64),
                _ => unreachable!(),
            },
            4 => {
                let precision = self.rng.gen_range(1..=76);
                let scale = self.rng.gen_range(0..=precision);
                let size = DecimalSize { precision, scale };
                if precision <= 38 {
                    DataType::Decimal(DecimalDataType::Decimal128(size))
                } else {
                    DataType::Decimal(DecimalDataType::Decimal256(size))
                }
            }
            5 => DataType::Timestamp,
            6 => DataType::Date,
            7 => DataType::Bitmap,
            8 => DataType::Variant,
            9 => {
                let inner_ty = self.gen_simple_data_type();
                if !inner_ty.is_nullable_or_null() {
                    DataType::Nullable(Box::new(inner_ty))
                } else {
                    inner_ty
                }
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn gen_nested_data_type(&mut self) -> DataType {
        match self.rng.gen_range(0..=8) {
            0 => DataType::EmptyArray,
            1 => DataType::EmptyMap,
            2..=3 => DataType::Array(Box::new(self.gen_data_type())),
            4..=5 => {
                let key_ty = match self.rng.gen_range(0..=6) {
                    0 => DataType::Boolean,
                    1 => DataType::String,
                    2 => DataType::Number(NumberDataType::UInt64),
                    3 => DataType::Number(NumberDataType::Int64),
                    4 => DataType::Number(NumberDataType::Float64),
                    5 => DataType::Timestamp,
                    6 => DataType::Date,
                    _ => unreachable!(),
                };
                let val_ty = self.gen_data_type();
                let inner_ty = DataType::Tuple(vec![key_ty, val_ty]);
                DataType::Map(Box::new(inner_ty))
            }
            6..=7 => {
                let len = self.rng.gen_range(1..=5);
                let mut inner_tys = Vec::with_capacity(len);
                for _ in 0..len {
                    inner_tys.push(self.gen_data_type());
                }
                DataType::Tuple(inner_tys)
            }
            8 => {
                let inner_ty = self.gen_nested_data_type();
                if !inner_ty.is_nullable_or_null() {
                    DataType::Nullable(Box::new(inner_ty))
                } else {
                    inner_ty
                }
            }
            _ => unreachable!(),
        }
    }
}
