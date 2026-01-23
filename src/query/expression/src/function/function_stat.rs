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

use databend_common_base::runtime::metrics::Histogram;
use databend_common_statistics::Datum;
use databend_common_statistics::F64;
pub use databend_common_statistics::Ndv;

use crate::FunctionContext;
use crate::Scalar;
use crate::types::number::NumberScalar;

#[derive(Clone, Copy)]
pub enum DeriveStat {
    Nullary(fn(cardinality: f64, ctx: &FunctionContext) -> Result<ReturnStat, String>),
    Unary(fn(&StatUnaryArg, ctx: &FunctionContext) -> Result<ReturnStat, String>),
    Binary(fn(&StatBinaryArg, ctx: &FunctionContext) -> Result<ReturnStat, String>),
}

pub struct MinMax<'a> {
    pub min: &'a Datum,
    pub max: &'a Datum,
}

#[derive(Clone, Copy, Default)]
pub struct StatUnaryArg;

impl StatUnaryArg {
    pub const fn new() -> Self {
        Self
    }

    pub fn cardinality(&self) -> f64 {
        todo!("StatUnaryArg::cardinality placeholder")
    }

    pub fn ndv_1_arg(&self) -> Option<Ndv> {
        todo!("StatUnaryArg::ndv_1_arg placeholder")
    }

    pub fn null_count_1_arg(&self) -> Option<u64> {
        todo!("StatUnaryArg::null_count_1_arg placeholder")
    }

    pub fn min_max_1_arg(&self) -> Option<MinMax<'_>> {
        todo!("StatUnaryArg::min_max_1_arg placeholder")
    }

    pub fn histogram_1_arg(&self) -> Option<&Histogram> {
        todo!("StatUnaryArg::histogram_1_arg placeholder")
    }
}

#[derive(Clone, Copy, Default)]
pub struct StatBinaryArg;

impl StatBinaryArg {
    pub const fn new() -> Self {
        Self
    }

    pub fn cardinality(&self) -> f64 {
        todo!("StatBinaryArg::cardinality placeholder")
    }

    pub fn ndv_2_arg(&self) -> (Option<Ndv>, Option<Ndv>) {
        todo!("StatBinaryArg::ndv_2_arg placeholder")
    }

    pub fn null_count_2_arg(&self) -> (Option<u64>, Option<u64>) {
        todo!("StatBinaryArg::null_count_2_arg placeholder")
    }

    pub fn min_max_2_arg(&self) -> (Option<MinMax<'_>>, Option<MinMax<'_>>) {
        todo!("StatBinaryArg::min_max_2_arg placeholder")
    }

    pub fn histogram_2_arg(&self) -> (Option<&Histogram>, Option<&Histogram>) {
        todo!("StatBinaryArg::histogram_2_arg placeholder")
    }
}

#[derive(Default)]
pub struct ReturnStat {
    pub ndv: Option<Ndv>,
    pub null_count: Option<u64>,
    pub min_max: Option<(Datum, Datum)>, // min,max
    pub histogram: Option<Histogram>,
}

impl Scalar {
    pub fn to_datum(self) -> Option<Datum> {
        match self {
            Scalar::Boolean(v) => Some(Datum::Bool(v)),
            Scalar::Number(NumberScalar::Int8(v)) => Some(Datum::Int(v as i64)),
            Scalar::Number(NumberScalar::Int16(v)) => Some(Datum::Int(v as i64)),
            Scalar::Number(NumberScalar::Int32(v)) | Scalar::Date(v) => Some(Datum::Int(v as i64)),
            Scalar::Number(NumberScalar::Int64(v)) | Scalar::Timestamp(v) => Some(Datum::Int(v)),
            Scalar::Number(NumberScalar::UInt8(v)) => Some(Datum::UInt(v as u64)),
            Scalar::Number(NumberScalar::UInt16(v)) => Some(Datum::UInt(v as u64)),
            Scalar::Number(NumberScalar::UInt32(v)) => Some(Datum::UInt(v as u64)),
            Scalar::Number(NumberScalar::UInt64(v)) => Some(Datum::UInt(v)),
            Scalar::Number(NumberScalar::Float32(v)) => {
                Some(Datum::Float(F64::from(f32::from(v) as f64)))
            }
            Scalar::Decimal(v) => Some(Datum::Float(F64::from(v.to_float64()))),
            Scalar::Number(NumberScalar::Float64(v)) => Some(Datum::Float(v)),
            Scalar::Binary(v) => Some(Datum::Bytes(v)),
            Scalar::String(v) => Some(Datum::Bytes(v.as_bytes().to_vec())),
            _ => None,
        }
    }
}
