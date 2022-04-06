// Copyright 2021 Datafuse Labs.
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

use crc32fast::Hasher as CRC32;

use super::round::RoundNumberFunction;
use super::round::TruncNumberFunction;
use crate::scalars::AbsFunction;
use crate::scalars::BaseHashFunction;
use crate::scalars::CeilFunction;
use crate::scalars::DegressFunction;
use crate::scalars::ExpFunction;
use crate::scalars::FloorFunction;
use crate::scalars::FunctionFactory;
use crate::scalars::LnFunction;
use crate::scalars::Log10Function;
use crate::scalars::Log2Function;
use crate::scalars::LogFunction;
use crate::scalars::PiFunction;
use crate::scalars::PowFunction;
use crate::scalars::RadiansFunction;
use crate::scalars::RandomFunction;
use crate::scalars::SignFunction;
use crate::scalars::SqrtFunction;
use crate::scalars::TrigonometricAcosFunction;
use crate::scalars::TrigonometricAsinFunction;
use crate::scalars::TrigonometricAtan2Function;
use crate::scalars::TrigonometricAtanFunction;
use crate::scalars::TrigonometricCosFunction;
use crate::scalars::TrigonometricCotFunction;
use crate::scalars::TrigonometricSinFunction;
use crate::scalars::TrigonometricTanFunction;

pub type CRC32Function = BaseHashFunction<CRC32, u32>;

pub struct MathsFunction;

impl MathsFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register_typed("abs", AbsFunction::desc());
        factory.register_typed("sign", SignFunction::desc());
        factory.register_typed("pi", PiFunction::desc());
        factory.register_typed("crc32", CRC32Function::desc());
        factory.register_typed("exp", ExpFunction::desc());
        factory.register_typed("sqrt", SqrtFunction::desc());
        factory.register_typed("ceil", CeilFunction::desc());
        factory.register_typed("ceiling", CeilFunction::desc());
        factory.register_typed("floor", FloorFunction::desc());

        factory.register_typed("log", LogFunction::desc());
        factory.register_typed("log10", Log10Function::desc());
        factory.register_typed("log2", Log2Function::desc());
        factory.register_typed("ln", LnFunction::desc());
        factory.register_typed("pow", PowFunction::desc());
        factory.register_typed("power", PowFunction::desc());
        factory.register_typed("rand", RandomFunction::desc());
        factory.register_typed("round", RoundNumberFunction::desc());
        factory.register_typed("truncate", TruncNumberFunction::desc());

        factory.register_typed("sin", TrigonometricSinFunction::desc());
        factory.register_typed("cos", TrigonometricCosFunction::desc());
        factory.register_typed("tan", TrigonometricTanFunction::desc());
        factory.register_typed("cot", TrigonometricCotFunction::desc());
        factory.register_typed("asin", TrigonometricAsinFunction::desc());
        factory.register_typed("acos", TrigonometricAcosFunction::desc());
        factory.register_typed("atan", TrigonometricAtanFunction::desc());
        factory.register_typed("atan2", TrigonometricAtan2Function::desc());

        factory.register_typed("degrees", DegressFunction::desc());
        factory.register_typed("radians", RadiansFunction::desc());
    }
}
