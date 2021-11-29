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

use crate::scalars::function_factory::FunctionFactory;
use crate::scalars::AsciiFunction;
use crate::scalars::HexFunction;
use crate::scalars::LTrimFunction;
use crate::scalars::OctFunction;
use crate::scalars::QuoteFunction;
use crate::scalars::RTrimFunction;
use crate::scalars::RepeatFunction;
use crate::scalars::SubstringFunction;
use crate::scalars::UnhexFunction;

#[derive(Clone)]
pub struct StringFunction;

impl StringFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register("substring", SubstringFunction::desc());
        factory.register("oct", OctFunction::desc());
        factory.register("repeat", RepeatFunction::desc());
        factory.register("ltrim", LTrimFunction::desc());
        factory.register("rtrim", RTrimFunction::desc());
        factory.register("hex", HexFunction::desc());
        factory.register("unhex", UnhexFunction::desc());
        factory.register("quote", QuoteFunction::desc());
        factory.register("ascii", AsciiFunction::desc());
    }
}
