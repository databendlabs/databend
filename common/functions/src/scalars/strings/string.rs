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
use crate::scalars::Base64DecodeFunction;
use crate::scalars::Base64EncodeFunction;
use crate::scalars::BitLengthFunction;
use crate::scalars::ConcatFunction;
use crate::scalars::FieldFunction;
use crate::scalars::HexFunction;
use crate::scalars::InsertFunction;
use crate::scalars::InstrFunction;
use crate::scalars::LTrimFunction;
use crate::scalars::LocateFunction;
use crate::scalars::OctFunction;
use crate::scalars::OctetLengthFunction;
use crate::scalars::PositionFunction;
use crate::scalars::QuoteFunction;
use crate::scalars::RTrimFunction;
use crate::scalars::RepeatFunction;
use crate::scalars::ReplaceFunction;
use crate::scalars::StrcmpFunction;
use crate::scalars::SubstringFunction;
use crate::scalars::TrimFunction;
use crate::scalars::UnhexFunction;

#[derive(Clone)]
pub struct StringFunction;

impl StringFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register("mid", SubstringFunction::desc());
        factory.register("substr", SubstringFunction::desc());
        factory.register("substring", SubstringFunction::desc());
        factory.register("substring", SubstringFunction::desc());
        factory.register("oct", OctFunction::desc());
        factory.register("repeat", RepeatFunction::desc());
        factory.register("ltrim", LTrimFunction::desc());
        factory.register("rtrim", RTrimFunction::desc());
        factory.register("trim", TrimFunction::desc());
        factory.register("hex", HexFunction::desc());
        factory.register("unhex", UnhexFunction::desc());
        factory.register("quote", QuoteFunction::desc());
        factory.register("ascii", AsciiFunction::desc());
        factory.register("to_base64", Base64EncodeFunction::desc());
        factory.register("from_base64", Base64DecodeFunction::desc());
        factory.register("locate", LocateFunction::desc());
        factory.register("position", PositionFunction::desc());
        factory.register("instr", InstrFunction::desc());
        factory.register("insert", InsertFunction::desc());
        factory.register("field", FieldFunction::desc());
        factory.register("octet_length", OctetLengthFunction::desc());
        factory.register("concat", ConcatFunction::desc());
        factory.register("bit_length", BitLengthFunction::desc());
        factory.register("replace", ReplaceFunction::desc());
        factory.register("strcmp", StrcmpFunction::desc());
    }
}
