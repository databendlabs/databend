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

use crate::scalars::AsciiFunction;
use crate::scalars::Base64DecodeFunction;
use crate::scalars::Base64EncodeFunction;
use crate::scalars::BinFunction;
use crate::scalars::BitLengthFunction;
use crate::scalars::CharFunction;
use crate::scalars::CharLengthFunction;
use crate::scalars::ConcatFunction;
use crate::scalars::ConcatWsFunction;
use crate::scalars::EltFunction;
use crate::scalars::ExportSetFunction;
use crate::scalars::FieldFunction;
use crate::scalars::FindInSetFunction;
use crate::scalars::FormatFunction;
use crate::scalars::FunctionFactory;
use crate::scalars::HexFunction;
use crate::scalars::InsertFunction;
use crate::scalars::InstrFunction;
use crate::scalars::LTrimFunction;
use crate::scalars::LeftFunction;
use crate::scalars::LeftPadFunction;
use crate::scalars::LengthFunction;
use crate::scalars::LocateFunction;
use crate::scalars::LowerFunction;
use crate::scalars::OctFunction;
use crate::scalars::OctetLengthFunction;
use crate::scalars::OrdFunction;
use crate::scalars::PositionFunction;
use crate::scalars::QuoteFunction;
use crate::scalars::RTrimFunction;
use crate::scalars::RegexpInStrFunction;
use crate::scalars::RegexpLikeFunction;
use crate::scalars::RegexpSubStrFunction;
use crate::scalars::RepeatFunction;
use crate::scalars::ReplaceFunction;
use crate::scalars::ReverseFunction;
use crate::scalars::RightFunction;
use crate::scalars::RightPadFunction;
use crate::scalars::SoundexFunction;
use crate::scalars::SpaceFunction;
use crate::scalars::StrcmpFunction;
use crate::scalars::SubstringFunction;
use crate::scalars::SubstringIndexFunction;
use crate::scalars::TrimFunction;
use crate::scalars::UnhexFunction;
use crate::scalars::UpperFunction;

#[derive(Clone)]
pub struct StringFunction;

impl StringFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register_typed("to_base64", Base64EncodeFunction::desc());
        factory.register_typed("from_base64", Base64DecodeFunction::desc());
        factory.register_typed("rtrim", RTrimFunction::desc());
        factory.register_typed("trim", TrimFunction::desc());
        factory.register_typed("ltrim", LTrimFunction::desc());
        factory.register_typed("quote", QuoteFunction::desc());
        factory.register_typed("lower", LowerFunction::desc());
        factory.register_typed("lcase", LowerFunction::desc());
        factory.register_typed("upper", UpperFunction::desc());
        factory.register_typed("ucase", UpperFunction::desc());
        factory.register_typed("reverse", ReverseFunction::desc());
        factory.register_typed("soundex", SoundexFunction::desc());
        factory.register("ascii", AsciiFunction::desc());
        factory.register("bit_length", BitLengthFunction::desc());
        factory.register("octet_length", OctetLengthFunction::desc());
        factory.register("char_length", CharLengthFunction::desc());
        factory.register("character_length", CharLengthFunction::desc());
        factory.register("ord", OrdFunction::desc());
        factory.register("length", LengthFunction::desc());
        factory.register("regexp_instr", RegexpInStrFunction::desc());
        factory.register("regexp_like", RegexpLikeFunction::desc());
        factory.register("regexp_substr", RegexpSubStrFunction::desc());
        factory.register("bin", BinFunction::desc());
        factory.register("oct", OctFunction::desc());
        factory.register("hex", HexFunction::desc());
        factory.register("unhex", UnhexFunction::desc());
        factory.register("repeat", RepeatFunction::desc());
        factory.register("substring", SubstringFunction::desc());
        factory.register("mid", SubstringFunction::desc());
        factory.register("substr", SubstringFunction::desc());
        factory.register("substring_index", SubstringIndexFunction::desc());
        factory.register("left", LeftFunction::desc());
        factory.register("right", RightFunction::desc());
        factory.register("concat_ws", ConcatWsFunction::desc());
        factory.register("elt", EltFunction::desc());
        factory.register("space", SpaceFunction::desc());
        factory.register("lpad", LeftPadFunction::desc());
        factory.register("rpad", RightPadFunction::desc());
        factory.register("export_set", ExportSetFunction::desc());
        factory.register("find_in_set", FindInSetFunction::desc());
        factory.register("format", FormatFunction::desc());
        factory.register("char", CharFunction::desc());
        factory.register("insert", InsertFunction::desc());
        factory.register("field", FieldFunction::desc());
        factory.register("concat", ConcatFunction::desc());
        factory.register("replace", ReplaceFunction::desc());
        factory.register("strcmp", StrcmpFunction::desc());
        factory.register("locate", LocateFunction::desc());
        factory.register("position", PositionFunction::desc());
        factory.register("instr", InstrFunction::desc());
    }
}
