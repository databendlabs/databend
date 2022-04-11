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
        factory.register_typed("ascii", AsciiFunction::desc());
        factory.register_typed("bit_length", BitLengthFunction::desc());
        factory.register_typed("octet_length", OctetLengthFunction::desc());
        factory.register_typed("char_length", CharLengthFunction::desc());
        factory.register_typed("character_length", CharLengthFunction::desc());
        factory.register_typed("ord", OrdFunction::desc());
        factory.register_typed("length", LengthFunction::desc());
        factory.register_typed("regexp_instr", RegexpInStrFunction::desc());
        factory.register_typed("regexp_like", RegexpLikeFunction::desc());
        factory.register_typed("regexp_substr", RegexpSubStrFunction::desc());
        factory.register_typed("bin", BinFunction::desc());
        factory.register_typed("oct", OctFunction::desc());
        factory.register_typed("hex", HexFunction::desc());
        factory.register_typed("unhex", UnhexFunction::desc());
        factory.register_typed("repeat", RepeatFunction::desc());
        factory.register_typed("substring", SubstringFunction::desc());
        factory.register_typed("mid", SubstringFunction::desc());
        factory.register_typed("substr", SubstringFunction::desc());
        factory.register_typed("substring_index", SubstringIndexFunction::desc());
        factory.register_typed("left", LeftFunction::desc());
        factory.register_typed("right", RightFunction::desc());
        factory.register_typed("concat_ws", ConcatWsFunction::desc());
        factory.register_typed("elt", EltFunction::desc());
        factory.register_typed("space", SpaceFunction::desc());
        factory.register_typed("lpad", LeftPadFunction::desc());
        factory.register_typed("rpad", RightPadFunction::desc());
        factory.register_typed("export_set", ExportSetFunction::desc());
        factory.register_typed("find_in_set", FindInSetFunction::desc());
        factory.register_typed("format", FormatFunction::desc());
        factory.register_typed("char", CharFunction::desc());
        factory.register_typed("insert", InsertFunction::desc());
        factory.register_typed("field", FieldFunction::desc());
        factory.register_typed("concat", ConcatFunction::desc());
        factory.register_typed("replace", ReplaceFunction::desc());
        factory.register_typed("strcmp", StrcmpFunction::desc());
        factory.register_typed("locate", LocateFunction::desc());
        factory.register_typed("position", PositionFunction::desc());
        factory.register_typed("instr", InstrFunction::desc());
    }
}
