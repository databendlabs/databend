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

// Template Patterns for Numeric Formatting

use databend_common_exception::Result;
use enumflags2::bitflags;
use enumflags2::make_bitflags;
use enumflags2::BitFlags;

// https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/formatting.c

#[derive(Clone, Copy)]
enum NumPoz {
    NumComma,
    NumDec,
    Num0,
    Num9,
    NumB,
    NumC,
    NumD,
    NumE,
    NumFM,
    NumG,
    NumL,
    NumMI,
    NumPL,
    NumPR,
    NumRN,
    NumSG,
    NumSP,
    NumS,
    NumTH,
    NumV,
    Numb,
    Numc,
    Numd,
    Nume,
    Numfm,
    Numg,
    Numl,
    Nummi,
    Numpl,
    Numpr,
    Numrn,
    Numsg,
    Numsp,
    Nums,
    Numth,
    Numv,
}

// ----------
// Flags for NUMBER version
// ----------

#[bitflags]
#[repr(u16)]
#[derive(Clone, Copy, Debug)]
enum NumFlag {
    Decimal,
    LDecimal,
    Zero,
    Blank,
    FillMode,
    LSign,
    Bracket,
    Minus,
    Plus,
    Roman,
    Multi,
    PlusPost,
    MinusPost,
    EEEE,
}

enum NumLSign {
    Pre,
    Post,
}

#[derive(Clone)]
struct KeyWord {
    name: &'static str,
    id: NumPoz,
    // is_digit: bool,
    // date_mode: FromCharDateMode,
}

impl KeyWord {
    const fn new(name: &'static str, id: NumPoz) -> KeyWord {
        KeyWord { name, id }
    }
}

const NUM_KEYWORDS: [KeyWord; 36] = [
    KeyWord::new(",", NumPoz::NumComma),
    KeyWord::new(".", NumPoz::NumDec),
    KeyWord::new("0", NumPoz::Num0),
    KeyWord::new("9", NumPoz::Num9),
    KeyWord::new("B", NumPoz::NumB),
    KeyWord::new("C", NumPoz::NumC),
    KeyWord::new("D", NumPoz::NumD),
    KeyWord::new("EEEE", NumPoz::NumE),
    KeyWord::new("FM", NumPoz::NumFM),
    KeyWord::new("G", NumPoz::NumG),
    KeyWord::new("L", NumPoz::NumL),
    KeyWord::new("MI", NumPoz::NumMI),
    KeyWord::new("PL", NumPoz::NumPL),
    KeyWord::new("PR", NumPoz::NumPR),
    KeyWord::new("RN", NumPoz::NumRN),
    KeyWord::new("SG", NumPoz::NumSG),
    KeyWord::new("SP", NumPoz::NumSP),
    KeyWord::new("S", NumPoz::NumS),
    KeyWord::new("TH", NumPoz::NumTH),
    KeyWord::new("V", NumPoz::NumV),
    KeyWord::new("b", NumPoz::NumB),
    KeyWord::new("c", NumPoz::Numc),
    KeyWord::new("d", NumPoz::NumD),
    KeyWord::new("eeee", NumPoz::NumE),
    KeyWord::new("fm", NumPoz::NumFM),
    KeyWord::new("g", NumPoz::NumG),
    KeyWord::new("l", NumPoz::NumL),
    KeyWord::new("mi", NumPoz::NumMI),
    KeyWord::new("pl", NumPoz::NumPL),
    KeyWord::new("pr", NumPoz::NumPR),
    KeyWord::new("rn", NumPoz::Numrn),
    KeyWord::new("sg", NumPoz::NumSG),
    KeyWord::new("sp", NumPoz::NumSP),
    KeyWord::new("s", NumPoz::NumS),
    KeyWord::new("th", NumPoz::Numth),
    KeyWord::new("v", NumPoz::NumV),
];

struct FormatNode {
    typ: NodeType,
    character: Vec<char>, // if type is CHAR
    suffix: u8,           // keyword prefix/suffix code, if any
    key: KeyWord,         // if type is ACTION
}

enum NodeType {
    End,
    Action,
    Char,
    Separator,
    Space,
}

// ----------
// Number description struct
// ----------
#[derive(Default, Debug)]
struct NumDesc {
    pre: usize,              // (count) numbers before decimal
    post: usize,             // (count) numbers after decimal
    lsign: Option<NumLSign>, // want locales sign
    flag: BitFlags<NumFlag>, // number parameters
    pre_lsign_num: usize,    // tmp value for lsign
    multi: usize,            // multiplier for 'V'
    zero_start: usize,       // position of first zero
    zero_end: usize,         // position of last zero
    need_locale: bool,       // needs it locale
}

impl NumDesc {
    pub fn try_new(mut str: &str) -> Result<NumDesc> {
        let mut num = NumDesc::default();

        while !str.is_empty() {
            match NUM_KEYWORDS.iter().find(|k| str.starts_with(k.name)) {
                Some(k) => {
                    let n = FormatNode {
                        typ: NodeType::Action,
                        character: Vec::new(),
                        suffix: 0,
                        key: k.clone(),
                    };

                    num.prepare(&n)?;

                    str = &str[k.name.len()..]
                }
                None => todo!(),
            }
        }
        Ok(num)
    }

    fn prepare(&mut self, n: &FormatNode) -> std::result::Result<(), &'static str> {
        if !matches!(n.typ, NodeType::Action) {
            return Ok(());
        }

        if self.flag.contains(NumFlag::EEEE) && !matches!(n.key.id, NumPoz::NumE) {
            return Err("\"EEEE\" must be the last pattern used");
        }

        match n.key.id {
            NumPoz::Num9 => {
                if self.flag.contains(NumFlag::Bracket) {
                    return Err("\"9\" must be ahead of \"PR\"");
                }

                if self.flag.contains(NumFlag::Multi) {
                    self.multi += 1;
                    return Ok(());
                }

                if self.flag.contains(NumFlag::Decimal) {
                    self.post += 1;
                } else {
                    self.pre += 1;
                }
                return Ok(());
            }

            NumPoz::Num0 => {
                if self.flag.contains(NumFlag::Bracket) {
                    return Err("\"0\" must be ahead of \"PR\"");
                }

                if !self.flag.intersects(NumFlag::Zero | NumFlag::Decimal) {
                    self.flag.insert(NumFlag::Zero);
                    self.zero_start = self.pre + 1;
                }

                if !self.flag.contains(NumFlag::Decimal) {
                    self.pre += 1;
                } else {
                    self.post += 1;
                }

                self.zero_end = self.pre + self.post;
                Ok(())
            }

            NumPoz::NumB => {
                if self.pre == 0 && self.post == 0 && !self.flag.contains(NumFlag::Zero) {
                    self.flag.insert(NumFlag::Blank)
                }
                Ok(())
            }

            NumPoz::NumD => {
                self.flag.insert(NumFlag::LDecimal);
                self.need_locale = true;

                if self.flag.contains(NumFlag::Decimal) {
                    return Err("multiple decimal points");
                }
                if self.flag.contains(NumFlag::Multi) {
                    return Err("cannot use \"V\" and decimal point together");
                }

                self.flag.insert(NumFlag::Decimal);
                Ok(())
            }

            NumPoz::NumDec => {
                if self.flag.contains(NumFlag::Decimal) {
                    return Err("multiple decimal points");
                }
                if self.flag.contains(NumFlag::Multi) {
                    return Err("cannot use \"V\" and decimal point together");
                }

                self.flag.insert(NumFlag::Decimal);
                Ok(())
            }

            NumPoz::NumFM => {
                self.flag.insert(NumFlag::FillMode);
                Ok(())
            }

            NumPoz::NumS => {
                if self.flag.contains(NumFlag::LSign) {
                    return Err("cannot use \"S\" twice");
                }
                if self
                    .flag
                    .intersects(NumFlag::Plus | NumFlag::Minus | NumFlag::Bracket)
                {
                    return Err("cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together");
                }

                if self.flag.contains(NumFlag::Decimal) {
                    self.lsign = Some(NumLSign::Pre);
                    self.pre_lsign_num = self.pre;
                    self.need_locale = true;
                    self.flag.insert(NumFlag::LSign);
                    return Ok(());
                }

                if self.lsign.is_none() {
                    self.lsign = Some(NumLSign::Post);
                    self.need_locale = true;
                    self.flag.insert(NumFlag::LSign);
                }
                Ok(())
            }

            NumPoz::NumMI => {
                if self.flag.contains(NumFlag::LSign) {
                    return Err("cannot use \"S\" and \"MI\" together");
                }

                self.flag.insert(NumFlag::Minus);
                if self.flag.contains(NumFlag::Decimal) {
                    self.flag.insert(NumFlag::MinusPost)
                }
                Ok(())
            }
            NumPoz::NumPL => {
                if self.flag.contains(NumFlag::LSign) {
                    return Err("cannot use \"S\" and \"PL\" together");
                }

                self.flag.insert(NumFlag::Plus);
                if self.flag.contains(NumFlag::Decimal) {
                    self.flag.insert(NumFlag::PlusPost)
                }
                Ok(())
            }
            NumPoz::NumSG => {
                if self.flag.contains(NumFlag::LSign) {
                    return Err("cannot use \"S\" and \"SG\" together");
                }
                self.flag.insert(NumFlag::Plus | NumFlag::Minus);
                Ok(())
            }
            NumPoz::NumPR => {
                if self
                    .flag
                    .intersects(NumFlag::LSign | NumFlag::Plus | NumFlag::Minus)
                {
                    return Err("cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together");
                }

                self.flag.insert(NumFlag::Bracket);
                Ok(())
            }
            NumPoz::Numrn | NumPoz::NumRN => {
                self.flag.insert(NumFlag::Roman);
                Ok(())
            }

            NumPoz::NumL | NumPoz::NumG => {
                self.need_locale = true;
                Ok(())
            }

            NumPoz::NumV => {
                if self.flag.contains(NumFlag::Decimal) {
                    return Err("cannot use \"V\" and decimal point together");
                }
                self.flag.insert(NumFlag::Multi);
                Ok(())
            }

            NumPoz::NumE => {
                if self.flag.contains(NumFlag::EEEE) {
                    return Err("cannot use \"EEEE\" twice");
                }

                if self.flag.intersects(
                    NumFlag::Blank
                        | NumFlag::FillMode
                        | NumFlag::LSign
                        | NumFlag::Bracket
                        | NumFlag::Minus
                        | NumFlag::Plus
                        | NumFlag::Roman
                        | NumFlag::Multi,
                ) {
                    return Err(
                        "\"EEEE\" may only be used together with digit and decimal point patterns.",
                    );
                }

                self.flag.insert(NumFlag::EEEE);
                Ok(())
            }

            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::number::NumDesc;

    #[test]
    fn test_aa() {
        let fmt = "9990999.9";

        let desc = NumDesc::try_new(fmt).unwrap();

        println!("{:?}", desc);
    }
}
