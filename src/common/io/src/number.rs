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

use std::str::FromStr;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use enumflags2::BitFlags;
use enumflags2::bitflags;

// Template Patterns for Numeric Formatting
// https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/formatting.c

#[derive(Debug, Clone)]
struct KeyWord {
    name: &'static str,
    id: NumPoz,
    // is_digit: bool,
    // date_mode: FromCharDateMode,
}

#[derive(Debug, Clone, Copy)]
#[expect(dead_code)]
enum NumPoz {
    TkComma,
    TkDec,
    Tk0,
    Tk9,
    TkB,
    TkC,
    TkD,
    TkE,
    TkFM,
    TkG,
    TkL,
    TkMI,
    TkPL,
    TkPR,
    TkRN,
    TkSG,
    TkSP,
    TkS,
    TkTH,
    TkV,
    Tkb,
    Tkc,
    Tkd,
    Tke,
    Tkfm,
    Tkg,
    Tkl,
    Tkmi,
    Tkpl,
    Tkpr,
    Tkrn,
    Tksg,
    Tksp,
    Tks,
    Tkth,
    Tkv,
}

const NUM_KEYWORDS: [KeyWord; 36] = [
    KeyWord::new(",", NumPoz::TkComma),
    KeyWord::new(".", NumPoz::TkDec),
    KeyWord::new("0", NumPoz::Tk0),
    KeyWord::new("9", NumPoz::Tk9),
    KeyWord::new("B", NumPoz::TkB),
    KeyWord::new("C", NumPoz::TkC),
    KeyWord::new("D", NumPoz::TkD),
    KeyWord::new("EEEE", NumPoz::TkE),
    KeyWord::new("FM", NumPoz::TkFM),
    KeyWord::new("G", NumPoz::TkG),
    KeyWord::new("L", NumPoz::TkL),
    KeyWord::new("MI", NumPoz::TkMI),
    KeyWord::new("PL", NumPoz::TkPL),
    KeyWord::new("PR", NumPoz::TkPR),
    KeyWord::new("RN", NumPoz::TkRN),
    KeyWord::new("SG", NumPoz::TkSG),
    KeyWord::new("SP", NumPoz::TkSP),
    KeyWord::new("S", NumPoz::TkS),
    KeyWord::new("TH", NumPoz::TkTH),
    KeyWord::new("V", NumPoz::TkV),
    KeyWord::new("b", NumPoz::Tkb),
    KeyWord::new("c", NumPoz::Tkc),
    KeyWord::new("d", NumPoz::TkD),
    KeyWord::new("eeee", NumPoz::TkE),
    KeyWord::new("fm", NumPoz::TkFM),
    KeyWord::new("g", NumPoz::TkG),
    KeyWord::new("l", NumPoz::TkL),
    KeyWord::new("mi", NumPoz::TkMI),
    KeyWord::new("pl", NumPoz::TkPL),
    KeyWord::new("pr", NumPoz::TkPR),
    KeyWord::new("rn", NumPoz::Tkrn),
    KeyWord::new("sg", NumPoz::TkSG),
    KeyWord::new("sp", NumPoz::TkSP),
    KeyWord::new("s", NumPoz::TkS),
    KeyWord::new("th", NumPoz::Tkth),
    KeyWord::new("v", NumPoz::TkV),
];

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
    Eeee,
}

#[derive(Debug, Clone, Copy)]
enum NumLSign {
    Pre,
    Post,
}

impl KeyWord {
    const fn new(name: &'static str, id: NumPoz) -> KeyWord {
        KeyWord { name, id }
    }
}

#[derive(Debug)]
#[expect(dead_code)]
enum FormatNode {
    End,
    Action(KeyWord),
    Char(String),
    Separator,
    Space,
}

// ----------
// Number description struct
// ----------
#[derive(Default, Debug, Clone)]
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
    fn prepare(&mut self, n: &FormatNode) -> std::result::Result<(), &'static str> {
        if let FormatNode::Action(key) = n {
            if self.flag.contains(NumFlag::Eeee) && !matches!(key.id, NumPoz::TkE) {
                return Err("\"EEEE\" must be the last pattern used");
            }

            match key.id {
                NumPoz::Tk9 => {
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
                    Ok(())
                }

                NumPoz::Tk0 => {
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

                NumPoz::TkB => {
                    if self.pre == 0 && self.post == 0 && !self.flag.contains(NumFlag::Zero) {
                        self.flag.insert(NumFlag::Blank)
                    }
                    Ok(())
                }

                NumPoz::TkD => {
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

                NumPoz::TkDec => {
                    if self.flag.contains(NumFlag::Decimal) {
                        return Err("multiple decimal points");
                    }
                    if self.flag.contains(NumFlag::Multi) {
                        return Err("cannot use \"V\" and decimal point together");
                    }

                    self.flag.insert(NumFlag::Decimal);
                    Ok(())
                }

                NumPoz::TkFM => {
                    self.flag.insert(NumFlag::FillMode);
                    Ok(())
                }

                NumPoz::TkS => {
                    if self.flag.contains(NumFlag::LSign) {
                        return Err("cannot use \"S\" twice");
                    }
                    self.need_locale = true;
                    self.flag.insert(NumFlag::LSign);

                    if self
                        .flag
                        .intersects(NumFlag::Plus | NumFlag::Minus | NumFlag::Bracket)
                    {
                        return Err("cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together");
                    }

                    if !self.flag.contains(NumFlag::Decimal) {
                        self.lsign = Some(NumLSign::Pre);
                        self.pre_lsign_num = self.pre;
                    } else {
                        self.lsign = Some(NumLSign::Post);
                    }

                    Ok(())
                }

                NumPoz::TkMI => {
                    if self.flag.contains(NumFlag::LSign) {
                        return Err("cannot use \"S\" and \"MI\" together");
                    }

                    self.flag.insert(NumFlag::Minus);
                    if self.flag.contains(NumFlag::Decimal) {
                        self.flag.insert(NumFlag::MinusPost)
                    }
                    Ok(())
                }

                NumPoz::TkPL => {
                    if self.flag.contains(NumFlag::LSign) {
                        return Err("cannot use \"S\" and \"PL\" together");
                    }

                    self.flag.insert(NumFlag::Plus);
                    if self.flag.contains(NumFlag::Decimal) {
                        self.flag.insert(NumFlag::PlusPost)
                    }
                    Ok(())
                }

                NumPoz::TkSG => {
                    if self.flag.contains(NumFlag::LSign) {
                        return Err("cannot use \"S\" and \"SG\" together");
                    }
                    self.flag.insert(NumFlag::Plus | NumFlag::Minus);
                    Ok(())
                }

                NumPoz::TkPR => {
                    if self
                        .flag
                        .intersects(NumFlag::LSign | NumFlag::Plus | NumFlag::Minus)
                    {
                        return Err("cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together");
                    }

                    self.flag.insert(NumFlag::Bracket);
                    Ok(())
                }

                NumPoz::Tkrn | NumPoz::TkRN => {
                    self.flag.insert(NumFlag::Roman);
                    Ok(())
                }

                NumPoz::TkL | NumPoz::TkG => {
                    self.need_locale = true;
                    Ok(())
                }

                NumPoz::TkV => {
                    if self.flag.contains(NumFlag::Decimal) {
                        return Err("cannot use \"V\" and decimal point together");
                    }
                    self.flag.insert(NumFlag::Multi);
                    Ok(())
                }

                NumPoz::TkE => {
                    if self.flag.contains(NumFlag::Eeee) {
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
                        return Err("\"EEEE\" is incompatible with other formats");
                    }

                    self.flag.insert(NumFlag::Eeee);
                    Ok(())
                }

                NumPoz::TkComma => Ok(()),

                _ => unreachable!(),
            }
        } else {
            unreachable!()
        }
    }

    fn i64_to_num_part(&self, value: i64) -> Result<NumPart> {
        if self.flag.contains(NumFlag::Roman) {
            return Err(ErrorCode::Unimplemented("to_char RN (Roman numeral)"));
        }

        if self.flag.contains(NumFlag::Eeee) {
            // we can do it easily because f64 won't lose any precision
            let number = format!("{:+.*e}", self.post, value as f64);

            // Swap a leading positive sign for a space.
            let number = number.replace("+", " ");

            return Ok(NumPart {
                sign: value >= 0,
                number,
                out_pre_spaces: 0,
            });
        }

        if self.flag.contains(NumFlag::Multi) {
            return Err(ErrorCode::Unimplemented("to_char V (multiplies)"));
        }

        let mut orgnum = if value == i64::MIN {
            format!("{}", -(i64::MIN as i128))
        } else {
            format!("{}", value.abs())
        };

        let numstr_pre_len = orgnum.len();

        // post-decimal digits?  Pad out with zeros.
        if self.post > 0 {
            orgnum.push('.');
            orgnum.push_str(&"0".repeat(self.post))
        }

        let (number, out_pre_spaces) = match numstr_pre_len.cmp(&self.pre) {
            // needs padding?
            std::cmp::Ordering::Less => (orgnum, self.pre - numstr_pre_len),
            // overflowed prefix digit format?
            std::cmp::Ordering::Greater => {
                (["#".repeat(self.pre), "#".repeat(self.post)].join("."), 0)
            }
            std::cmp::Ordering::Equal => (orgnum, 0),
        };

        Ok(NumPart {
            sign: value >= 0,
            number,
            out_pre_spaces,
        })
    }

    fn f64_to_num_part(&mut self, value: f64) -> Result<NumPart> {
        self.float_to_num_part(value, 15)
    }

    fn f32_to_num_part(&mut self, value: f32) -> Result<NumPart> {
        self.float_to_num_part(value as f64, 6)
    }

    fn float_to_num_part(&mut self, value: f64, float_digits: usize) -> Result<NumPart> {
        if self.flag.contains(NumFlag::Roman) {
            return Err(ErrorCode::Unimplemented("to_char RN (Roman numeral)"));
        }

        if self.flag.contains(NumFlag::Eeee) {
            let number = if value.is_normal() {
                let orgnum = format!("{:+.*e}", self.post, value);
                // Swap a leading positive sign for a space.
                orgnum.replace("+", " ")
            } else {
                // Allow 6 characters for the leading sign, the decimal point,
                // "e", the exponent's sign and two exponent digits.
                let mut orgnum = String::with_capacity(self.pre + self.post + 6);
                orgnum.push(' ');
                orgnum.push_str(&"#".repeat(self.pre));
                orgnum.push('.');
                orgnum.push_str(&"#".repeat(self.post + 4));
                orgnum
            };
            return Ok(NumPart {
                sign: !value.is_sign_negative(),
                number,
                out_pre_spaces: 0,
            });
        }

        if self.flag.contains(NumFlag::Multi) {
            return Err(ErrorCode::Unimplemented("to_char V (multiplies)"));
        }

        let orgnum = format!("{:.0}", value.abs());
        let numstr_pre_len = orgnum.len();

        // adjust post digits to fit max float digits
        if numstr_pre_len >= float_digits {
            self.post = 0;
        } else if numstr_pre_len + self.post > float_digits {
            self.post = float_digits - numstr_pre_len;
        }
        let orgnum = format!("{:.*}", self.post, value.abs());

        let numstr_pre_len = match orgnum.find('.') {
            Some(p) => p,
            None => orgnum.len(),
        };

        let (number, out_pre_spaces) = match numstr_pre_len.cmp(&self.pre) {
            // needs padding?
            std::cmp::Ordering::Less => (orgnum, self.pre - numstr_pre_len),
            // overflowed prefix digit format?
            std::cmp::Ordering::Greater => {
                (["#".repeat(self.pre), "#".repeat(self.post)].join("."), 0)
            }
            std::cmp::Ordering::Equal => (orgnum, 0),
        };

        Ok(NumPart {
            sign: !value.is_sign_negative(),
            number,
            out_pre_spaces,
        })
    }
}

struct NumPart {
    sign: bool,
    number: String,
    out_pre_spaces: usize,
}

fn parse_format(
    mut str: &str,
    kw: &[KeyWord],
    mut num: Option<&mut NumDesc>,
) -> Result<Vec<FormatNode>> {
    let mut nodes = Vec::new();
    while !str.is_empty() {
        if let Some(remain) = str.strip_prefix(' ') {
            str = remain;
            nodes.push(FormatNode::Space);
            continue;
        }

        if str.starts_with('"') {
            let (offset, literal) =
                parse_literal_string(str).map_err(|e| ErrorCode::SyntaxException(e.to_string()))?;
            nodes.push(FormatNode::Char(literal));
            str = &str[offset..];
            continue;
        }

        if let Some(k) = kw.iter().find(|k| str.starts_with(k.name)) {
            let n = FormatNode::Action(k.clone());

            if let Some(num) = num.as_mut() {
                num.prepare(&n).map_err(ErrorCode::SyntaxException)?;
            }
            str = &str[k.name.len()..];

            nodes.push(n);
            continue;
        }

        Err(ErrorCode::SyntaxException(
            "Currently only key words are supported".to_string(),
        ))?;
    }
    Ok(nodes)
}

fn parse_literal_string(data: &str) -> std::result::Result<(usize, String), enquote::Error> {
    let mut escape = false;
    for (i, ch) in data.char_indices() {
        if i == 0 {
            continue;
        }
        match ch {
            '"' if !escape => {
                let end = i + 1;
                return enquote::unquote(&data[..end]).map(|s| (end, s));
            }
            '\\' if !escape => escape = true,
            _ if escape => escape = false,
            _ => {}
        }
    }
    Err(enquote::Error::UnexpectedEOF)
}

struct NumProc {
    desc: NumDesc, // number description

    sign: bool,            // '-' or '+'
    sign_wrote: bool,      // was sign write
    num_count: usize,      // number of write digits
    num_in: bool,          // is inside number
    num_curr: usize,       // current position in number
    out_pre_spaces: usize, // spaces before first digit

    number: Vec<char>,
    number_p: usize,

    inout: String,

    last_relevant: Option<(char, usize)>, // last relevant number after decimal point

    decimal: String,
    loc_negative_sign: String,
    loc_positive_sign: String,
    loc_thousands_sep: String,
    loc_currency_symbol: String,
}

impl NumProc {
    // ----------
    // Add digit or sign to number-string
    // ----------
    fn numpart_to_char(&mut self, id: NumPoz) {
        // Write sign if real number will write to output Note: IS_PREDEC_SPACE()
        // handle "9.9" --> " .1"
        if !self.sign_wrote
            && (self.num_curr >= self.out_pre_spaces
                || self.desc.flag.contains(NumFlag::Zero) && self.desc.zero_start == self.num_curr)
            && (!self.is_predec_space() || self.last_relevant.is_some())
        {
            if self.desc.flag.contains(NumFlag::LSign) {
                if matches!(self.desc.lsign, Some(NumLSign::Pre)) {
                    if self.sign {
                        self.inout.push_str(&self.loc_positive_sign)
                    } else {
                        self.inout.push_str(&self.loc_negative_sign)
                    }
                    self.sign_wrote = true;
                }
            } else if self.desc.flag.contains(NumFlag::Bracket) {
                if self.sign {
                    self.inout.push(' ')
                } else {
                    self.inout.push('<')
                }
                self.sign_wrote = true;
            } else if self.sign {
                if !self.desc.flag.contains(NumFlag::FillMode) {
                    self.inout.push(' '); // Write +
                }
                self.sign_wrote = true;
            } else {
                self.inout.push('-'); // Write -
                self.sign_wrote = true;
            }
        }

        // digits / FM / Zero / Dec. point
        if matches!(id, NumPoz::Tk9 | NumPoz::Tk0 | NumPoz::TkDec | NumPoz::TkD) {
            if self.num_curr < self.out_pre_spaces
                && (self.desc.zero_start > self.num_curr || !self.desc.flag.contains(NumFlag::Zero))
            {
                // Write blank space
                if !self.desc.flag.contains(NumFlag::FillMode) {
                    self.inout.push(' ') // Write ' '
                }
            } else if self.desc.flag.contains(NumFlag::Zero)
                && self.num_curr < self.out_pre_spaces
                && self.desc.zero_start <= self.num_curr
            {
                // Write ZERO
                self.inout.push('0'); // Write '0'
                self.num_in = true
            } else {
                // Write Decimal point
                if self.number.get(self.number_p).is_some_and(|c| *c == '.') {
                    if !self.last_relevant_is_dot()
                        || self.desc.flag.contains(NumFlag::FillMode) && self.last_relevant_is_dot()
                    // Ora 'n' -- FM9.9 --> 'n.'s
                    {
                        self.inout.push_str(&self.decimal) // Write DEC/D
                    }
                } else if self.last_relevant.is_some_and(|(_, i)| self.number_p > i)
                    && !matches!(id, NumPoz::Tk0)
                {
                }
                // '0.1' -- 9.9 --> '  .1'
                else if self.is_predec_space() {
                    if !self.desc.flag.contains(NumFlag::FillMode) {
                        self.inout.push(' ');
                    }
                    // '0' -- FM9.9 --> '0.'
                    else if self.last_relevant_is_dot() {
                        self.inout.push('0')
                    }
                } else if self.number_p < self.number.len() {
                    self.inout.push(self.number[self.number_p]); // Write DIGIT
                    self.num_in = true
                }
                if self.number_p < self.number.len() {
                    self.number_p += 1;
                }
            }

            let end = self.num_count
                + if self.out_pre_spaces > 0 { 1 } else { 0 }
                + if self.desc.flag.contains(NumFlag::Decimal) {
                    1
                } else {
                    0
                };

            let end = if self.last_relevant.is_some_and(|(_, i)| i == self.number_p) {
                self.num_curr
            } else {
                end
            };

            if self.num_curr + 1 == end {
                if self.sign_wrote && self.desc.flag.contains(NumFlag::Bracket) {
                    self.inout.push(if self.sign { ' ' } else { '>' })
                } else if self.desc.flag.contains(NumFlag::LSign)
                    && matches!(self.desc.lsign, Some(NumLSign::Post))
                {
                    self.inout.push_str(if self.sign {
                        &self.loc_positive_sign
                    } else {
                        &self.loc_negative_sign
                    })
                }
            }
        }

        self.num_curr += 1;
    }

    fn is_predec_space(&self) -> bool {
        !self.desc.flag.contains(NumFlag::Zero)
            && self.number_p == 0
            && self.number[0] == '0'
            && self.desc.post != 0
    }

    fn calc_last_relevant_decnum(&mut self) {
        let mut n = None;
        for (i, c) in self.number.iter().enumerate() {
            match n.as_ref() {
                Some(_) if *c != '0' => n = Some(i),
                None if *c == '.' => n = Some(i),
                _ => {}
            }
        }
        self.last_relevant = n.map(|n| (*self.number.get(n).unwrap(), n));
    }

    fn last_relevant_is_dot(&self) -> bool {
        self.last_relevant.is_some_and(|(c, _)| c == '.')
    }

    fn prepare_locale(&mut self) {
        // todo: True localization will be the next step
        self.loc_negative_sign = "-".to_string();
        self.loc_positive_sign = "+".to_string();
        self.loc_thousands_sep = ",".to_string();
        self.loc_currency_symbol = "$".to_string();
    }
}

pub struct FmtCacheEntry {
    format: Vec<FormatNode>,
    _str: String,
    desc: NumDesc,
}

impl FromStr for FmtCacheEntry {
    type Err = ErrorCode;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut desc = NumDesc::default();
        let format = parse_format(s, &NUM_KEYWORDS, Some(&mut desc))?;
        Ok(FmtCacheEntry {
            format,
            _str: s.to_string(),
            desc,
        })
    }
}

impl FmtCacheEntry {
    pub fn process_i64(&self, value: i64) -> Result<String> {
        let desc = self.desc.clone();
        let num_part = desc.i64_to_num_part(value)?;
        self.process(desc, num_part)
    }

    pub fn process_f64(&self, value: f64) -> Result<String> {
        let mut desc = self.desc.clone();
        let num_part = desc.f64_to_num_part(value)?;
        self.process(desc, num_part)
    }

    pub fn process_f32(&self, value: f32) -> Result<String> {
        let mut desc = self.desc.clone();
        let num_part = desc.f32_to_num_part(value)?;
        self.process(desc, num_part)
    }

    fn process(&self, desc: NumDesc, num_part: NumPart) -> Result<String> {
        num_processor(&self.format, desc, num_part)
    }
}

fn num_processor(nodes: &[FormatNode], desc: NumDesc, num_part: NumPart) -> Result<String> {
    let NumPart {
        sign,
        number,
        out_pre_spaces,
    } = num_part;
    let mut np = NumProc {
        desc,
        sign,
        sign_wrote: false,
        num_count: 0,
        num_in: false,
        num_curr: 0,
        out_pre_spaces,
        number: number.chars().collect(),
        number_p: 0,
        inout: String::new(),
        last_relevant: None,
        decimal: ".".to_string(),
        loc_negative_sign: String::new(),
        loc_positive_sign: String::new(),
        loc_thousands_sep: String::new(),
        loc_currency_symbol: String::new(),
    };

    if np.desc.zero_start > 0 {
        np.desc.zero_start -= 1;
    }

    if np.desc.flag.contains(NumFlag::Eeee) {
        return Ok(String::from_iter(np.number.iter()));
    }

    // Roman correction
    if np.desc.flag.contains(NumFlag::Roman) {
        unimplemented!()
    }

    // Sign

    // MI/PL/SG - write sign itself and not in number
    if np.desc.flag.intersects(NumFlag::Plus | NumFlag::Minus) {
        // if np.desc.flag.contains(NumFlag::Plus) && !np.desc.flag.contains(NumFlag::Minus) {
        //     np.sign_wrote = false; /* need sign */
        // } else {
        // TODO: Why is this not the same as the postgres implementation?
        np.sign_wrote = true; // needn't sign
    // }
    } else {
        if np.sign && np.desc.flag.contains(NumFlag::FillMode) {
            np.desc.flag.remove(NumFlag::Bracket)
        }

        if np.sign
            && np.desc.flag.contains(NumFlag::FillMode)
            && !np.desc.flag.contains(NumFlag::LSign)
        {
            np.sign_wrote = true // needn't sign
        } else {
            np.sign_wrote = false // need sign
        }
        if matches!(np.desc.lsign, Some(NumLSign::Pre)) && np.desc.pre == np.desc.pre_lsign_num {
            np.desc.lsign = Some(NumLSign::Post)
        }
    }

    // Count
    np.num_count = np.desc.post + np.desc.pre - 1;

    if np.desc.flag.contains(NumFlag::FillMode) && np.desc.flag.contains(NumFlag::Decimal) {
        np.calc_last_relevant_decnum();

        // If any '0' specifiers are present, make sure we don't strip
        // those digits.  But don't advance last_relevant beyond the last
        // character of the np.number string, which is a hazard if the
        // number got shortened due to precision limitations.
        if let Some(last_relevant) = np.last_relevant
            && np.desc.zero_end > np.out_pre_spaces
        {
            // note that np.number cannot be zero-length here
            let last_zero_pos = np.number.len() - 1;
            let last_zero_pos = last_zero_pos.min(np.desc.zero_end - np.out_pre_spaces);

            if last_relevant.1 < last_zero_pos {
                let ch = np.number[last_zero_pos];
                np.last_relevant = Some((ch, last_zero_pos))
            }
        }
    }

    if !np.sign_wrote && np.out_pre_spaces == 0 {
        np.num_count += 1;
    }

    // Locale
    if np.desc.need_locale {
        np.prepare_locale();
    }

    // Processor direct cycle
    for n in nodes.iter() {
        match n {
            // Format pictures actions
            FormatNode::Action(key) => match key.id {
                // Note: The locale sign is anchored to number and we
                // write it when we work with first or last number
                // (Tk0/Tk9).
                NumPoz::TkS => (),
                id @ (NumPoz::Tk9 | NumPoz::Tk0 | NumPoz::TkDec | NumPoz::TkD) => {
                    np.numpart_to_char(id)
                }

                NumPoz::TkComma => {
                    if np.num_in {
                        np.inout.push(',');
                        continue;
                    }
                    if !np.desc.flag.contains(NumFlag::FillMode) {
                        np.inout.push(' ')
                    }
                }

                NumPoz::TkG => {
                    if np.num_in {
                        np.inout.push_str(&np.loc_thousands_sep);
                        continue;
                    }
                    if np.desc.flag.contains(NumFlag::FillMode) {
                        continue;
                    }
                    let sep = " ".repeat(np.loc_thousands_sep.len());
                    np.inout.push_str(&sep);
                    continue;
                }

                NumPoz::TkL => {
                    np.inout.push_str(&np.loc_currency_symbol);
                }

                NumPoz::TkMI => {
                    if np.sign {
                        if !np.desc.flag.contains(NumFlag::FillMode) {
                            np.inout.push(' ');
                        }
                    } else {
                        np.inout.push('-');
                    }
                }

                NumPoz::TkPL => {
                    if np.sign {
                        np.inout.push('+');
                    } else if !np.desc.flag.contains(NumFlag::FillMode) {
                        np.inout.push(' ');
                    }
                }

                NumPoz::TkSG => np.inout.push(if np.sign { '+' } else { '-' }),

                NumPoz::TkPR => (),
                NumPoz::TkFM => (),
                _ => unimplemented!(),
            },
            FormatNode::End => break,
            FormatNode::Char(character) => {
                // In TO_CHAR, non-pattern characters in the format are copied to
                // the output.
                np.inout.push_str(character)
            }
            FormatNode::Space => np.inout.push(' '),
            _ => unimplemented!(),
        }
    }

    Ok(np.inout)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn i64_to_char(value: i64, fmt: &str) -> Result<String> {
        fmt.parse::<FmtCacheEntry>()?.process_i64(value)
    }

    fn f64_to_char(value: f64, fmt: &str) -> Result<String> {
        fmt.parse::<FmtCacheEntry>()?.process_f64(value)
    }

    fn f32_to_char(value: f32, fmt: &str) -> Result<String> {
        fmt.parse::<FmtCacheEntry>()?.process_f32(value)
    }

    #[test]
    fn test_i64() -> Result<()> {
        assert_eq!(" 123", i64_to_char(123, "999")?);
        assert_eq!("-123", i64_to_char(-123, "999")?);

        assert_eq!(" 0123", i64_to_char(123, "0999")?);
        assert_eq!("-0123", i64_to_char(-123, "0999")?);

        assert_eq!("   123", i64_to_char(123, "99999")?);
        assert_eq!("  -123", i64_to_char(-123, "99999")?);

        assert_eq!("    0123", i64_to_char(123, "9990999")?);
        assert_eq!("   -0123", i64_to_char(-123, "9990999")?);

        assert_eq!("    0123 ", i64_to_char(123, "9990999PR")?);
        assert_eq!("   <0123>", i64_to_char(-123, "9990999PR")?);

        assert_eq!("   12345", i64_to_char(12345, "9990999")?);
        assert_eq!("  -12345", i64_to_char(-12345, "9990999")?);

        assert_eq!("    0012.0", i64_to_char(12, "9990999.9")?);
        assert_eq!("   -0012.0", i64_to_char(-12, "9990999.9")?);
        assert_eq!("0012.", i64_to_char(12, "FM9990999.9")?);
        assert_eq!("-0012.", i64_to_char(-12, "FM9990999.9")?);

        assert_eq!(" ##", i64_to_char(123, "99")?);
        assert_eq!("-##", i64_to_char(-123, "99")?);

        assert_eq!(" ##.", i64_to_char(123, "99.")?);
        assert_eq!("-##.", i64_to_char(-123, "99.")?);

        assert_eq!(" ##.#", i64_to_char(123, "99.0")?);
        assert_eq!("-##.#", i64_to_char(-123, "99.0")?);

        assert_eq!(
            "  9223372036854775807",
            i64_to_char(i64::MAX, "99999999999999999999")?
        );
        assert_eq!(
            " -9223372036854775808",
            i64_to_char(i64::MIN, "99999999999999999999")?
        );
        assert_eq!(
            " -9223372036854775807",
            i64_to_char(i64::MIN + 1, "99999999999999999999")?
        );

        // Regarding the way the exponent part of the scientific notation is formatted,
        // there is a slight difference between the rust implementation and the c implementation.
        //  1.23456000e+05
        assert_eq!(" 1.23456000e5", i64_to_char(123456, "9.99999999EEEE")?);
        assert_eq!("-1.23456e5", i64_to_char(-123456, "9.99999EEEE")?);

        assert_eq!(" 4 8 5", i64_to_char(485, "9 9 9")?);
        assert_eq!(" 1,485", i64_to_char(1485, "9,999")?);

        assert_eq!("Good number: 485", i64_to_char(485, "\"Good number:\"999")?);

        assert_eq!("+485", i64_to_char(485, "SG999")?);
        assert_eq!("-485", i64_to_char(-485, "SG999")?);
        assert_eq!("4-85", i64_to_char(-485, "9SG99")?);

        assert_eq!("+485", i64_to_char(485, "PL999")?);
        assert_eq!(" 485", i64_to_char(-485, "PL999")?);

        assert_eq!("48+5", i64_to_char(485, "99PL9")?);
        assert_eq!("48 5", i64_to_char(-485, "99PL9")?);

        assert_eq!("485-", i64_to_char(-485, "999MI")?);
        assert_eq!("485 ", i64_to_char(485, "999MI")?);
        assert_eq!("485", i64_to_char(485, "FM999MI")?);

        assert_eq!(" 1,485", i64_to_char(1485, "9G999")?);
        assert_eq!("-1,485", i64_to_char(-1485, "9G999")?);

        assert_eq!("1,485", i64_to_char(1485, "FM9G999")?);
        assert_eq!("-1,485", i64_to_char(-1485, "FM9G999")?);

        assert_eq!("  12,345,678", i64_to_char(12345678, "999G999G999")?);
        assert_eq!(" -12,345,678", i64_to_char(-12345678, "999G999G999")?);

        assert_eq!("12,345,678", i64_to_char(12345678, "FM999G999G999")?);
        assert_eq!("-12,345,678", i64_to_char(-12345678, "FM999G999G999")?);

        assert_eq!("$ 485", i64_to_char(485, "L999")?);
        assert_eq!("$-485", i64_to_char(-485, "L999")?);

        assert_eq!("485+", i64_to_char(485, "999S")?);
        assert_eq!("485-", i64_to_char(-485, "999S")?);

        assert_eq!("+485", i64_to_char(485, "S999")?);
        assert_eq!("-485", i64_to_char(-485, "S999")?);

        Ok(())
    }

    #[test]
    fn test_float() -> Result<()> {
        assert_eq!(" 12.34", f64_to_char(12.34, "99.99")?);
        assert_eq!("-12.34", f64_to_char(-12.34, "99.99")?);
        assert_eq!("   .10", f64_to_char(0.1, "99.99")?);
        assert_eq!("  -.10", f64_to_char(-0.1, "99.99")?);

        assert_eq!(" 4.86e-4", f64_to_char(0.0004859, "9.99EEEE")?);
        assert_eq!("-4.86e-4", f64_to_char(-0.0004859, "9.99EEEE")?);

        assert_eq!(" 0.1", f64_to_char(0.1, "0.9")?);
        assert_eq!("-.1", f64_to_char(-0.1, "FM9.99")?);
        assert_eq!("-0.1", f64_to_char(-0.1, "FM90.99")?);

        assert_eq!(" 148.500", f64_to_char(148.5, "999.999")?);
        assert_eq!("148.5", f64_to_char(148.5, "FM999.999")?);
        assert_eq!("148.500", f64_to_char(148.5, "FM999.990")?);

        assert_eq!(
            "Pre: 485 Post: .800",
            f64_to_char(485.8, "\"Pre:\"999\" Post:\" .999")?
        );

        assert_eq!(" 148.500", f64_to_char(148.5, "999D999")?);

        assert_eq!(" 0003148.50", f32_to_char(3148.5, "0009999.999")?);
        assert_eq!(" 3,148.50", f32_to_char(3148.5, "9G999D999")?);

        assert_eq!(
            "  1234567040",
            f32_to_char(1.234_567e9, "99999999999D999999")?
        );
        assert_eq!(
            "     1234567",
            f32_to_char(1234567.0, "99999999999D999999")?
        );
        assert_eq!(
            "        1234.57",
            f32_to_char(1.234_567e3, "99999999999D999999")?
        );
        assert_eq!(
            "           1.23457",
            f32_to_char(1.234_567, "99999999999D999999")?
        );
        assert_eq!(
            "            .00123",
            f32_to_char(1.234_567e-3, "99999999999D999999")?
        );

        // assert_eq!("        CDLXXXV", f64_to_char(485, "RN")?);
        // assert_eq!("CDLXXXV", f64_to_char(485, "FMRN")?);
        // assert_eq!("V", f64_to_char(5.2, "FMRN")?);

        // assert_eq!(" 482nd", f64_to_char(482, "999th")?);

        // assert_eq!(" 12000", f64_to_char(12, "99V999")?);
        // assert_eq!(" 12400", f64_to_char(12.4, "99V999")?);
        // assert_eq!(" 125", f64_to_char(12.45, "99V9")?);

        Ok(())
    }
}
