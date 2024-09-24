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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use enumflags2::bitflags;
use enumflags2::BitFlags;

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
    EEEE,
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
    Char(Vec<char>),
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
            if self.flag.contains(NumFlag::EEEE) && !matches!(key.id, NumPoz::TkE) {
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
                    return Ok(());
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
        } else {
            unreachable!()
        }
    }
}

fn parse_format(
    mut str: &str,
    kw: &[KeyWord],
    mut num: Option<&mut NumDesc>,
) -> Result<Vec<FormatNode>> {
    let mut nodes = Vec::new();
    while !str.is_empty() {
        match kw.iter().find(|k| str.starts_with(k.name)) {
            Some(k) => {
                let n = FormatNode::Action(k.clone());

                if let Some(num) = num.as_mut() {
                    num.prepare(&n).map_err(ErrorCode::SyntaxException)?;
                }
                str = &str[k.name.len()..];

                nodes.push(n)
            }
            None => Err(ErrorCode::SyntaxException(
                "Currently only key words are supported".to_string(),
            ))?,
        }
    }
    Ok(nodes)
}

struct NumProc {
    desc: NumDesc, // number description

    sign: bool,            // '-' or '+'
    sign_wrote: bool,      // was sign write
    num_count: usize,      // number of write digits
    num_in: bool,          // is inside number
    num_curr: usize,       // current position in number
    out_pre_spaces: usize, // spaces before first digit

    _read_dec: bool,   // to_number - was read dec. point
    _read_post: usize, // to_number - number of dec. digit
    _read_pre: usize,  // to_number - number non-dec. digit

    number: Vec<char>,
    number_p: usize,

    inout: String,

    last_relevant: Option<(char, usize)>, // last relevant number after decimal point

    decimal: String,
    loc_negative_sign: String,
    loc_positive_sign: String,
    _loc_thousands_sep: String,
    _loc_currency_symbol: String,
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
                    self.inout.push(' '); /* Write + */
                }
                self.sign_wrote = true;
            } else {
                self.inout.push('-'); /* Write - */
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
                    self.inout.push(' ') /* Write ' ' */
                }
            } else if self.desc.flag.contains(NumFlag::Zero)
                && self.num_curr < self.out_pre_spaces
                && self.desc.zero_start <= self.num_curr
            {
                // Write ZERO
                self.inout.push('0'); /* Write '0' */
                self.num_in = true
            } else {
                // Write Decimal point
                if self.number.get(self.number_p).is_some_and(|c| *c == '.') {
                    if !self.last_relevant_is_dot() {
                        self.inout.push_str(&self.decimal) /* Write DEC/D */
                    }
                    // Ora 'n' -- FM9.9 --> 'n.'
                    else if self.desc.flag.contains(NumFlag::FillMode)
                        && self.last_relevant_is_dot()
                    {
                        self.inout.push_str(&self.decimal) /* Write DEC/D */
                    }
                } else {
                    if self.last_relevant.is_some_and(|(_, i)| self.number_p > i)
                        && !matches!(id, NumPoz::Tk0)
                    {
                    }
                    // '0.1' -- 9.9 --> '  .1'
                    else if self.is_predec_space() {
                        if self.desc.flag.contains(NumFlag::FillMode) {
                            self.inout.push(' ');
                        }
                        // '0' -- FM9.9 --> '0.'
                        else if self.last_relevant_is_dot() {
                            self.inout.push('0')
                        }
                    } else {
                        if self.number_p < self.number.len() {
                            self.inout.push(self.number[self.number_p]); /* Write DIGIT */
                            self.num_in = true
                        }
                    }
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
        self.last_relevant = n.map(|n| (*self.number.iter().nth(n).unwrap(), n));
    }

    fn last_relevant_is_dot(&self) -> bool {
        self.last_relevant.is_some_and(|(c, _)| c == '.')
    }
}

fn num_processor(
    nodes: &[FormatNode],
    desc: NumDesc,
    number: String,
    out_pre_spaces: usize,
    sign: bool,
) -> String {
    let mut np = NumProc {
        desc,
        sign,
        sign_wrote: false,
        num_count: 0,
        num_in: false,
        num_curr: 0,
        out_pre_spaces,
        _read_dec: false,
        _read_post: 0,
        _read_pre: 0,
        number: number.chars().collect(),
        number_p: 0,
        inout: String::new(),
        last_relevant: None,
        decimal: ".".to_string(),
        loc_negative_sign: String::new(),
        loc_positive_sign: String::new(),
        _loc_thousands_sep: String::new(),
        _loc_currency_symbol: String::new(),
    };

    if np.desc.zero_start > 0 {
        np.desc.zero_start -= 1;
    }

    // Roman correction
    if np.desc.flag.contains(NumFlag::Roman) {
        unimplemented!()
    }

    // Sign

    // MI/PL/SG - write sign itself and not in number
    if np.desc.flag.contains(NumFlag::Plus | NumFlag::Minus) {
        if np.desc.flag.contains(NumFlag::Plus) && !np.desc.flag.contains(NumFlag::Minus) {
            np.sign_wrote = false; /* need sign */
        } else {
            np.sign_wrote = true; /* needn't sign */
        }
    } else {
        if np.sign && np.desc.flag.contains(NumFlag::FillMode) {
            np.desc.flag.remove(NumFlag::Bracket)
        }

        if np.sign
            && np.desc.flag.contains(NumFlag::FillMode)
            && !np.desc.flag.contains(NumFlag::LSign)
        {
            np.sign_wrote = true /* needn't sign */
        } else {
            np.sign_wrote = false /* need sign */
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
        if let Some(last_relevant) = np.last_relevant {
            if np.desc.zero_end > np.out_pre_spaces {
                // note that np.number cannot be zero-length here
                let last_zero_pos = np.number.len() - 1;
                let last_zero_pos = last_zero_pos.min(np.desc.zero_end - np.out_pre_spaces);

                if last_relevant.1 < last_zero_pos {
                    let ch = np.number[last_zero_pos];
                    np.last_relevant = Some((ch, last_zero_pos))
                }
            }
        }
    }

    if !np.sign_wrote && np.out_pre_spaces == 0 {
        np.num_count += 1;
    }

    // Locale
    // 	NUM_prepare_locale(Np);

    // Processor direct cycle
    for n in nodes.iter() {
        match n {
            // Format pictures actions
            FormatNode::Action(key) => match key.id {
                id @ (NumPoz::Tk9 | NumPoz::Tk0 | NumPoz::TkDec | NumPoz::TkD) => {
                    np.numpart_to_char(id)
                }
                _ => unimplemented!(),
            },
            FormatNode::End => break,
            _ => unimplemented!(),
        }
    }

    np.inout
}

fn i32_to_char(value: i32, fmt: &str) -> Result<String> {
    let mut desc = NumDesc::default();
    let nodes = parse_format(fmt, &NUM_KEYWORDS, Some(&mut desc))?;

    let sign = value >= 0;
    let (numstr, out_pre_spaces) = if desc.flag.contains(NumFlag::Roman) {
        unimplemented!()
    } else if desc.flag.contains(NumFlag::EEEE) {
        // we can do it easily because f32 won't lose any precision
        let orgnum = format!("{:+.*e}", desc.post, value as f32);

        // Swap a leading positive sign for a space.
        let orgnum = orgnum.replace("+", "_");

        (orgnum, 0)
    } else {
        let mut orgnum = if desc.flag.contains(NumFlag::Multi) {
            unimplemented!()
        } else {
            format!("{}", value.abs())
        };

        let numstr_pre_len = orgnum.len();

        // post-decimal digits?  Pad out with zeros.
        if desc.post > 0 {
            orgnum.push('.');
            orgnum.push_str(&"0".repeat(desc.post))
        }

        match numstr_pre_len.cmp(&desc.pre) {
            // needs padding?
            std::cmp::Ordering::Less => (orgnum, desc.pre - numstr_pre_len),
            std::cmp::Ordering::Equal => (orgnum, 0),
            std::cmp::Ordering::Greater => {
                // overflowed prefix digit format?
                (["#".repeat(desc.pre), "#".repeat(desc.post)].join("."), 0)
            }
        }
    };

    Ok(num_processor(&nodes, desc, numstr, out_pre_spaces, sign))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i32() -> Result<()> {
        assert_eq!(" 123", i32_to_char(123, "999")?);
        assert_eq!("-123", i32_to_char(-123, "999")?);

        assert_eq!(" 0123", i32_to_char(123, "0999")?);
        assert_eq!("-0123", i32_to_char(-123, "0999")?);

        assert_eq!("   123", i32_to_char(123, "99999")?);
        assert_eq!("  -123", i32_to_char(-123, "99999")?);

        assert_eq!("    0123", i32_to_char(123, "9990999")?);
        assert_eq!("   -0123", i32_to_char(-123, "9990999")?);

        assert_eq!("   12345", i32_to_char(12345, "9990999")?);
        assert_eq!("  -12345", i32_to_char(-12345, "9990999")?);

        assert_eq!(" ##", i32_to_char(123, "99")?);
        assert_eq!("-##", i32_to_char(-123, "99")?);

        assert_eq!(" ##.", i32_to_char(123, "99.")?);
        assert_eq!("-##.", i32_to_char(-123, "99.")?);

        assert_eq!(" ##.#", i32_to_char(123, "99.0")?);
        assert_eq!("-##.#", i32_to_char(-123, "99.0")?);

        assert_eq!("    0012.0", i32_to_char(12, "9990999.9")?);

        Ok(())
    }
}
