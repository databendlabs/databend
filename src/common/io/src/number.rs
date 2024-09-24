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
use enumflags2::BitFlags;

// https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/formatting.c

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone, Copy)]
enum NumLSign {
    Pre,
    Post,
}

#[derive(Debug, Clone)]
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

#[derive(Debug)]
struct FormatNode {
    typ: NodeType,
    character: Vec<char>, // if type is CHAR
    suffix: u8,           // keyword prefix/suffix code, if any
    key: KeyWord,         // if type is ACTION
}

#[derive(Debug)]
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

fn parse_format(
    mut str: &str,
    kw: &[KeyWord],
    mut num: Option<&mut NumDesc>,
) -> Result<Vec<FormatNode>> {
    let mut nodes = Vec::new();
    while !str.is_empty() {
        match kw.iter().find(|k| str.starts_with(k.name)) {
            Some(k) => {
                let n = FormatNode {
                    typ: NodeType::Action,
                    character: Vec::new(),
                    suffix: 0, // todo
                    key: k.clone(),
                };

                if let Some(num) = num.as_mut() {
                    num.prepare(&n)?
                }
                str = &str[k.name.len()..];

                nodes.push(n)
            }
            None => todo!(),
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

    read_dec: bool,   // to_number - was read dec. point
    read_post: usize, // to_number - number of dec. digit
    read_pre: usize,  // to_number - number non-dec. digit

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
                    self.inout.push(' '); /* Write + */
                }
                self.sign_wrote = true;
            } else {
                // Write -
                self.inout.push('-');
                self.sign_wrote = true;
            }
        }

        //  if (Np->sign_wrote == false &&
        // 	(Np->num_curr >= Np->out_pre_spaces || (IS_ZERO(Np->Num) && Np->Num->zero_start == Np->num_curr)) &&
        // 	(IS_PREDEC_SPACE(Np) == false || (Np->last_relevant && *Np->last_relevant == '.')))
        // {

        // }

        match id {
            NumPoz::Num9 | NumPoz::Num0 | NumPoz::NumDec | NumPoz::NumD => {
                if self.num_curr < self.out_pre_spaces
                    && (self.desc.zero_start > self.num_curr
                        || !self.desc.flag.contains(NumFlag::Zero))
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
                            && !matches!(id, NumPoz::Num0)
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
            }

            _ => unimplemented!(),
        }

        self.num_curr += 1;
    }

    fn is_predec_space(&self) -> bool {
        !self.desc.flag.contains(NumFlag::Zero) &&
         // self.number == self.number_p &&
         // 		 (_n)->number == (_n)->number_p && \
// 		 *(_n)->number == '0' && \
          self.desc.post !=0
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
        read_dec: false,
        read_post: 0,
        read_pre: 0,
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

    // 	MemSet(Np, 0, sizeof(NUMProc));

    // 	Np->number = number;
    // 	Np->inout = inout;
    // 	Np->last_relevant = NULL;
    // 	Np->read_post = 0;
    // 	Np->read_pre = 0;
    // 	Np->read_dec = false;

    // 	if (Np->Num->zero_start)
    // 		--Np->Num->zero_start;

    // 	if (IS_EEEE(Np->Num))
    // 	{
    // 		if (!Np->is_to_char)
    // 			ereport(ERROR,
    // 					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
    // 					 errmsg("\"EEEE\" not supported for input")));
    // 		return strcpy(inout, number);
    // 	}

    // 	/*
    // 	 * Roman correction
    // 	 */
    // 	if (IS_ROMAN(Np->Num))
    // 	{
    // 	}

    // 	/*
    // 	 * Sign
    // 	 */
    // 	if (is_to_char)
    // 	{
    // 		Np->sign = sign;

    // 		/* MI/PL/SG - write sign itself and not in number */
    // 		if (IS_PLUS(Np->Num) || IS_MINUS(Np->Num))
    // 		{
    // 			if (IS_PLUS(Np->Num) && IS_MINUS(Np->Num) == false)
    // 				Np->sign_wrote = false; /* need sign */
    // 			else
    // 				Np->sign_wrote = true;	/* needn't sign */
    // 		}
    // 		else
    // 		{
    if np.sign && np.desc.flag.contains(NumFlag::FillMode) {
        // 					Np->Num->flag &= ~NUM_F_BRACKET;
    }

    if np.sign && np.desc.flag.contains(NumFlag::FillMode) && !np.desc.flag.contains(NumFlag::LSign)
    {
        np.sign_wrote = true /* needn't sign */
    } else {
        np.sign_wrote = false /* need sign */
    }
    if matches!(np.desc.lsign, Some(NumLSign::Pre)) && np.desc.pre == np.desc.pre_lsign_num {
        np.desc.lsign = Some(NumLSign::Post)
    }

    // Count
    np.num_count = np.desc.post + np.desc.pre - 1;

    // 	if (is_to_char)
    // 	{
    // 		Np->out_pre_spaces = to_char_out_pre_spaces;

    if np.desc.flag.contains(NumFlag::FillMode) && np.desc.flag.contains(NumFlag::Decimal) {
        np.calc_last_relevant_decnum();

        // 			/*
        // 			 * If any '0' specifiers are present, make sure we don't strip
        // 			 * those digits.  But don't advance last_relevant beyond the last
        // 			 * character of the Np->number string, which is a hazard if the
        // 			 * number got shortened due to precision limitations.
        // 			 */
        // 			if (Np->last_relevant && Np->Num->zero_end > Np->out_pre_spaces)
        // 			{
        // 				int			last_zero_pos;
        // 				char	   *last_zero;

        // 				/* note that Np->number cannot be zero-length here */
        // 				last_zero_pos = strlen(Np->number) - 1;
        // 				last_zero_pos = Min(last_zero_pos,
        // 									Np->Num->zero_end - Np->out_pre_spaces);
        // 				last_zero = Np->number + last_zero_pos;
        // 				if (Np->last_relevant < last_zero)
        // 					Np->last_relevant = last_zero;
        // 			}
    }

    if !np.sign_wrote && np.out_pre_spaces == 0 {
        np.num_count += 1;
    }

    // 	/*
    // 	 * Locale
    // 	 */
    // 	NUM_prepare_locale(Np);

    // 	/*
    // 	 * Processor direct cycle
    // 	 */
    for n in nodes.iter() {
        // 		if (!Np->is_to_char)
        // 		{
        // 			/*
        // 			 * Check at least one byte remains to be scanned.  (In actions
        // 			 * below, must use AMOUNT_TEST if we want to read more bytes than
        // 			 * that.)
        // 			 */
        // 			if (OVERLOAD_TEST)
        // 				break;
        // 		}

        // 		/*
        // 		 * Format pictures actions
        // 		 */
        // 		if (n->type == NODE_TYPE_ACTION)
        // 		{
        // 			/*
        // 			 * Create/read digit/zero/blank/sign/special-case
        // 			 *
        // 			 * 'NUM_S' note: The locale sign is anchored to number and we
        // 			 * read/write it when we work with first or last number
        // 			 * (NUM_0/NUM_9).  This is why NUM_S is missing in switch().
        // 			 *
        // 			 * Notice the "Np->inout_p++" at the bottom of the loop.  This is
        // 			 * why most of the actions advance inout_p one less than you might
        // 			 * expect.  In cases where we don't want that increment to happen,
        // 			 * a switch case ends with "continue" not "break".
        // 			 */
        match n.key.id {
            id @ (NumPoz::Num9 | NumPoz::Num0 | NumPoz::NumDec | NumPoz::NumD) => {
                np.numpart_to_char(id)
            }

            NumPoz::NumComma => todo!(),

            NumPoz::NumB => todo!(),
            NumPoz::NumC => todo!(),
            NumPoz::NumE => todo!(),

            _ => unimplemented!(),
        }

        // 			switch (n->key->id)
        // 			{
        // 				case NUM_9:
        // 				case NUM_0:
        // 				case NUM_DEC:
        // 				case NUM_D:

        // 				case NUM_COMMA:
        // 					if (Np->is_to_char)
        // 					{
        // 						if (!Np->num_in)
        // 						{
        // 							if (IS_FILLMODE(Np->Num))
        // 								continue;
        // 							else
        // 								*Np->inout_p = ' ';
        // 						}
        // 						else
        // 							*Np->inout_p = ',';
        // 					}
        // 					else
        // 					{
        // 						if (!Np->num_in)
        // 						{
        // 							if (IS_FILLMODE(Np->Num))
        // 								continue;
        // 						}
        // 						if (*Np->inout_p != ',')
        // 							continue;
        // 					}
        // 					break;

        // 				case NUM_G:
        // 					pattern = Np->L_thousands_sep;
        // 					pattern_len = strlen(pattern);
        // 					if (Np->is_to_char)
        // 					{
        // 						if (!Np->num_in)
        // 						{
        // 							if (IS_FILLMODE(Np->Num))
        // 								continue;
        // 							else
        // 							{
        // 								/* just in case there are MB chars */
        // 								pattern_len = pg_mbstrlen(pattern);
        // 								memset(Np->inout_p, ' ', pattern_len);
        // 								Np->inout_p += pattern_len - 1;
        // 							}
        // 						}
        // 						else
        // 						{
        // 							strcpy(Np->inout_p, pattern);
        // 							Np->inout_p += pattern_len - 1;
        // 						}
        // 					}
        // 					else
        // 					{
        // 						if (!Np->num_in)
        // 						{
        // 							if (IS_FILLMODE(Np->Num))
        // 								continue;
        // 						}

        // 						/*
        // 						 * Because L_thousands_sep typically contains data
        // 						 * characters (either '.' or ','), we can't use
        // 						 * NUM_eat_non_data_chars here.  Instead skip only if
        // 						 * the input matches L_thousands_sep.
        // 						 */
        // 						if (AMOUNT_TEST(pattern_len) &&
        // 							strncmp(Np->inout_p, pattern, pattern_len) == 0)
        // 							Np->inout_p += pattern_len - 1;
        // 						else
        // 							continue;
        // 					}
        // 					break;

        // 				case NUM_L:
        // 					pattern = Np->L_currency_symbol;
        // 					if (Np->is_to_char)
        // 					{
        // 						strcpy(Np->inout_p, pattern);
        // 						Np->inout_p += strlen(pattern) - 1;
        // 					}
        // 					else
        // 					{
        // 						NUM_eat_non_data_chars(Np, pg_mbstrlen(pattern), input_len);
        // 						continue;
        // 					}
        // 					break;

        // 				case NUM_RN:
        // 					if (IS_FILLMODE(Np->Num))
        // 					{
        // 						strcpy(Np->inout_p, Np->number_p);
        // 						Np->inout_p += strlen(Np->inout_p) - 1;
        // 					}
        // 					else
        // 					{
        // 						sprintf(Np->inout_p, "%15s", Np->number_p);
        // 						Np->inout_p += strlen(Np->inout_p) - 1;
        // 					}
        // 					break;

        // 				case NUM_rn:
        // 					if (IS_FILLMODE(Np->Num))
        // 					{
        // 						strcpy(Np->inout_p, asc_tolower_z(Np->number_p));
        // 						Np->inout_p += strlen(Np->inout_p) - 1;
        // 					}
        // 					else
        // 					{
        // 						sprintf(Np->inout_p, "%15s", asc_tolower_z(Np->number_p));
        // 						Np->inout_p += strlen(Np->inout_p) - 1;
        // 					}
        // 					break;

        // 				case NUM_th:
        // 					if (IS_ROMAN(Np->Num) || *Np->number == '#' ||
        // 						Np->sign == '-' || IS_DECIMAL(Np->Num))
        // 						continue;

        // 					if (Np->is_to_char)
        // 					{
        // 						strcpy(Np->inout_p, get_th(Np->number, TH_LOWER));
        // 						Np->inout_p += 1;
        // 					}
        // 					else
        // 					{
        // 						/* All variants of 'th' occupy 2 characters */
        // 						NUM_eat_non_data_chars(Np, 2, input_len);
        // 						continue;
        // 					}
        // 					break;

        // 				case NUM_TH:
        // 					if (IS_ROMAN(Np->Num) || *Np->number == '#' ||
        // 						Np->sign == '-' || IS_DECIMAL(Np->Num))
        // 						continue;

        // 					if (Np->is_to_char)
        // 					{
        // 						strcpy(Np->inout_p, get_th(Np->number, TH_UPPER));
        // 						Np->inout_p += 1;
        // 					}
        // 					else
        // 					{
        // 						/* All variants of 'TH' occupy 2 characters */
        // 						NUM_eat_non_data_chars(Np, 2, input_len);
        // 						continue;
        // 					}
        // 					break;

        // 				case NUM_MI:
        // 					if (Np->is_to_char)
        // 					{
        // 						if (Np->sign == '-')
        // 							*Np->inout_p = '-';
        // 						else if (IS_FILLMODE(Np->Num))
        // 							continue;
        // 						else
        // 							*Np->inout_p = ' ';
        // 					}
        // 					else
        // 					{
        // 						if (*Np->inout_p == '-')
        // 							*Np->number = '-';
        // 						else
        // 						{
        // 							NUM_eat_non_data_chars(Np, 1, input_len);
        // 							continue;
        // 						}
        // 					}
        // 					break;

        // 				case NUM_PL:
        // 					if (Np->is_to_char)
        // 					{
        // 						if (Np->sign == '+')
        // 							*Np->inout_p = '+';
        // 						else if (IS_FILLMODE(Np->Num))
        // 							continue;
        // 						else
        // 							*Np->inout_p = ' ';
        // 					}
        // 					else
        // 					{
        // 						if (*Np->inout_p == '+')
        // 							*Np->number = '+';
        // 						else
        // 						{
        // 							NUM_eat_non_data_chars(Np, 1, input_len);
        // 							continue;
        // 						}
        // 					}
        // 					break;

        // 				case NUM_SG:
        // 					if (Np->is_to_char)
        // 						*Np->inout_p = Np->sign;
        // 					else
        // 					{
        // 						if (*Np->inout_p == '-')
        // 							*Np->number = '-';
        // 						else if (*Np->inout_p == '+')
        // 							*Np->number = '+';
        // 						else
        // 						{
        // 							NUM_eat_non_data_chars(Np, 1, input_len);
        // 							continue;
        // 						}
        // 					}
        // 					break;

        // 				default:
        // 					continue;
        // 					break;
        // 			}
        // 		}
        // 		else
        // 		{
        // 			/*
        // 			 * In TO_CHAR, non-pattern characters in the format are copied to
        // 			 * the output.  In TO_NUMBER, we skip one input character for each
        // 			 * non-pattern format character, whether or not it matches the
        // 			 * format character.
        // 			 */
        // 			if (Np->is_to_char)
        // 			{
        // 				strcpy(Np->inout_p, n->character);
        // 				Np->inout_p += strlen(Np->inout_p);
        // 			}
        // 			else
        // 			{
        // 				Np->inout_p += pg_mblen(Np->inout_p);
        // 			}
        // 			continue;
        // 		}
        // 		Np->inout_p++;
    }

    // 	if (Np->is_to_char)
    // 	{
    // 		*Np->inout_p = '\0';
    // 		return Np->inout;
    // 	}
    // 	else
    // 	{
    // 		if (*(Np->number_p - 1) == '.')
    // 			*(Np->number_p - 1) = '\0';
    // 		else
    // 			*Np->number_p = '\0';

    // 		/*
    // 		 * Correction - precision of dec. number
    // 		 */
    // 		Np->Num->post = Np->read_post;

    // #ifdef DEBUG_TO_FROM_CHAR
    // 		elog(DEBUG_elog_output, "TO_NUMBER (number): '%s'", Np->number);
    // #endif
    // 		return Np->number;
    // 	}

    np.inout
}

fn i32_to_char(value: i32, fmt: &str) -> Result<String> {
    let mut desc = NumDesc::default();
    let nodes = parse_format(fmt, &NUM_KEYWORDS, Some(&mut desc)).unwrap();

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
            todo!();
            // orgnum = DatumGetCString(DirectFunctionCall1(int4out,
            // 											 Int32GetDatum(value * ((int32) pow((double) 10, (double) Num.multi)))));
            // desc.pre += desc.multi;
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

        // assert_eq!(" ##.#",i32_to_char(123,"99.0")?);
        // assert_eq!("-##.#",i32_to_char(-123,"99.0")?);

        Ok(())
    }

    fn run_test(num: &str, fmt: &str, sign: bool) {
        let mut desc = NumDesc::default();
        let nodes = parse_format(fmt, &NUM_KEYWORDS, Some(&mut desc)).unwrap();

        let numstr = num.to_string();

        let numstr_pre_len = match numstr.find('.') {
            Some(i) => i,
            None => numstr.len(),
        };

        let out_pre_spaces = if numstr_pre_len < desc.pre {
            desc.pre - numstr_pre_len
        } else {
            0
        };

        let out = num_processor(&nodes, desc, numstr, out_pre_spaces, sign);

        println!("{out:?}")
    }
}

// 123 '9990999' 0123
// 123 '99900999.999' 0123.000
