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

use std::borrow::Cow;

use memchr::memchr;

use crate::VolnitskyBase;

#[derive(Debug, Clone)]
pub enum LikePattern<'a> {
    // e.g. 'Arrow'.
    OrdinalStr(Cow<'a, [u8]>),
    // e.g. '%rrow'.
    StartOfPercent(Cow<'a, [u8]>),
    // e.g. 'Arrow%'.
    EndOfPercent(Cow<'a, [u8]>),
    // e.g. '%Arrow%'.
    SurroundByPercent(VolnitskyBase<'a>),
    // e.g. 'A%row', 'A_row', 'A\\%row'.
    ComplexPattern(Cow<'a, [u8]>),
    // Only includes %, e.g. 'A%r%w'.
    // SimplePattern is composed of: (has_start_percent, has_end_percent, segments).
    SimplePattern((bool, bool, Vec<Vec<u8>>)),
    Constant(bool),
}

impl PartialEq for LikePattern<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LikePattern::OrdinalStr(a), LikePattern::OrdinalStr(b)) => a == b,
            (LikePattern::StartOfPercent(a), LikePattern::StartOfPercent(b)) => a == b,
            (LikePattern::EndOfPercent(a), LikePattern::EndOfPercent(b)) => a == b,
            (LikePattern::SurroundByPercent(a), LikePattern::SurroundByPercent(b)) => a == b,
            (LikePattern::ComplexPattern(a), LikePattern::ComplexPattern(b)) => a == b,
            (LikePattern::SimplePattern((a, b, c)), LikePattern::SimplePattern((d, e, f))) => {
                a == d && b == e && c == f
            }
            (LikePattern::Constant(a), LikePattern::Constant(b)) => a == b,
            _ => false,
        }
    }
}

impl LikePattern<'_> {
    #[inline]
    pub fn compare(&self, haystack: &[u8]) -> bool {
        match self {
            LikePattern::OrdinalStr(s) => haystack == s.as_ref(),
            // '%abc'
            LikePattern::StartOfPercent(s) => haystack.ends_with(s),
            // 'abc%'
            LikePattern::EndOfPercent(s) => haystack.starts_with(s),
            // '%abc%'
            LikePattern::SurroundByPercent(s) => s.search(haystack).is_some(),
            LikePattern::ComplexPattern(s) => Self::complex_pattern(haystack, s),
            LikePattern::SimplePattern((has_start_percent, has_end_percent, segments)) => {
                Self::simple_pattern(haystack, *has_start_percent, *has_end_percent, segments)
            }
            LikePattern::Constant(b) => *b,
        }
    }

    /// Borrow from [tikv](https://github.com/tikv/tikv/blob/fe997db4db8a5a096f8a45c0db3eb3c2e5879262/components/tidb_query_expr/src/impl_like.rs)
    pub fn complex_pattern(haystack: &[u8], pattern: &[u8]) -> bool {
        // current search positions in pattern and target.
        let (mut px, mut tx) = (0, 0);
        // positions for backtrace.
        let (mut next_px, mut next_tx) = (0, 0);
        while px < pattern.len() || tx < haystack.len() {
            if let Some((c, mut poff)) = decode_one(&pattern[px..]) {
                let code: u32 = c.into();
                if code == '_' as u32 {
                    if let Some((_, toff)) = decode_one(&haystack[tx..]) {
                        px += poff;
                        tx += toff;
                        continue;
                    }
                } else if code == '%' as u32 {
                    // update the backtrace point.
                    next_px = px;
                    px += poff;
                    next_tx = tx;
                    next_tx += if let Some((_, toff)) = decode_one(&haystack[tx..]) {
                        toff
                    } else {
                        1
                    };
                    continue;
                } else {
                    if code == '\\' as u32 && px + poff < pattern.len() {
                        px += poff;
                        poff = if let Some((_, off)) = decode_one(&pattern[px..]) {
                            off
                        } else {
                            break;
                        }
                    }
                    if let Some((_, toff)) = decode_one(&haystack[tx..]) {
                        if let std::cmp::Ordering::Equal =
                            haystack[tx..tx + toff].cmp(&pattern[px..px + poff])
                        {
                            tx += toff;
                            px += poff;
                            continue;
                        }
                    }
                }
            }
            // mismatch and backtrace to last %.
            if 0 < next_tx && next_tx <= haystack.len() {
                px = next_px;
                tx = next_tx;
                continue;
            }
            return false;
        }
        true
    }

    pub fn simple_pattern(
        haystack: &[u8],
        has_start_percent: bool,
        has_end_percent: bool,
        segments: &[Vec<u8>],
    ) -> bool {
        let haystack_len = haystack.len();
        if haystack_len == 0 {
            return false;
        }
        let segments_len = segments.len();
        debug_assert!(haystack_len > 0);
        debug_assert!(segments_len > 1);
        let mut haystack_start_idx = 0;
        let mut segment_idx = 0;
        if !has_start_percent {
            let segment = &segments[0];
            let haystack_end = haystack_start_idx + segment.len();
            if haystack_end > haystack_len {
                return false;
            }
            // # Safety
            // `haystack_start_idx` = 0, `haystack_len` > 0, `haystack_end` <= `haystack_len`.
            if unsafe { haystack.get_unchecked(haystack_start_idx..haystack_end) } != segment {
                return false;
            }
            haystack_start_idx = haystack_end;
            segment_idx += 1;
        }
        while segment_idx < segments_len {
            if haystack_start_idx >= haystack_len {
                return false;
            }
            let segment = &segments[segment_idx];
            if segment_idx == segments_len - 1 && !has_end_percent {
                if haystack_len - haystack_start_idx < segment.len() {
                    return false;
                }
                // # Safety
                // `haystack_start_idx` + `segment.len()` <= `haystack_len`.
                if unsafe { haystack.get_unchecked((haystack_len - segment.len())..) } != segment {
                    return false;
                }
            } else if let Some(offset) =
                unsafe { find(haystack.get_unchecked(haystack_start_idx..), segment) }
            {
                haystack_start_idx += offset;
            } else {
                return false;
            }
            segment_idx += 1;
        }
        true
    }
}

#[inline]
pub fn is_like_pattern_escape(c: char) -> bool {
    c == '%' || c == '_' || c == '\\'
}

/// Check the like pattern type.
/// For example:
/// 'a\\%row'
/// '\\%' will be escaped to a percent. Need transform to `a%row`.
#[inline]
pub fn generate_like_pattern<'a, B: Into<Cow<'a, [u8]>>>(
    pattern: B,
    haystack_size_hint: usize,
) -> LikePattern<'a> {
    let pattern: Cow<'a, [u8]> = pattern.into();
    let len = pattern.len();
    if len == 0 {
        return LikePattern::Constant(true);
    }

    let mut index = 0;
    let mut first_non_percent = 0;
    let mut percent_num = 0;
    let has_start_percent = pattern[0] == b'%';
    let mut has_end_percent = false;
    let mut segments = Vec::new();
    let mut simple_pattern = true;
    if has_start_percent {
        index += 1;
        first_non_percent += 1;
        percent_num += 1;
    }

    while index < len {
        match pattern[index] {
            b'_' => return LikePattern::ComplexPattern(pattern),
            b'%' => {
                percent_num += 1;
                if index > first_non_percent {
                    segments.push(pattern[first_non_percent..index].to_vec());
                }
                first_non_percent = index + 1;
                if index == len - 1 {
                    has_end_percent = true;
                }
            }
            b'\\' => {
                simple_pattern = false;
                if index < len - 1 {
                    index += 1;
                    if is_like_pattern_escape(pattern[index] as char) {
                        return LikePattern::ComplexPattern(pattern);
                    }
                }
            }
            _ => {}
        }
        index += 1;
    }

    match percent_num {
        0 => LikePattern::OrdinalStr(pattern),
        1 if has_start_percent => match pattern {
            Cow::Borrowed(v) => LikePattern::StartOfPercent(Cow::Borrowed(&v[1..])),
            Cow::Owned(v) => LikePattern::StartOfPercent(Cow::Owned(v[1..].to_vec())),
        },
        1 if has_end_percent => match pattern {
            Cow::Borrowed(v) => LikePattern::EndOfPercent(Cow::Borrowed(&v[..v.len() - 1])),
            Cow::Owned(v) => LikePattern::EndOfPercent(Cow::Owned(v[..v.len() - 1].to_vec())),
        },
        2 if has_start_percent && has_end_percent => {
            let needle = &pattern[1..len - 1];
            if needle.is_empty() {
                LikePattern::Constant(true)
            } else {
                let needle = match pattern {
                    Cow::Borrowed(v) => Cow::Borrowed(&v[1..v.len() - 1]),
                    Cow::Owned(v) => Cow::Owned(v[1..v.len() - 1].to_vec()),
                };
                LikePattern::SurroundByPercent(VolnitskyBase::new_cow(needle, haystack_size_hint))
            }
        }
        _ => {
            if simple_pattern {
                if first_non_percent < len {
                    segments.push(pattern[first_non_percent..len].to_vec());
                }
                LikePattern::SimplePattern((has_start_percent, has_end_percent, segments))
            } else {
                LikePattern::ComplexPattern(pattern)
            }
        }
    }
}

#[inline]
fn decode_one(data: &[u8]) -> Option<(u8, usize)> {
    if data.is_empty() {
        None
    } else {
        Some((data[0], 1))
    }
}

fn find(mut haystack: &[u8], needle: &[u8]) -> Option<usize> {
    let haystack_len = haystack.len();
    let needle_len = needle.len();
    if needle_len > haystack_len {
        return None;
    }
    let offset = memchr(needle[0], haystack)?;
    // # Safety
    // The `offset` returned by `memchr` is less than `haystack_len`.
    haystack = unsafe { haystack.get_unchecked(offset..) };
    let haystack_len = haystack.len();
    if needle_len > haystack_len {
        return None;
    }
    // Inspired by fast_strstr (https://github.com/RaphaelJ/fast_strstr).
    let mut checksum: i64 = 0;
    for i in 0..needle_len {
        // # Safety
        // `needle_len` <= haystack_len
        unsafe {
            checksum += *haystack.get_unchecked(i) as i64;
            checksum -= *needle.get_unchecked(i) as i64;
        }
    }
    let mut idx = 0;
    loop {
        // # Safety
        // `idx` < `haystack_len` and `idx` + `needle_len` <= `haystack_len`.
        unsafe {
            if checksum == 0
                && haystack[idx] == needle[0]
                && haystack.get_unchecked(idx..(idx + needle_len)) == needle
            {
                return Some(offset + idx + needle_len);
            }
        }
        if idx + needle_len >= haystack_len {
            return None;
        }
        // # Safety
        // `idx` < `haystack_len` and `idx` + `needle_len` < `haystack_len`.
        unsafe {
            checksum -= *haystack.get_unchecked(idx) as i64;
            checksum += *haystack.get_unchecked(idx + needle_len) as i64;
        }
        idx += 1;
    }
}

#[test]
fn test_generate_like_pattern() {
    let segments = vec![
        "databend".as_bytes().to_vec(),
        "cloud".as_bytes().to_vec(),
        "data".as_bytes().to_vec(),
        "warehouse".as_bytes().to_vec(),
    ];
    let test_cases = vec![
        (
            "databend",
            LikePattern::OrdinalStr("databend".as_bytes().into()),
        ),
        (
            "%databend",
            LikePattern::StartOfPercent("databend".as_bytes().into()),
        ),
        (
            "databend%",
            LikePattern::EndOfPercent("databend".as_bytes().into()),
        ),
        (
            "%databend%",
            LikePattern::SurroundByPercent(VolnitskyBase::new("databend".as_bytes(), 1)),
        ),
        (
            "databend%cloud%data%warehouse",
            LikePattern::SimplePattern((false, false, segments.clone())),
        ),
        (
            "%databend%cloud%data%warehouse",
            LikePattern::SimplePattern((true, false, segments.clone())),
        ),
        (
            "databend%cloud%data%warehouse%",
            LikePattern::SimplePattern((false, true, segments.clone())),
        ),
        (
            "%databend%cloud%data%warehouse%",
            LikePattern::SimplePattern((true, true, segments)),
        ),
        (
            "databend_cloud%data%warehouse",
            LikePattern::ComplexPattern("databend_cloud%data%warehouse".as_bytes().into()),
        ),
        (
            "databend\\%cloud%data%warehouse",
            LikePattern::ComplexPattern("databend\\%cloud%data%warehouse".as_bytes().into()),
        ),
        (
            "databend%cloud_data%warehouse",
            LikePattern::ComplexPattern("databend%cloud_data%warehouse".as_bytes().into()),
        ),
    ];
    for (pattern, pattern_type) in test_cases {
        assert_eq!(pattern_type, generate_like_pattern(pattern.as_bytes(), 1));
    }
}
