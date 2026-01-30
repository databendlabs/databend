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

//! Process key prefix.

use crate::kvapi::KeyError;

/// Convert a `prefix` to a left-close-right-open range (start, end) that includes exactly all possible string that starts with `prefix`.
///
/// It only supports ASCII characters.
pub fn prefix_to_range(prefix: &str) -> Result<(String, String), KeyError> {
    Ok((prefix.to_string(), ascii_next(prefix)?))
}

// TODO: the algo is not likely correct.
/// Return a ascii string that bigger than all the string starts with the input `s`.
///
/// It only supports ASCII char.
///
/// "a" -> "b"
/// "1" -> "2"
/// [96,97,127] -> [96,98,127]
/// [127] -> [127, 127]
/// [127, 127, 127, 127] -> [127, 127, 127, 127, 127]
fn ascii_next(s: &str) -> Result<String, KeyError> {
    for c in s.chars() {
        if !c.is_ascii() {
            return Err(KeyError::AsciiError {
                non_ascii: s.to_string(),
            });
        }
    }

    let mut l = s.len();
    while l > 0 {
        l -= 1;
        if let Some(c) = s.chars().nth(l) {
            if c == 127 as char {
                continue;
            }
            return Ok(replace_nth_char(s, l, (c as u8 + 1) as char));
        }
    }

    Ok(format!("{}{}", s, 127 as char))
}

/// Replace idx-th char as new char
///
/// If `idx` is out of len(s) range, then no replacement is performed.
/// replace_nth_char("a13", 1, '2') -> 'a23'
/// replace_nth_char("a13", 10, '2') -> 'a13'
pub fn replace_nth_char(s: &str, idx: usize, new_char: char) -> String {
    s.chars()
        .enumerate()
        .map(|(i, c)| if i == idx { new_char } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::kvapi::prefix::ascii_next;
    use crate::kvapi::prefix::replace_nth_char;
    use crate::kvapi::prefix_to_range;

    #[test]
    fn test_ascii_next() -> anyhow::Result<()> {
        assert_eq!("b".to_string(), ascii_next("a")?);
        assert_eq!("2".to_string(), ascii_next("1")?);
        assert_eq!(
            "__fd_table_by_ie".to_string(),
            ascii_next("__fd_table_by_id")?
        );
        {
            let str = 127 as char;
            let s = str.to_string();
            let ret = ascii_next(&s)?;
            for byte in ret.as_bytes() {
                assert_eq!(*byte, 127_u8);
            }
        }
        {
            let s = format!("ab{}", 127 as char);
            let ret = ascii_next(&s)?;
            assert_eq!(ret, format!("ac{}", 127 as char));
        }
        {
            let s = "我".to_string();
            let ret = ascii_next(&s);
            match ret {
                Err(e) => {
                    assert_eq!("Non-ascii char are not supported: '我'", e.to_string(),);
                }
                Ok(_) => panic!("MUST return error "),
            }
        }

        Ok(())
    }

    #[test]
    fn test_prefix_to_range() -> anyhow::Result<()> {
        assert_eq!(("aa".to_string(), "ab".to_string()), prefix_to_range("aa")?);
        assert_eq!(("a1".to_string(), "a2".to_string()), prefix_to_range("a1")?);
        {
            let str = 127 as char;
            let s = str.to_string();
            let (_, end) = prefix_to_range(&s)?;
            for byte in end.as_bytes() {
                assert_eq!(*byte, 127_u8);
            }
        }
        {
            let ret = prefix_to_range("我");
            match ret {
                Err(e) => {
                    assert_eq!("Non-ascii char are not supported: '我'", e.to_string(),);
                }
                Ok(_) => panic!("MUST return error "),
            }
        }
        Ok(())
    }

    #[test]
    fn test_replace_nth_char() {
        assert_eq!("a23".to_string(), replace_nth_char("a13", 1, '2'));
        assert_eq!("a13".to_string(), replace_nth_char("a13", 10, '2'));
    }
}
