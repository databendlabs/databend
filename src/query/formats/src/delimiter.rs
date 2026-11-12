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

#[derive(Debug, Default)]
pub enum RecordDelimiter {
    #[default]
    Crlf,
    Any(u8),
}

impl RecordDelimiter {
    pub fn end(&self) -> u8 {
        match self {
            RecordDelimiter::Crlf => b'\n',
            RecordDelimiter::Any(b) => *b,
        }
    }
}

impl FromStr for RecordDelimiter {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<Self> {
        Self::try_from(s.as_bytes())
    }
}

impl TryFrom<&str> for RecordDelimiter {
    type Error = ErrorCode;
    fn try_from(s: &str) -> Result<Self> {
        s.parse()
    }
}

impl TryFrom<&[u8]> for RecordDelimiter {
    type Error = ErrorCode;
    fn try_from(s: &[u8]) -> Result<Self> {
        match s.len() {
            1 => {
                if s.eq(b"\n") {
                    Ok(RecordDelimiter::Crlf)
                } else {
                    Ok(RecordDelimiter::Any(s[0]))
                }
            }
            2 if s.eq(b"\r\n") => Ok(RecordDelimiter::Crlf),
            _ => Err(ErrorCode::InvalidArgument(format!(
                "bad RecordDelimiter: '{:?}'",
                s
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_exception::ErrorCode;

    use super::RecordDelimiter;

    #[test]
    fn parse_record_delimiter() {
        for input in ["\n", "\r\n"] {
            assert!(matches!(input.parse(), Ok(RecordDelimiter::Crlf)));
            assert!(matches!(
                RecordDelimiter::try_from(input),
                Ok(RecordDelimiter::Crlf)
            ));
        }
        for input in ["|", "\r", "\0"] {
            let expected = input.as_bytes()[0];
            assert!(matches!(
                input.parse(),
                Ok(RecordDelimiter::Any(byte)) if byte == expected
            ));
            assert!(matches!(
                RecordDelimiter::try_from(input),
                Ok(RecordDelimiter::Any(byte)) if byte == expected
            ));
        }
    }

    #[test]
    fn reject_invalid_record_delimiter() {
        for input in ["", "||", "abc", "é"] {
            let parsed = input.parse::<RecordDelimiter>().unwrap_err();
            let converted = RecordDelimiter::try_from(input).unwrap_err();
            assert_eq!(parsed.code(), ErrorCode::INVALID_ARGUMENT);
            assert_eq!(parsed.to_string(), converted.to_string());
        }
    }
}
