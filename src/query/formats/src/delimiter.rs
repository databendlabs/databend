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

impl TryFrom<&str> for RecordDelimiter {
    type Error = ErrorCode;
    fn try_from(s: &str) -> Result<Self> {
        Self::try_from(s.as_bytes())
    }
}

impl TryFrom<&[u8]> for RecordDelimiter {
    type Error = ErrorCode;
    fn try_from(s: &[u8]) -> Result<Self> {
        match s.len() {
            1 => Ok(RecordDelimiter::Any(s[0])),
            2 if s.eq(b"\r\n") => Ok(RecordDelimiter::Crlf),
            _ => Err(ErrorCode::InvalidArgument(format!(
                "bad RecordDelimiter: '{:?}'",
                s
            ))),
        }
    }
}
