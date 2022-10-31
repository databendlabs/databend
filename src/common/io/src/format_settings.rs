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

use chrono_tz::Tz;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::consts::*;

// fixed the format in struct/array,
// when it`s repr as a string in csv/tsv/json/...
// should be compatible with the format used SQL
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NestedFormatSettings {
    pub true_bytes: Vec<u8>,
    pub false_bytes: Vec<u8>,
    pub null_bytes: Vec<u8>,
    pub nan_bytes: Vec<u8>,
    pub inf_bytes: Vec<u8>,
    pub quote_char: u8,
}

impl Default for NestedFormatSettings {
    fn default() -> Self {
        NestedFormatSettings {
            true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
            false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
            null_bytes: NULL_BYTES_UPPER.as_bytes().to_vec(),
            nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
            inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
            quote_char: b'\'',
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatSettings {
    // both
    pub timezone: Tz,

    // inner
    pub nested: NestedFormatSettings,

    // outer
    pub true_bytes: Vec<u8>,
    pub false_bytes: Vec<u8>,
    pub null_bytes: Vec<u8>,
    pub nan_bytes: Vec<u8>,
    pub inf_bytes: Vec<u8>,
    pub quote_char: u8,
    pub escape: Option<u8>,

    pub record_delimiter: Vec<u8>,
    pub field_delimiter: Vec<u8>,
    pub empty_as_default: bool,

    pub json_quote_denormals: bool,
    pub json_escape_forward_slashes: bool,
    pub ident_case_sensitive: bool,
}

impl FormatSettings {
    pub fn parse_escape(option: &str, default: Option<u8>) -> Option<u8> {
        if option.is_empty() {
            default
        } else {
            Some(option.as_bytes()[0])
        }
    }

    pub fn parse_quote(option: &str) -> Result<u8> {
        if option.len() != 1 {
            Err(ErrorCode::InvalidArgument(
                "quote_char can only contain one char",
            ))
        } else {
            Ok(option.as_bytes()[0])
        }
    }

    fn tsv_default() -> Self {
        Self {
            timezone: "UTC".parse::<Tz>().unwrap(),
            nested: Default::default(),

            true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
            false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
            nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
            inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
            null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
            quote_char: b'\'',
            escape: Some(b'\\'),

            record_delimiter: vec![b'\n'],
            field_delimiter: vec![b'\t'],

            // not used
            empty_as_default: true,
            json_quote_denormals: false,
            json_escape_forward_slashes: true,
            ident_case_sensitive: false,
        }
    }
}

// only used for tests
impl Default for FormatSettings {
    fn default() -> Self {
        FormatSettings::tsv_default()
    }
}
