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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatSettings {
    pub record_delimiter: Vec<u8>,
    pub field_delimiter: Vec<u8>,
    pub empty_as_default: bool,
    pub timezone: Tz,
    pub true_bytes: Vec<u8>,
    pub false_bytes: Vec<u8>,
    pub null_bytes: Vec<u8>,
    pub nan_bytes: Vec<u8>,
    pub inf_bytes: Vec<u8>,
    pub quote_char: u8,
    pub escape: Option<u8>,

    pub csv_null_bytes: Vec<u8>,
    pub tsv_null_bytes: Vec<u8>,
    pub json_quote_denormals: bool,
    pub json_escape_forward_slashes: bool,

    pub ident_case_sensitive: bool,
}

impl Default for FormatSettings {
    fn default() -> Self {
        Self {
            record_delimiter: vec![b'\n'],
            field_delimiter: vec![b','],
            empty_as_default: true,
            timezone: "UTC".parse::<Tz>().unwrap(),
            true_bytes: vec![b'1'],
            false_bytes: vec![b'0'],
            null_bytes: vec![b'N', b'U', b'L', b'L'],
            nan_bytes: vec![b'N', b'a', b'N'],
            inf_bytes: vec![b'i', b'n', b'f'],
            csv_null_bytes: vec![b'\\', b'N'],
            tsv_null_bytes: vec![b'\\', b'N'],
            json_quote_denormals: false,
            json_escape_forward_slashes: true,
            ident_case_sensitive: false,
            quote_char: b'\'',
            escape: Some(b'\\'),
        }
    }
}
