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

use base64::Engine as _;
use base64::engine::general_purpose;
use chrono_tz::Tz;
use databend_common_exception::ErrorCode;
use jiff::tz::TimeZone;

use crate::GeometryDataType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BinaryDisplayFormat {
    #[default]
    Hex,
    Base64,
    Utf8,
    Utf8Lossy,
}

impl BinaryDisplayFormat {
    pub fn parse(s: &str) -> Result<Self, ErrorCode> {
        match s.to_ascii_lowercase().as_str() {
            "hex" => Ok(BinaryDisplayFormat::Hex),
            "base64" => Ok(BinaryDisplayFormat::Base64),
            "utf-8" => Ok(BinaryDisplayFormat::Utf8),
            "utf-8-lossy" => Ok(BinaryDisplayFormat::Utf8Lossy),
            other => Err(ErrorCode::InvalidArgument(format!(
                "Invalid binary format '{other}', valid values: HEX | BASE64 | UTF-8 | UTF-8-LOSSY"
            ))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            BinaryDisplayFormat::Hex => "HEX",
            BinaryDisplayFormat::Base64 => "BASE64",
            BinaryDisplayFormat::Utf8 => "UTF-8",
            BinaryDisplayFormat::Utf8Lossy => "UTF-8-LOSSY",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatSettings {
    pub timezone: Tz,
    pub jiff_timezone: TimeZone,
    pub geometry_format: GeometryDataType,
    pub binary_format: BinaryDisplayFormat,
    pub enable_dst_hour_fix: bool,
    pub format_null_as_str: bool,
}

// only used for tests
impl Default for FormatSettings {
    fn default() -> Self {
        Self {
            timezone: "UTC".parse::<Tz>().unwrap(),
            jiff_timezone: TimeZone::UTC,
            geometry_format: GeometryDataType::default(),
            binary_format: BinaryDisplayFormat::Hex,
            enable_dst_hour_fix: false,
            format_null_as_str: false,
        }
    }
}

impl FormatSettings {
    pub fn format_binary<'a>(&self, value: &'a [u8]) -> Result<Cow<'a, str>, ErrorCode> {
        match self.binary_format {
            BinaryDisplayFormat::Hex => Ok(Cow::Owned(hex::encode_upper(value))),
            BinaryDisplayFormat::Base64 => Ok(Cow::Owned(general_purpose::STANDARD.encode(value))),
            BinaryDisplayFormat::Utf8 => match std::str::from_utf8(value) {
                Ok(s) => Ok(Cow::Borrowed(s)),
                Err(err) => Err(ErrorCode::InvalidUtf8String(format!(
                    "Invalid UTF-8 sequence while formatting binary column: {err}. Consider \
setting binary_output_format to 'UTF-8-LOSSY'."
                ))),
            },
            BinaryDisplayFormat::Utf8Lossy => {
                Ok(Cow::Owned(String::from_utf8_lossy(value).into_owned()))
            }
        }
    }
}
