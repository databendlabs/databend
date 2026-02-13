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
            "utf-8" | "utf8" => Ok(BinaryDisplayFormat::Utf8),
            "utf-8-lossy" | "utf8-lossy" => Ok(BinaryDisplayFormat::Utf8Lossy),
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
    pub fn format<'a>(&self, value: &'a [u8]) -> Result<Cow<'a, str>, ErrorCode> {
        match self {
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

    pub fn encode<'a>(&self, value: &'a [u8]) -> Result<Cow<'a, [u8]>, ErrorCode> {
        match self {
            BinaryDisplayFormat::Hex => Ok(Cow::Owned(hex::encode_upper(value).into_bytes())),
            BinaryDisplayFormat::Base64 => Ok(Cow::Owned(
                general_purpose::STANDARD.encode(value).into_bytes(),
            )),
            BinaryDisplayFormat::Utf8 => match std::str::from_utf8(value) {
                Ok(_) => Ok(Cow::Borrowed(value)),
                Err(err) => Err(ErrorCode::InvalidUtf8String(format!(
                    "Invalid UTF-8 sequence while formatting binary column: {err}. Consider \
setting binary_output_format to 'UTF-8-LOSSY'."
                ))),
            },
            BinaryDisplayFormat::Utf8Lossy => Ok(Cow::Owned(
                String::from_utf8_lossy(value).into_owned().into_bytes(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputFormatSettings {
    pub jiff_timezone: TimeZone,
    pub geometry_format: GeometryDataType,
    pub binary_format: BinaryDisplayFormat,

    pub is_rounding_mode: bool,
    pub disable_variant_check: bool,
}

// only used for tests
impl Default for InputFormatSettings {
    fn default() -> Self {
        Self {
            jiff_timezone: TimeZone::UTC,
            geometry_format: GeometryDataType::default(),
            binary_format: BinaryDisplayFormat::Hex,
            is_rounding_mode: true,
            disable_variant_check: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputFormatSettings {
    pub jiff_timezone: TimeZone,
    pub geometry_format: GeometryDataType,
    pub binary_format: BinaryDisplayFormat,

    // used only in http handler response
    pub format_null_as_str: bool,
}

impl Default for OutputFormatSettings {
    fn default() -> Self {
        Self {
            jiff_timezone: TimeZone::UTC,
            geometry_format: GeometryDataType::default(),
            binary_format: BinaryDisplayFormat::Hex,
            format_null_as_str: false,
        }
    }
}

impl OutputFormatSettings {
    pub fn format_binary<'a>(&self, value: &'a [u8]) -> Result<Cow<'a, str>, ErrorCode> {
        self.binary_format.format(value)
    }
}
