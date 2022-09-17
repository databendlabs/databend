// Copyright 2022 Datafuse Labs.
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

use core::fmt::Display;

/// List of possible errors
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Error {
    InvalidUtf8,
    InvalidEOF,

    InvalidJsonb,
    InvalidJsonbHeader,
    InvalidJsonbJEntry,

    InvalidValue,
    InvalidNullValue,
    InvalidFalseValue,
    InvalidTrueValue,
    InvalidNumberValue,
    InvalidStringValue,
    InvalidArrayValue,
    InvalidObjectValue,

    InvalidEscaped(u8),
    InvalidHex(u8),
    InvalidLoneLeadingSurrogateInHexEscape(u16),
    InvalidSurrogateInHexEscape(u16),
    UnexpectedEndOfHexEscape,
    UnexpectedTrailingCharacters,
}

impl Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<std::io::Error> for Error {
    fn from(_error: std::io::Error) -> Self {
        Error::InvalidUtf8
    }
}

impl From<decimal_rs::DecimalConvertError> for Error {
    fn from(_error: decimal_rs::DecimalConvertError) -> Self {
        Error::InvalidUtf8
    }
}
