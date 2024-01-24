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

//! Defines kvapi::KVApi key behaviors.

use std::convert::Infallible;
use std::fmt::Debug;
use std::string::FromUtf8Error;

use crate::kvapi;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),

    #[error("Non-ascii char are not supported: '{non_ascii}'")]
    AsciiError { non_ascii: String },

    #[error("Expect {i}-th segment to be '{expect}', but: '{got}'")]
    InvalidSegment {
        i: usize,
        expect: String,
        got: String,
    },

    #[error("Expect {expect} segments, but: '{got}'")]
    WrongNumberOfSegments { expect: usize, got: String },

    #[error("Expect at least {expect} segments, but {actual} segments found")]
    AtleastSegments { expect: usize, actual: usize },

    #[error("Invalid id string: '{s}': {reason}")]
    InvalidId { s: String, reason: String },
}

/// Convert structured key to a string key used by kvapi::KVApi and backwards
pub trait Key: Debug
where Self: Sized
{
    const PREFIX: &'static str;

    type ValueType;

    /// Encode structured key into a string.
    fn to_string_key(&self) -> String;

    /// Decode str into a structured key.
    fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError>;
}

impl kvapi::Key for String {
    const PREFIX: &'static str = "";

    /// For a non structured key, the value type can never be used.
    type ValueType = Infallible;

    fn to_string_key(&self) -> String {
        self.clone()
    }

    fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
        Ok(s.to_string())
    }
}
