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

use std::fmt::Debug;
use std::string::FromUtf8Error;

use crate::kvapi;
use crate::kvapi::key_codec::KeyCodec;

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

    #[error("Expect {i}-th segment to be non-empty")]
    EmptySegment { i: usize },

    #[error("Expect {expect} segments, but: '{got}'")]
    WrongNumberOfSegments { expect: usize, got: String },

    #[error("Expect at least {expect} segments, but {actual} segments found")]
    AtleastSegments { expect: usize, actual: usize },

    #[error("Invalid id string: '{s}': {reason}")]
    InvalidId { s: String, reason: String },

    #[error("Unknown kvapi::Key prefix: '{prefix}'")]
    UnknownPrefix { prefix: String },
}

/// Convert structured key to a string key used by kvapi::KVApi and backwards
pub trait Key: KeyCodec + Debug
where Self: Sized
{
    const PREFIX: &'static str;

    type ValueType: kvapi::Value;

    /// Return the parent key of this key.
    ///
    /// For example, a table name's(`(database_id, table_name)`) parent is the database id.
    fn parent(&self) -> Option<String>;

    /// Encode structured key into a string.
    fn to_string_key(&self) -> String {
        let b = kvapi::KeyBuilder::new_prefixed(Self::PREFIX);
        self.encode_key(b).done()
    }

    /// Decode str into a structured key.
    fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
        let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;
        let k = Self::decode_key(&mut p)?;
        p.done()?;

        Ok(k)
    }
}

#[cfg(test)]
mod tests {

    use crate::kvapi::testing::FooKey;
    use crate::kvapi::DirName;
    use crate::kvapi::Key;

    #[test]
    fn test_with_key_space() {
        let k = FooKey {
            a: 1,
            b: "b".to_string(),
            c: 2,
        };

        let dir = DirName::new(k);
        assert_eq!("pref/1/b", dir.to_string_key());

        let k = FooKey {
            a: 1,
            b: "b".to_string(),
            c: 2,
        };

        assert_eq!("pref/1/b/2", k.to_string_key());
    }
}
