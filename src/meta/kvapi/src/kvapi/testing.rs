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

use crate::kvapi;
use crate::kvapi::Key;
use crate::kvapi::KeyCodec;
use crate::kvapi::KeyError;
use crate::kvapi::KeyParser;
use crate::kvapi::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FooKey {
    pub(crate) a: u64,
    pub(crate) b: String,
    pub(crate) c: u64,
}

impl KeyCodec for FooKey {
    fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
        b.push_u64(self.a).push_str(&self.b).push_u64(self.c)
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let a = parser.next_u64()?;
        let b = parser.next_str()?;
        let c = parser.next_u64()?;

        Ok(FooKey { a, b, c })
    }
}

#[derive(Debug)]
pub(crate) struct FooValue;

impl Key for FooKey {
    const PREFIX: &'static str = "pref";
    type ValueType = FooValue;

    fn parent(&self) -> Option<String> {
        None
    }
}

impl Value for FooValue {
    type KeyType = FooKey;

    fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
        []
    }
}
