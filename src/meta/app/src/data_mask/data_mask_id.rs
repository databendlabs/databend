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

use databend_common_meta_kvapi::kvapi::KeyBuilder;
use databend_common_meta_kvapi::kvapi::KeyCodec;
use databend_common_meta_kvapi::kvapi::KeyError;
use databend_common_meta_kvapi::kvapi::KeyParser;
use derive_more::Deref;
use derive_more::DerefMut;
use derive_more::From;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deref, DerefMut, From)]
pub struct DataMaskId {
    id: u64,
}

impl DataMaskId {
    pub fn new(id: u64) -> Self {
        DataMaskId { id }
    }
}

impl KeyCodec for DataMaskId {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        b.push_u64(self.id)
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let v = parser.next_u64()?;
        Ok(Self::new(v))
    }
}
