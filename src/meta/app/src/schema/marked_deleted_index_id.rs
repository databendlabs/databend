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

#[derive(Clone, Debug, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub struct MarkedDeletedIndexId {
    pub table_id: u64,
    pub index_id: u64,
}

impl MarkedDeletedIndexId {
    pub fn new(table_id: u64, index_id: u64) -> Self {
        MarkedDeletedIndexId { table_id, index_id }
    }
}

mod kvapi_key_impl {
    use databend_meta_kvapi::kvapi;

    use super::MarkedDeletedIndexId;

    impl kvapi::KeyCodec for MarkedDeletedIndexId {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.table_id).push_u64(self.index_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let table_id = parser.next_u64()?;
            let index_id = parser.next_u64()?;
            Ok(Self { table_id, index_id })
        }
    }
}
