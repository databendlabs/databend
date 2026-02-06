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

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub struct MarkedDeletedTableIndexId {
    pub table_id: u64,
    pub index_name: String,
    pub index_version: String,
}

impl MarkedDeletedTableIndexId {
    pub fn new(table_id: u64, index_name: String, index_version: String) -> Self {
        MarkedDeletedTableIndexId {
            table_id,
            index_name,
            index_version,
        }
    }
}

mod kvapi_key_impl {
    use databend_meta_kvapi::kvapi;

    use super::MarkedDeletedTableIndexId;

    impl kvapi::KeyCodec for MarkedDeletedTableIndexId {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.table_id)
                .push_str(&self.index_name)
                .push_str(&self.index_version)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let table_id = parser.next_u64()?;
            let index_name = parser.next_str()?;
            let index_version = parser.next_str()?;
            Ok(Self {
                table_id,
                index_name,
                index_version,
            })
        }
    }
}
