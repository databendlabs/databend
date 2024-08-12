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

/// Uniquely identifies a dictionary with a db_id and a dict_name
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Hash, Default)]
pub struct DictionaryIdentity {
    pub db_id: u64,
    pub dict_name: String,
}

impl DictionaryIdentity {
    pub fn new(db_id: u64, dict_name: impl ToString) -> Self {
        Self {
            db_id,
            dict_name: dict_name.to_string(),
        }
    }
}

mod kvapi_key_impl {

    use databend_common_meta_kvapi::kvapi;

    use super::DictionaryIdentity;
    use crate::schema::DatabaseId;
    use crate::schema::DictionaryId;

    impl kvapi::KeyCodec for DictionaryIdentity {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.db_id).push_str(&self.dict_name)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
        where Self: Sized {
            let db_id = parser.next_u64()?;
            let dict_name = parser.next_str()?;
            Ok(Self { db_id, dict_name })
        }
    }

    impl kvapi::Key for DictionaryIdentity {
        const PREFIX: &'static str = "__fd_dictionary";

        type ValueType = DictionaryId;

        fn parent(&self) -> Option<String> {
            Some(DatabaseId::new(self.db_id).to_string_key())
        }
    }
}
