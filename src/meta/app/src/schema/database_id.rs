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

use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use derive_more::Deref;
use derive_more::DerefMut;

#[derive(Clone, Debug, Copy, Default, Eq, PartialEq, PartialOrd, Ord, Deref, DerefMut)]
pub struct DatabaseId {
    pub db_id: u64,
}

impl DatabaseId {
    pub fn new(db_id: u64) -> Self {
        DatabaseId { db_id }
    }
}

impl From<u64> for DatabaseId {
    fn from(db_id: u64) -> Self {
        DatabaseId { db_id }
    }
}

impl Display for DatabaseId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.db_id)
    }
}

mod kvapi_key_impl {
    use databend_meta_kvapi::kvapi;

    use crate::schema::DatabaseId;
    use crate::schema::DatabaseMeta;

    impl kvapi::KeyCodec for DatabaseId {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.db_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let db_id = parser.next_u64()?;
            Ok(Self { db_id })
        }
    }

    /// "__fd_database_by_id/<db_id>"
    impl kvapi::Key for DatabaseId {
        const PREFIX: &'static str = "__fd_database_by_id";

        type ValueType = DatabaseMeta;

        fn parent(&self) -> Option<String> {
            None
        }
    }

    impl kvapi::Value for DatabaseMeta {
        type KeyType = DatabaseId;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
