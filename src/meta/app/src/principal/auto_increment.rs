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

use std::fmt::Display;

use databend_common_expression::ColumnId;
use databend_meta_types::MetaId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct AutoIncrementKey {
    pub table_id: MetaId,
    pub column_id: ColumnId,
}

impl AutoIncrementKey {
    pub fn new(table_id: MetaId, column_id: ColumnId) -> Self {
        Self {
            table_id,
            column_id,
        }
    }
}

impl Display for AutoIncrementKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AutoIncrement({}/{})", self.table_id, self.column_id)
    }
}

mod kvapi_key_impl {
    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::KeyBuilder;
    use databend_meta_kvapi::kvapi::KeyError;
    use databend_meta_kvapi::kvapi::KeyParser;

    use crate::principal::auto_increment::AutoIncrementKey;

    impl kvapi::KeyCodec for AutoIncrementKey {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.table_id).push_u64(self.column_id as u64)
        }

        fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
        where Self: Sized {
            let table_id = parser.next_u64()?;
            let column_id = parser.next_u64()?;

            Ok(Self {
                table_id,
                column_id: column_id as u32,
            })
        }
    }
}
