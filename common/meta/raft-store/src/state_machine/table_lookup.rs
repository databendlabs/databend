// Copyright 2021 Datafuse Labs.
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
use std::io::Cursor;

use bytes::BytesMut;
use common_exception::ErrorCode;
use common_io::prelude::BinaryRead;
use common_io::prelude::BinaryWriteBuf;
use common_meta_sled_store::sled::IVec;
use common_meta_sled_store::SledOrderedSerde;
use serde::Deserialize;
use serde::Serialize;

pub type DbKey = u64;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableLookupKey {
    pub database_id: DbKey,
    pub table_name: String,
}

impl SledOrderedSerde for TableLookupKey {
    fn ser(&self) -> Result<IVec, ErrorCode> {
        let mut buf = BytesMut::new();
        if buf.write_uvarint(self.database_id).is_ok() && buf.write_string(&self.table_name).is_ok()
        {
            return Ok(IVec::from(buf.to_vec()));
        }
        Err(ErrorCode::MetaStoreDamaged("invalid key IVec"))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, ErrorCode>
    where Self: Sized {
        let mut buf_read = Cursor::new(v);
        let database_id = buf_read.read_uvarint();
        if let Ok(database_id) = database_id {
            let table_name_result = buf_read.read_string();
            if let Ok(table_name) = table_name_result {
                return Ok(TableLookupKey {
                    database_id,
                    table_name,
                });
            }
        }
        Err(ErrorCode::MetaStoreDamaged("invalid key IVec"))
    }
}

impl fmt::Display for TableLookupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "TableLookupKey_{}-{}", self.database_id, self.table_name)
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct TableLookupValue(pub u64);

impl fmt::Display for TableLookupValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
