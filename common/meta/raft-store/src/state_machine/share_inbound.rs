// Copyright 2022 Datafuse Labs.
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
use common_io::prelude::BinaryRead;
use common_io::prelude::BinaryWriteBuf;
use common_meta_sled_store::sled::IVec;
use common_meta_sled_store::SledOrderedSerde;
use common_meta_types::MetaStorageError;
use common_meta_types::ToMetaStorageError;
use serde::Deserialize;
use serde::Serialize;

pub type ShareKey = u64;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShareInboundKey {
    pub consumer: String,
    pub share_key: ShareKey,
}

impl SledOrderedSerde for ShareInboundKey {
    fn ser(&self) -> Result<IVec, MetaStorageError> {
        let mut buf = BytesMut::new();
        buf.write_string(&self.consumer)
            .map_error_to_meta_storage_error(MetaStorageError::BytesError, || "write_string")?;
        buf.write_uvarint(self.ShareKey)
            .map_error_to_meta_storage_error(MetaStorageError::BytesError, || "write_uvarint")?;
        Ok(IVec::from(buf.to_vec()))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, MetaStorageError>
    where Self: Sized {
        let mut buf_read = Cursor::new(v);
        let consumer = buf_read
            .read_string()
            .map_error_to_meta_storage_error(MetaStorageError::BytesError, || "read_string")?;
        let share_key = buf_read
            .read_uvarint()
            .map_error_to_meta_storage_error(MetaStorageError::BytesError, || "read_uvarint")?;
        Ok(ShareInboundKey {
            consumer,
            share_key,
        })
    }
}

impl fmt::Display for ShareInboundKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ShareInboundKey{}-{}", self.consumer, self.share_key)
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ShareInboundValue {
    share_id: ShareKey,
    database_name: Option<String>,
}

impl fmt::Display for ShareInboundValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(db={:?})", self.share_id, self.database_name)
    }
}
