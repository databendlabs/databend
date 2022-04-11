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

pub type ShareID = u64;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShareOutboundKey {
    pub consumer: String,
    pub share_id: ShareID,
}

impl SledOrderedSerde for ShareOutboundKey {
    fn ser(&self) -> Result<IVec, MetaStorageError> {
        let mut buf = BytesMut::new();
        buf.write_uvarint(self.share_id)
            .map_error_to_meta_storage_error(MetaStorageError::BytesError, || "write_uvarint")?;
        buf.write_string(&self.consumer)
            .map_error_to_meta_storage_error(MetaStorageError::BytesError, || "write_string")?;
        Ok(IVec::from(buf.to_vec()))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, MetaStorageError>
    where Self: Sized {
        let mut buf_read = Cursor::new(v);
        let share_key = buf_read
            .read_uvarint()
            .map_error_to_meta_storage_error(MetaStorageError::BytesError, || "read_uvarint")?;
        let consumer = buf_read
            .read_string()
            .map_error_to_meta_storage_error(MetaStorageError::BytesError, || "read_string")?;
        Ok(ShareOutboundKey {
            consumer,
            share_id: share_key,
        })
    }
}

impl fmt::Display for ShareOutboundKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ShareOutboundKey{}-{}", self.share_id, self.consumer)
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ShareOutboundValue {
    share_id: ShareID,
}

impl fmt::Display for ShareOutboundValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.share_id)
    }
}
