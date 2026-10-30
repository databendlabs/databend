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

use std::sync::Arc;

use bytes::Bytes;
use databend_common_exception::Result;

use crate::items::*;

/// Serialization for kinds that have a disk tier.
pub trait DiskCodec: Sized {
    fn encode(&self) -> Result<Vec<u8>>;

    fn decode(bytes: Bytes) -> Result<Self>;
}

/// Types that already convert to and from bytes for the pre-refactor hybrid caches.
macro_rules! disk_codec_via_try_from {
    ($($ty:ty),* $(,)?) => {$(
        impl DiskCodec for $ty {
            fn encode(&self) -> Result<Vec<u8>> {
                Vec::<u8>::try_from(self)
            }

            fn decode(bytes: Bytes) -> Result<Self> {
                Self::try_from(bytes)
            }
        }
    )*};
}

disk_codec_via_try_from!(
    BloomIndexMeta,
    FilterImpl,
    IndexMeta,
    IndexFile,
    VectorIndexMeta,
    VirtualColumnFileMeta,
    InvertedIndexMeta,
    InvertedIndexLookupBytes,
    InvertedIndexPayloadBytes,
    ColumnData,
);

/// For kinds whose entries are handed out as `Arc<T>`.
impl<T: DiskCodec> DiskCodec for Arc<T> {
    fn encode(&self) -> Result<Vec<u8>> {
        T::encode(self)
    }

    fn decode(bytes: Bytes) -> Result<Self> {
        T::decode(bytes).map(Arc::new)
    }
}
