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

use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

/// Serialization for kinds that have a disk tier. Implemented by every type that already
/// converts to and from bytes for the existing hybrid caches.
pub trait DiskCodec: Sized {
    fn encode(&self) -> Result<Vec<u8>>;

    fn decode(bytes: Bytes) -> Result<Self>;
}

impl<T> DiskCodec for T
where
    Vec<u8>: for<'a> TryFrom<&'a T, Error = ErrorCode>,
    T: TryFrom<Bytes, Error = ErrorCode>,
{
    fn encode(&self) -> Result<Vec<u8>> {
        Vec::<u8>::try_from(self)
    }

    fn decode(bytes: Bytes) -> Result<Self> {
        T::try_from(bytes)
    }
}
