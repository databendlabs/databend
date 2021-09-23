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

use std::mem::size_of_val;

use byteorder::BigEndian;
use byteorder::ByteOrder;
use common_exception::ErrorCode;
use common_metatypes::KVValue;
use common_metatypes::LogId;
use common_metatypes::SeqValue;
use sled::IVec;

use crate::sled_store::SeqNum;
use crate::sled_store::SledOrderedSerde;
use crate::sled_store::SledSerde;

impl SledSerde for SeqNum {}
/// NodeId, LogIndex and Term need to be serialized with order preserved, for listing items.
impl SledOrderedSerde for u64 {
    fn ser(&self) -> Result<IVec, ErrorCode> {
        let size = size_of_val(self);
        let mut buf = vec![0; size];

        BigEndian::write_u64(&mut buf, *self);
        Ok(buf.into())
    }

    /// (de)serialize a value from `sled::IVec`.
    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, ErrorCode>
    where Self: Sized {
        let res = BigEndian::read_u64(v.as_ref());
        Ok(res)
    }
}

/// For LogId to be able to stored in sled::Tree as a key.
impl SledOrderedSerde for String {
    fn ser(&self) -> Result<IVec, ErrorCode> {
        Ok(IVec::from(self.as_str()))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, ErrorCode>
    where Self: Sized {
        Ok(String::from_utf8(v.as_ref().to_vec())?)
    }
}

impl SledSerde for String {
    fn ser(&self) -> Result<IVec, ErrorCode> {
        Ok(IVec::from(self.as_str()))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, ErrorCode>
    where Self: Sized {
        Ok(String::from_utf8(v.as_ref().to_vec())?)
    }
}

impl SledSerde for SeqValue<KVValue> {}

/// For LogId to be able to stored in sled::Tree as a value.
impl SledSerde for LogId {}
