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

use std::mem::size_of_val;

use byteorder::BigEndian;
use byteorder::ByteOrder;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sled::IVec;
use state_machine_api::ExpireKey;

use crate::SledBytesError;

/// Serialize/deserialize(ser/de) to/from sled values.
pub trait SledSerde: Serialize + DeserializeOwned {
    /// (ser)ialize a value to `sled::IVec`.
    fn ser(&self) -> Result<IVec, SledBytesError> {
        let x = serde_json::to_vec(self)?;
        Ok(x.into())
    }

    /// (de)serialize a value from `sled::IVec`.
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized;
}

/// Serialize/deserialize(ser/de) to/from sled values and keeps order after serializing.
///
/// E.g. serde_json does not preserve the order of u64:
/// 9 -> [57], 10 -> [49, 48]
/// While BigEndian encoding preserve the order.
///
/// A type that is used as a sled db key should be serialized with order preserved, such as log index.
pub trait SledOrderedSerde: Serialize + DeserializeOwned {
    /// (ser)ialize a value to `sled::IVec`.
    fn ser(&self) -> Result<IVec, SledBytesError>;

    /// (de)serialize a value from `sled::IVec`.
    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, SledBytesError>
    where Self: Sized;
}

/// NodeId, LogIndex and Term need to be serialized with order preserved, for listing items.
impl SledOrderedSerde for u64 {
    fn ser(&self) -> Result<IVec, SledBytesError> {
        let size = size_of_val(self);
        let mut buf = vec![0; size];

        BigEndian::write_u64(&mut buf, *self);
        Ok(buf.into())
    }

    /// (de)serialize a value from `sled::IVec`.
    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, SledBytesError>
    where Self: Sized {
        let res = BigEndian::read_u64(v.as_ref());
        Ok(res)
    }
}

/// For LogId to be able to stored in sled::Tree as a key.
impl SledOrderedSerde for String {
    fn ser(&self) -> Result<IVec, SledBytesError> {
        Ok(IVec::from(self.as_str()))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, SledBytesError>
    where Self: Sized {
        Ok(String::from_utf8(v.as_ref().to_vec())?)
    }
}

impl SledOrderedSerde for ExpireKey {
    fn ser(&self) -> Result<IVec, SledBytesError> {
        let size = size_of_val(self);
        let mut buf = vec![0; size];

        BigEndian::write_u64(&mut buf, self.time_ms);
        BigEndian::write_u64(&mut buf[size_of_val(&self.time_ms)..], self.seq);
        Ok(buf.into())
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, SledBytesError>
    where Self: Sized {
        let b = v.as_ref();

        let time_ms = BigEndian::read_u64(b);
        let seq = BigEndian::read_u64(&b[size_of_val(&time_ms)..]);

        Ok(Self { time_ms, seq })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_expire_key_sled_ordered_serde() -> anyhow::Result<()> {
        let k = ExpireKey::new(0x01020304, 0x04030201);
        let enc = <ExpireKey as SledOrderedSerde>::ser(&k)?;
        assert_eq!(
            vec![
                0u8, 0, 0, 0, 1, 2, 3, 4, //
                0u8, 0, 0, 0, 4, 3, 2, 1,
            ],
            enc.as_ref()
        );

        let got_dec = <ExpireKey as SledOrderedSerde>::de(enc)?;
        assert_eq!(k, got_dec);

        Ok(())
    }
}
