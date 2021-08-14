// Copyright 2020 Datafuse Labs.
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

use std::ops::Bound;
use std::ops::RangeBounds;

use common_exception::ErrorCode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::IVec;

/// Serialize/deserialize(ser/de) to/from sled values.
pub trait SledSerde: Serialize + DeserializeOwned + Sized {
    /// (ser)ialize a value to `sled::IVec`.
    fn ser(&self) -> Result<IVec, ErrorCode> {
        let x = serde_json::to_vec(self)?;
        Ok(x.into())
    }

    /// (de)serialize a value from `sled::IVec`.
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, ErrorCode>
    where Self: Sized {
        let s = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

/// Serialize/deserialize(ser/de) to/from sled values and keeps order after serializing.
///
/// E.g. serde_json does not preserve the order of u64:
/// 9 -> [57], 10 -> [49, 48]
/// While BigEndian encoding preserve the order.
///
/// A type that is used as a sled db key should be serialized with order preserved, such as log index.
pub trait SledOrderedSerde: Serialize + DeserializeOwned + Sized {
    /// (ser)ialize a value to `sled::IVec`.
    fn ser(&self) -> Result<IVec, ErrorCode>;

    /// (de)serialize a value from `sled::IVec`.
    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, ErrorCode>
    where Self: Sized;
}

/// Serialize/deserialize(ser/de) to/from range to sled IVec range.
/// The type must impl SledOrderedSerde so that after serialization the order is preserved.
pub trait SledRangeSerde<SD, V, R>
where
    SD: SledOrderedSerde,
    V: RangeBounds<SD>,
    R: RangeBounds<IVec>,
{
    /// (ser)ialize a range to range of `sled::IVec`.
    fn ser(&self) -> Result<R, ErrorCode>;

    // TODO(xp): do we need this?
    // /// (de)serialize a value from `sled::IVec`.
    // fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, ErrorCode>
    //     where Self: Sized;
}

/// Impl ser/de for range of value that can be ser/de to `sled::IVec`
impl<SD, V> SledRangeSerde<SD, V, (Bound<IVec>, Bound<IVec>)> for V
where
    SD: SledOrderedSerde,
    V: RangeBounds<SD>,
{
    fn ser(&self) -> Result<(Bound<IVec>, Bound<IVec>), ErrorCode> {
        let s = self.start_bound();
        let e = self.end_bound();

        let s = bound_ser(s)?;
        let e = bound_ser(e)?;

        Ok((s, e))
    }
}

fn bound_ser<SD: SledOrderedSerde>(v: Bound<&SD>) -> Result<Bound<sled::IVec>, ErrorCode> {
    let res = match v {
        Bound::Included(v) => Bound::Included(v.ser()?),
        Bound::Excluded(v) => Bound::Excluded(v.ser()?),
        Bound::Unbounded => Bound::Unbounded,
    };
    Ok(res)
}
