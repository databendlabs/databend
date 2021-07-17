// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::mem::size_of_val;
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
    fn ser(&self) -> Result<IVec, ErrorCode> {
        let size = size_of_val(self);
        let mut buf = vec![0; size];

        self.order_preserved_serialize(&mut buf);
        Ok(buf.into())
    }

    /// (de)serialize a value from `sled::IVec`.
    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, ErrorCode>
    where Self: Sized {
        let v = Self::order_preserved_deserialize(v.as_ref());
        Ok(v)
    }

    /// serialize keep the same ordering as original value
    fn order_preserved_serialize(&self, buf: &mut [u8]);

    /// deserialize from the order preserved bytes
    fn order_preserved_deserialize(buf: &[u8]) -> Self;
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
