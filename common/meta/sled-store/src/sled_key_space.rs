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

//! sled_key_space defines several key-value types to be used in sled::Tree, as an raft storage impl.

use std::fmt::Debug;
use std::fmt::Display;
use std::ops::Bound;
use std::ops::RangeBounds;

use common_exception::ErrorCode;
use sled::IVec;

use crate::SledOrderedSerde;
use crate::SledSerde;

/// Defines a key space in sled::Tree that has its own key value type.
/// And a prefix that is used to distinguish keys from different spaces in a SledTree.
pub trait SledKeySpace {
    /// Prefix is a unique u8 that is prepended before the serialized key, to identify a namespace in sled::Tree.
    const PREFIX: u8;

    /// The for human name of this key-value type.
    const NAME: &'static str;

    /// Type for key.
    type K: SledOrderedSerde + Display + Debug;

    /// Type for value.
    type V: SledSerde + Debug;

    fn serialize_key(k: &Self::K) -> Result<sled::IVec, ErrorCode> {
        let b = <Self::K as SledOrderedSerde>::ser(k)?;
        let x = b.as_ref();

        let mut buf = Vec::with_capacity(1 + x.len());
        buf.push(Self::PREFIX);
        buf.extend_from_slice(x);

        Ok(buf.into())
    }

    fn deserialize_key<T: AsRef<[u8]>>(iv: T) -> Result<Self::K, ErrorCode> {
        let b = iv.as_ref();
        if b[0] != Self::PREFIX {
            return Err(ErrorCode::MetaStoreDamaged("invalid prefix"));
        }
        <Self::K as SledOrderedSerde>::de(&b[1..])
    }

    fn serialize_value(v: &Self::V) -> Result<sled::IVec, ErrorCode> {
        v.ser()
    }

    fn deserialize_value<T: AsRef<[u8]>>(iv: T) -> Result<Self::V, ErrorCode> {
        Self::V::de(iv)
    }

    /// Convert range of user key to range of sled::IVec for query.
    fn serialize_range<R>(range: &R) -> Result<(Bound<IVec>, Bound<IVec>), ErrorCode>
    where R: RangeBounds<Self::K> {
        let s = range.start_bound();
        let e = range.end_bound();

        let s = Self::serialize_bound(s, "left")?;
        let e = Self::serialize_bound(e, "right")?;

        Ok((s, e))
    }

    /// Convert user key range bound to sled::IVec bound.
    /// A u8 prefix is prepended to the bound value and an open bound is converted to a namespaced bound.
    /// E.g., use the [PREFIX] as the left side closed bound,
    /// and use the [PREFIX+1] as the right side open bound.
    fn serialize_bound(v: Bound<&Self::K>, dir: &str) -> Result<Bound<sled::IVec>, ErrorCode> {
        let res = match v {
            Bound::Included(v) => Bound::Included(Self::serialize_key(v)?),
            Bound::Excluded(v) => Bound::Excluded(Self::serialize_key(v)?),
            Bound::Unbounded => {
                if dir == "left" {
                    Bound::Included(IVec::from(&[Self::PREFIX]))
                } else {
                    Bound::Excluded(IVec::from(&[Self::PREFIX + 1]))
                }
            }
        };
        Ok(res)
    }
}
