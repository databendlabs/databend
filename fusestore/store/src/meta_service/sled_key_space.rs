// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

//! sledkv defines several key-value types to be used in sled::Tree, as an raft storage impl.

use std::fmt::Debug;
use std::fmt::Display;
use std::ops::Bound;
use std::ops::RangeBounds;

use async_raft::raft::Entry;
use common_exception::ErrorCode;
use sled::IVec;

use crate::meta_service::LogEntry;
use crate::meta_service::LogIndex;
use crate::meta_service::Node;
use crate::meta_service::NodeId;
use crate::meta_service::SledOrderedSerde;
use crate::meta_service::SledSerde;

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
    type V: SledSerde;

    fn serialize_key(k: &Self::K) -> Result<sled::IVec, ErrorCode> {
        let b = k.ser()?;
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
        Self::K::de(&b[1..])
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

/// Types for raft log in SledTree
pub struct Logs {}
impl SledKeySpace for Logs {
    const PREFIX: u8 = 1;
    const NAME: &'static str = "log";
    type K = LogIndex;
    type V = Entry<LogEntry>;
}

/// Types for Node in SledTree
pub struct Nodes {}
impl SledKeySpace for Nodes {
    const PREFIX: u8 = 2;
    const NAME: &'static str = "node";
    type K = NodeId;
    type V = Node;
}
