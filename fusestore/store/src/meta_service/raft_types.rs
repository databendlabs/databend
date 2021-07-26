// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::LogId;
pub use async_raft::NodeId;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use common_exception::ErrorCode;
use sled::IVec;

use crate::meta_service::sled_serde::SledOrderedSerde;
use crate::meta_service::SledSerde;

pub type LogIndex = u64;
pub type Term = u64;

/// NodeId, LogIndex and Term need to be serialized with order preserved, for listing items.
impl SledOrderedSerde for u64 {
    fn order_preserved_serialize(&self, buf: &mut [u8]) {
        BigEndian::write_u64(buf, *self);
    }

    fn order_preserved_deserialize(buf: &[u8]) -> Self {
        BigEndian::read_u64(buf)
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

    fn order_preserved_serialize(&self, _buf: &mut [u8]) {
        todo!()
    }

    fn order_preserved_deserialize(_buf: &[u8]) -> Self {
        todo!()
    }
}

/// For LogId to be able to stored in sled::Tree as a value.
impl SledSerde for LogId {}
