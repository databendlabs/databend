// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub use async_raft::NodeId;
use byteorder::BigEndian;
use byteorder::ByteOrder;

use crate::meta_service::sled_serde::SledOrderedSerde;

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
