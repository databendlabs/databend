// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub use async_raft::NodeId;
use byteorder::BigEndian;
use byteorder::ByteOrder;

use crate::meta_service::sled_serde::SledOrderedSerde;

/// NodeId need to be serialized with order preserved, for listing.
/// This is required by `SledSerde` when saving node id into sled db.
impl SledOrderedSerde for NodeId {
    fn order_preserved_serialize(&self, buf: &mut [u8]) {
        BigEndian::write_u64(buf, *self);
    }

    fn order_preserved_deserialize(buf: &[u8]) -> Self {
        BigEndian::read_u64(buf)
    }
}
