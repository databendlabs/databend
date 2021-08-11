// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::meta_service::sled_serde::SledOrderedSerde;
use crate::meta_service::NodeId;

#[test]
fn test_serde_node_id() -> anyhow::Result<()> {
    let ids: Vec<NodeId> = vec![9, 10, 11];

    let want: Vec<sled::IVec> = vec![
        sled::IVec::from(vec![0, 0, 0, 0, 0, 0, 0, 9]),
        sled::IVec::from(vec![0, 0, 0, 0, 0, 0, 0, 10]),
        sled::IVec::from(vec![0, 0, 0, 0, 0, 0, 0, 11]),
    ];
    let got = ids.iter().map(|id| id.ser().unwrap()).collect::<Vec<_>>();
    assert_eq!(want, got);
    Ok(())
}
