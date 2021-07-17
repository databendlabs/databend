// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::ops::Bound;

use crate::meta_service::sled_serde::SledOrderedSerde;
use crate::meta_service::sled_serde::SledRangeSerde;
use crate::meta_service::NodeId;

#[test]
fn test_node_id_serde() -> anyhow::Result<()> {
    let id9: NodeId = 9;
    let id10: NodeId = 10;

    let got9 = id9.ser()?;
    let got10 = id10.ser()?;
    assert!(got9 < got10);

    let got9 = NodeId::de(got9)?;
    let got10 = NodeId::de(got10)?;
    assert_eq!(id9, got9);
    assert_eq!(id10, got10);

    Ok(())
}

#[test]
fn test_node_id_range_serde() -> anyhow::Result<()> {
    let a: NodeId = 8;
    let b: NodeId = 11;
    let got = (a..b).ser()?;
    let want = (
        Bound::Included(sled::IVec::from(vec![0, 0, 0, 0, 0, 0, 0, 8])),
        Bound::Excluded(sled::IVec::from(vec![0, 0, 0, 0, 0, 0, 0, 11])),
    );
    assert_eq!(want, got);
    Ok(())
}
