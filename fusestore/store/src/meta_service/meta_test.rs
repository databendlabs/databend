// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::meta_service::meta::Replication;
use crate::meta_service::Meta;
use crate::meta_service::Node;
use crate::meta_service::Slot;

#[test]
fn test_meta_assign_rand_nodes_to_slot() -> anyhow::Result<()> {
    // - Create a meta with 3 node 1,3,5.
    // - Assert that expected number of nodes are assigned to a slot.

    let mut meta = Meta {
        slots: vec![Slot::default(), Slot::default(), Slot::default()],
        nodes: maplit::hashmap! {
            1=> Node{..Default::default()},
            3=> Node{..Default::default()},
            5=> Node{..Default::default()},
        },
        replication: Replication::Mirror(3),
        ..Default::default()
    };

    // assign all node to slot 2
    meta.assign_rand_nodes_to_slot(2)?;
    assert_eq!(meta.slots[2].node_ids, vec![1, 3, 5]);

    // assign all node again to slot 2
    meta.assign_rand_nodes_to_slot(2)?;
    assert_eq!(meta.slots[2].node_ids, vec![1, 3, 5]);

    // assign 1 node again to slot 1
    meta.replication = Replication::Mirror(1);
    meta.assign_rand_nodes_to_slot(1)?;
    assert_eq!(1, meta.slots[1].node_ids.len());

    let id = meta.slots[1].node_ids[0];
    assert!(id == 1 || id == 3 || id == 5);

    Ok(())
}

#[test]
fn test_meta_init_slots() -> anyhow::Result<()> {
    // - Create a meta with 3 node 1,3,5.
    // - Initialize all slots.
    // - Assert slot states.

    let mut meta = Meta {
        slots: vec![Slot::default(), Slot::default(), Slot::default()],
        nodes: maplit::hashmap! {
            1=> Node{..Default::default()},
            3=> Node{..Default::default()},
            5=> Node{..Default::default()},
        },
        replication: Replication::Mirror(1),
        ..Default::default()
    };

    meta.init_slots()?;
    for slot in meta.slots.iter() {
        assert_eq!(1, slot.node_ids.len());

        let id = slot.node_ids[0];
        assert!(id == 1 || id == 3 || id == 5);
    }

    Ok(())
}

#[test]
fn test_meta_builder() -> anyhow::Result<()> {
    // - Assert default meta builder
    // - Assert customized meta builder

    let m = Meta::builder().build()?;
    assert_eq!(3, m.slots.len());
    let n = match m.replication {
        Replication::Mirror(x) => x,
    };
    assert_eq!(1, n);

    let m = Meta::builder().slots(5).mirror_replication(8).build()?;
    assert_eq!(5, m.slots.len());
    let n = match m.replication {
        Replication::Mirror(x) => x,
    };
    assert_eq!(8, n);
    Ok(())
}
