// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_cluster() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::clusters::*;

    let cluster = Cluster::empty();

    let node1 = Node {
        id: "node1".to_string(),
        cpus: 4,
        address: "127.0.0.1:9001".to_string(),
    };
    cluster.add_node(&node1)?;

    let node2 = Node {
        id: "node2".to_string(),
        cpus: 8,
        address: "127.0.0.1:9002".to_string(),
    };
    cluster.add_node(&node2)?;

    cluster.remove_node("node1".to_string())?;

    let nodes = cluster.get_nodes()?[0].clone();

    assert_eq!(node2, nodes);
    Ok(())
}
