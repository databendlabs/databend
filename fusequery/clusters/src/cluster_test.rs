// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_cluster() -> crate::error::ClusterResult<()> {
    use pretty_assertions::assert_eq;

    use crate::*;

    let cluster = Cluster::empty();

    let node1 = Node {
        name: "node1".to_string(),
        cpus: 4,
        address: "127.0.0.1:9001".to_string(),
        local: false,
    };
    cluster.add_node(&node1)?;

    let node2 = Node {
        name: "node2".to_string(),
        cpus: 8,
        address: "127.0.0.1:9002".to_string(),
        local: false,
    };
    cluster.add_node(&node2)?;

    let cluster_clone = cluster.clone();
    cluster_clone.remove_node("node1".to_string())?;
    cluster_clone.remove_node("node2".to_string())?;

    let nodes = cluster.get_nodes()?;

    assert_eq!(0, nodes.len());
    Ok(())
}
