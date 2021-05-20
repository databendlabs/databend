// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cluster() -> Result<()> {
    use pretty_assertions::assert_eq;

    use crate::clusters::cluster::Cluster;
    use crate::clusters::node::Node;

    let cluster = Cluster::empty();

    cluster.add_node(&String::from("node1"), 5, &String::from("127.0.0.1:9001")).await?;
    cluster.add_node(&String::from("node2"), 5, &String::from("127.0.0.1:9002")).await?;

    let cluster_clone = cluster.clone();
    cluster_clone.remove_node("node1".to_string())?;
    cluster_clone.remove_node("node2".to_string())?;

    let nodes = cluster.get_nodes()?;

    assert_eq!(0, nodes.len());
    Ok(())
}
