// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_exception::Result;
use common_runtime::tokio;
use pretty_assertions::assert_eq;

use crate::clusters::cluster::Cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_add_node_with_local() -> Result<()> {
    let cluster = Cluster::empty();

    cluster
        .add_node(&String::from("node1"), 5, &String::from("127.0.0.1:9001"))
        .await?;
    assert_eq!(
        cluster.get_node_by_name(String::from("node1"))?.local,
        false
    );
    cluster
        .add_node(&String::from("node2"), 5, &String::from("127.0.0.1:9090"))
        .await?;
    assert_eq!(cluster.get_node_by_name(String::from("node2"))?.local, true);
    cluster
        .add_node(&String::from("node3"), 5, &String::from("localhost:9090"))
        .await?;
    assert_eq!(cluster.get_node_by_name(String::from("node3"))?.local, true);
    cluster
        .add_node(&String::from("node4"), 5, &String::from("github.com:9001"))
        .await?;
    assert_eq!(
        cluster.get_node_by_name(String::from("node4"))?.local,
        false
    );
    cluster
        .add_node(&String::from("node5"), 5, &String::from("github.com:9090"))
        .await?;
    assert_eq!(
        cluster.get_node_by_name(String::from("node5"))?.local,
        false
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_add_node_with_clone() -> Result<()> {
    let cluster = Cluster::empty();

    cluster
        .add_node(&String::from("node1"), 5, &String::from("127.0.0.1:9001"))
        .await?;
    cluster
        .add_node(&String::from("node2"), 5, &String::from("127.0.0.1:9002"))
        .await?;
    assert_eq!(cluster.get_nodes()?.len(), 2);

    let cluster_clone = cluster.clone();
    assert_eq!(cluster_clone.get_nodes()?.len(), 2);

    cluster_clone.remove_node("node1".to_string())?;
    assert_eq!(cluster.get_nodes()?.len(), 1);
    assert_eq!(cluster_clone.get_nodes()?.len(), 1);

    cluster.remove_node("node2".to_string())?;
    assert_eq!(cluster.get_nodes()?.len(), 0);
    assert_eq!(cluster_clone.get_nodes()?.len(), 0);

    Ok(())
}
