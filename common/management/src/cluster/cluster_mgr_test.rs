// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_runtime::tokio;
use pretty_assertions::assert_eq;

use crate::cluster::address::Address;
use crate::ClusterMeta;
use crate::ClusterMgr;

#[tokio::test]
async fn test_cluster_mgr() -> Result<()> {
    let meta1 = ClusterMeta {
        name: "n1".to_string(),
        priority: 0,
        address: Address::create("192.168.0.1:9091")?,
        local: false,
        sequence: 0,
    };
    let meta2 = ClusterMeta {
        name: "n2".to_string(),
        priority: 0,
        address: Address::create("192.168.0.2:9091")?,
        local: false,
        sequence: 0,
    };
    let namespace = "namespace-1".to_string();

    let mut cluster_mgr = ClusterMgr::create_with_memory_backend();
    cluster_mgr.register(namespace.clone(), &meta1).await?;
    cluster_mgr.register(namespace.clone(), &meta2).await?;
    cluster_mgr.register(namespace.clone(), &meta1).await?;
    cluster_mgr.register(namespace.clone(), &meta2).await?;

    let actual = cluster_mgr.metas(namespace).await?;
    let expect = vec![meta1.clone(), meta2.clone()];
    assert_eq!(actual, expect);

    Ok(())
}
