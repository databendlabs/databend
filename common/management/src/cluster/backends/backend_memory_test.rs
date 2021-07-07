// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_runtime::tokio;
use pretty_assertions::assert_eq;

use crate::cluster::address::Address;
use crate::cluster::backend_api::BackendApi;
use crate::cluster::backends::MemoryBackend;
use crate::ClusterMeta;

#[tokio::test]
async fn test_backend_memory() -> Result<()> {
    let backend_store = MemoryBackend::create();
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

    backend_store.put(namespace.clone(), &meta1).await?;
    backend_store.put(namespace.clone(), &meta2).await?;
    backend_store.put(namespace.clone(), &meta1).await?;
    let actual = backend_store.get(namespace).await?;
    let expect = vec![meta2.clone(), meta1.clone()];
    assert_eq!(actual, expect);

    Ok(())
}
