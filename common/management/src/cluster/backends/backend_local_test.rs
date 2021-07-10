// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_flights::Address;
use common_runtime::tokio;
use pretty_assertions::assert_eq;

use crate::cluster::backends::LocalBackend;
use crate::cluster::ClusterBackend;
use crate::cluster::ClusterExecutor;

#[tokio::test]
async fn test_backend_memory() -> Result<()> {
    let executor1 = ClusterExecutor {
        name: "n1".to_string(),
        priority: 0,
        address: Address::create("192.168.0.1:9091")?,
        local: false,
        sequence: 0,
    };
    let executor2 = ClusterExecutor {
        name: "n2".to_string(),
        priority: 0,
        address: Address::create("192.168.0.2:9091")?,
        local: false,
        sequence: 0,
    };
    let namespace = "namespace-1".to_string();
    let backend = LocalBackend::create();

    // Put.
    {
        backend.put(namespace.clone(), &executor1).await?;
        backend.put(namespace.clone(), &executor2).await?;
        backend.put(namespace.clone(), &executor1).await?;
        let actual = backend.get(namespace.clone()).await?;
        let expect = vec![executor2.clone(), executor1.clone()];
        assert_eq!(actual, expect);
    }

    // Remove.
    {
        backend.remove(namespace.clone(), &executor2).await?;
        let actual = backend.get(namespace).await?;
        let expect = vec![executor1.clone()];
        assert_eq!(actual, expect);
    }

    Ok(())
}
