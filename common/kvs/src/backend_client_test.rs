// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_runtime::tokio;

use crate::BackendClient;

#[tokio::test]
async fn test_backend_client() -> Result<()> {
    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
    struct Val {
        item: String,
    }

    let client = BackendClient::create("local://xx".to_string());
    let k1 = "namespace/k1".to_string();
    let v1 = Val {
        item: "v1".to_string(),
    };
    let k2 = "namespace/k2".to_string();
    let v2 = Val {
        item: "v2".to_string(),
    };

    // Put test.
    client.put(k1.clone(), v1.clone()).await?;
    client.put(k2.clone(), v2.clone()).await?;

    // Get test.
    let r: Option<Val> = client.get(k1.clone()).await?;
    assert_eq!(r.unwrap(), v1.clone());

    // Prefix test.
    let prefix = "namespace".to_string();
    let actual = client.get_from_prefix(prefix).await?;
    let expect = vec![
        ("namespace/k1".to_string(), v1.clone()),
        ("namespace/k2".to_string(), v2.clone()),
    ];
    assert_eq!(actual, expect);

    // Remove test.
    client.remove(k2.clone()).await?;
    let r: Option<Val> = client.get(k2.clone()).await?;
    assert_eq!(None, r);

    Ok(())
}
