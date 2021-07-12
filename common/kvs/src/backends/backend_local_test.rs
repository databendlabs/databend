// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_runtime::tokio;

use crate::backends::Backend;
use crate::backends::LocalBackend;

#[tokio::test]
async fn test_local_backend() -> Result<()> {
    let backend = LocalBackend::create("".to_string());
    let k1 = "namespace/k1".to_string();
    let k2 = "namespace/k2".to_string();
    let v = "v1".to_string();

    // Put test.
    backend.put(k1.clone(), v.clone()).await?;
    // Insert k1 twice.
    backend.put(k1.clone(), v.clone()).await?;
    backend.put(k2.clone(), v.clone()).await?;

    // Get test.
    let r = backend.get(k1.clone()).await?;
    assert_eq!(r.unwrap(), "v1".to_string());

    // Prefix test.
    let prefix = "namespace".to_string();
    let actual = backend.get_from_prefix(prefix).await?;
    let expect = vec![
        ("namespace/k1".to_string(), "v1".to_string()),
        ("namespace/k2".to_string(), "v1".to_string()),
    ];
    assert_eq!(actual, expect);

    // Remove test.
    backend.remove(k2.clone()).await?;
    let r = backend.get(k2.clone()).await?;
    assert_eq!(None, r);

    Ok(())
}
