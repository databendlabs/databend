// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_exception::Result;
use common_runtime::tokio;

use crate::DNSResolver;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_resolver_github() -> Result<()> {
    let addrs = DNSResolver::instance()?.resolve("github.com").await?;
    assert_ne!(addrs.len(), 0);

    Ok(())
}
