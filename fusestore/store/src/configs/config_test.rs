// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_fuse_commit_version() -> anyhow::Result<()> {
    let v = &crate::configs::config::FUSE_COMMIT_VERSION;
    assert!(v.len() > 0);
    Ok(())
}
