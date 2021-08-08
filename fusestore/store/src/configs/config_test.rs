// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::configs::Config;

#[test]
fn test_fuse_commit_version() -> anyhow::Result<()> {
    let v = &crate::configs::config::FUSE_COMMIT_VERSION;
    assert!(v.len() > 0);
    Ok(())
}

#[test]
fn test_tls_rpc_enabled() -> anyhow::Result<()> {
    let mut conf = Config::empty();
    assert_eq!(false, conf.tls_rpc_server_enabled());
    conf.rpc_tls_server_key = "test".to_owned();
    assert_eq!(false, conf.tls_rpc_server_enabled());
    conf.rpc_tls_server_cert = "test".to_owned();
    assert_eq!(true, conf.tls_rpc_server_enabled());
    Ok(())
}
