// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_runtime::tokio;
use common_tracing::tracing;
use maplit::hashmap;
use pretty_assertions::assert_eq;
use tempfile::tempdir;
use tempfile::TempDir;

use crate::dfs::Dfs;
use crate::fs::FileSystem;
use crate::localfs::LocalFS;
use crate::meta_service::GetReq;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaServiceClient;
use crate::tests::assert_meta_connection;
use crate::tests::service::new_test_context;
use crate::tests::service::StoreTestContext;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_distributed_fs_single_node_read_all() -> anyhow::Result<()> {
    // - Brings a single node dfs online.
    // - Write several files.
    // - Test read_all()
    // - Test reading an absent file.

    let files = hashmap! {
        "foo" => "bar",
        "ping" => "pong",
        "who/is/hiding/deeply" => "jerry"
    };
    let dir = tempdir()?;
    let (tc, dfs) = bring_up_dfs(&dir, files.clone()).await?;
    let meta_addr = tc.config.meta_api_addr();

    let mut client = MetaServiceClient::connect(format!("http://{}", meta_addr)).await?;

    // test read every file

    for (key, content) in files.iter() {
        // check meta changes

        let req = tonic::Request::new(GetReq {
            key: key.to_string(),
        });
        let rst = client.get(req).await?.into_inner();

        // meanwhile the meta value is empty for every file
        assert_eq!("", rst.value);

        // read file and check

        let got = dfs.read_all(key).await?;
        assert_eq!(
            content.to_string().as_bytes(),
            got,
            "read content of file: {}",
            key
        );
    }

    // test reading absent file

    let got = dfs.read_all("absent".into()).await;
    assert!(got.is_err());
    assert_eq!(
        "dfs/meta: key not found: \"absent\"",
        got.unwrap_err().message()
    );

    // TODO: test a file presents in meta but not found on local fs.
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_distributed_fs_single_node_list() -> anyhow::Result<()> {
    // - Brings a single node dfs online.
    // - Write several files.
    // - Test list()

    let files = hashmap! {
        "foo" => "bar",
        "ping" => "pong",
        "ping_ping" => "pong",
        "who/is/hiding/deeply" => "jerry"
    };
    let dir = tempdir()?;
    let (_meta_addr, dfs) = bring_up_dfs(&dir, files.clone()).await?;

    let cases = vec![
        ("", vec!["foo", "ping", "ping_ping", "who/is/hiding/deeply"]),
        ("foo", vec!["foo"]),
        ("p", vec!["ping", "ping_ping"]),
        ("ping", vec!["ping", "ping_ping"]),
    ];

    for (prefix, want) in cases.iter() {
        let got = dfs.list(prefix).await?;
        assert_eq!(want.len(), got.files.len());
        for (i, w) in want.iter().enumerate() {
            assert_eq!(w.to_string(), got.files[i]);
        }
    }

    Ok(())
}

// Start an dfs.
// And feed files into dfs.
async fn bring_up_dfs(
    root: &TempDir,
    files: HashMap<&str, &str>,
) -> anyhow::Result<(StoreTestContext, Dfs)> {
    let root = root.path().to_str().unwrap().to_string();
    let fs = LocalFS::try_create(root)?;

    let mut tc = new_test_context();
    let meta_addr = tc.config.meta_api_addr();

    let mn = MetaNode::boot(0, &tc.config).await?;
    tc.meta_nodes.push(mn.clone());

    assert_meta_connection(&meta_addr).await?;

    let dfs = Dfs::create(fs, mn);
    for (key, content) in files.iter() {
        dfs.add((*key).into(), (*content).as_bytes()).await?;
        tracing::debug!("dfs added file: {} {:?}", *key, *content);
    }

    Ok((tc, dfs))
}
