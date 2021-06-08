// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use pretty_assertions::assert_eq;
use tempfile::tempdir;

use crate::dfs::Dfs;
use crate::fs::IFileSystem;
use crate::localfs::LocalFS;
use crate::meta_service::GetReq;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaServiceClient;
use crate::tests::rand_local_addr;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_distributed_fs_single_node() -> anyhow::Result<()> {
    // - Brings a single node dfs online.
    // - Write a file.
    // - Test read_all()
    // - Test reading an absent file.

    let dir = tempdir()?;
    let root = dir.path();

    let fs = LocalFS::try_create(root.to_str().unwrap().to_string())?;

    let meta_addr = rand_local_addr();

    let rst = MetaNode::boot(0, meta_addr.clone()).await;
    assert!(rst.is_ok());
    let mn = rst.unwrap();

    let dfs = Dfs::create(fs, mn);
    {
        let rst = dfs.add("foo".into(), "bar".as_bytes()).await;
        rst.unwrap();

        // check meta changes

        let mut client = MetaServiceClient::connect(format!("http://{}", meta_addr)).await?;
        let req = tonic::Request::new(GetReq { key: "foo".into() });
        let rst = client.get(req).await?.into_inner();
        assert_eq!("", rst.value);

        // read file and check

        let got = dfs.read_all("foo".into()).await?;
        assert_eq!("bar".to_string().as_bytes(), got);

        let got = dfs.read_all("absent".into()).await;
        assert!(got.is_err());
        assert_eq!(
            "dfs/meta: key not found: \"absent\"",
            got.unwrap_err().message()
        );

        // TODO: test a file presents in meta but not found on local fs.
    }
    Ok(())
}
