// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-Lise-Identifier: Apache-2.0.

use pretty_assertions::assert_eq;
use tempfile::tempdir;

use crate::dfs::Dfs;
use crate::fs::IFileSystem;
use crate::localfs::LocalFS;
use crate::meta_service::GetReq;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaServiceClient;
use crate::meta_service::MetaServiceImpl;
use crate::meta_service::MetaServiceServer;
use crate::tests::rand_local_addr;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_distributed_fs() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let root = dir.path();

    let fs = LocalFS::try_create(root.to_str().unwrap().to_string())?;

    let meta_addr = rand_local_addr();

    let mn = MetaNode::new(0).await;

    let meta_srv_impl = MetaServiceImpl::create(mn).await;
    let meta_srv = MetaServiceServer::new(meta_srv_impl);
    serve_grpc!(meta_addr, meta_srv);

    let dfs = Dfs::create(fs, meta_addr.clone());
    {
        let rst = dfs.add("foo".into(), "bar".as_bytes()).await;
        rst.unwrap();
        // check meta changes

        let mut client = MetaServiceClient::connect(format!("http://{}", meta_addr)).await?;
        let req = tonic::Request::new(GetReq { key: "foo".into() });
        let rst = client.get(req).await?.into_inner();
        assert_eq!("", rst.value);

        // TODO read file data and check result.
    }
    Ok(())
}
