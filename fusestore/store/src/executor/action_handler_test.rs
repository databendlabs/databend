// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use common_arrow::arrow_flight::FlightData;
use common_tracing::tracing;
use maplit::hashmap;
use pretty_assertions::assert_eq;
use tempfile::tempdir;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

use crate::dfs::Dfs;
use crate::executor::ActionHandler;
use crate::fs::IFileSystem;
use crate::localfs::LocalFS;
use crate::meta_service::MetaNode;
use crate::tests::rand_local_addr;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_action_handler_do_pull_file() -> anyhow::Result<()> {
    common_tracing::init_default_tracing();

    let dir = tempdir()?;
    let root = dir.path();

    let hdlr = bring_up_dfs_action_handler(root, hashmap! {
        "foo" => "bar",
    })
    .await?;

    {
        // pull file
        let (tx, mut rx): (
            Sender<Result<FlightData, tonic::Status>>,
            Receiver<Result<FlightData, tonic::Status>>,
        ) = tokio::sync::mpsc::channel(2);

        hdlr.do_pull_file("foo".into(), tx).await?;
        let rst = rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("should not be None"))?;
        let rst = rst?;
        let body = rst.data_body;
        assert_eq!("bar", std::str::from_utf8(&body)?);
    }
    Ok(())
}

// Start an ActionHandler backed with a dfs.
// And feed files into dfs.
async fn bring_up_dfs_action_handler(
    root: &Path,
    files: HashMap<&str, &str>,
) -> anyhow::Result<ActionHandler> {
    let fs = LocalFS::try_create(root.to_str().unwrap().to_string())?;

    let meta_addr = rand_local_addr();
    let mn = MetaNode::boot(0, meta_addr.clone()).await?;

    let dfs = Dfs::create(fs, mn);
    for (key, content) in files.iter() {
        dfs.add((*key).into(), (*content).as_bytes()).await?;
        tracing::debug!("added file: {} {:?}", *key, *content);
    }

    let ah = ActionHandler::create(Arc::new(dfs));

    Ok(ah)
}
