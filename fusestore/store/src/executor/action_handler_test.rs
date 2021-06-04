// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow_flight::FlightData;
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
    let dir = tempdir()?;
    let root = dir.path();

    let fs = LocalFS::try_create(root.to_str().unwrap().to_string())?;

    let meta_addr = rand_local_addr();
    let mn = MetaNode::boot(0, meta_addr.clone()).await?;

    let dfs = Dfs::create(fs, mn);
    dfs.add("foo".into(), "bar".as_bytes()).await?;

    let hdlr = ActionHandler::create(Arc::new(dfs));
    {
        // pull file
        let (tx, mut rx): (
            Sender<Result<FlightData, tonic::Status>>,
            Receiver<Result<FlightData, tonic::Status>>,
        ) = tokio::sync::mpsc::channel(2);

        hdlr.do_pull_file("foo".into(), tx).await?;
        let rst = rx.recv().await;
        match rst {
            Some(r) => {
                //
                let r = r?;
                let body = r.data_body;
                assert_eq!("bar", std::str::from_utf8(&body)?);
            }
            None => {
                panic!("should not be None");
            }
        }
    }
    Ok(())
}
