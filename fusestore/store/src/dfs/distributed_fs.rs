// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;

use crate::fs::IFileSystem;
use crate::fs::ListResult;
use crate::localfs::LocalFS;
use crate::meta_service::ClientRequest;
use crate::meta_service::Cmd;
use crate::meta_service::MetaNode;

/// DFS is a distributed file system impl.
/// When a file is added, it stores it locally, commit the this action into distributed meta data(something like a raft group).
/// Then notifies client Ok.
/// The replication is done by 2 other nodes, by subscribing meta data changes, and pulling the file.
/// TODO: There is a chance the node receiving the upload fails before replication is done, which results in a data loss.
///       A synchronous quorum write is required to solve this.
pub struct Dfs {
    /// The local fs to store data copies.
    /// The distributed fs is a cluster of local-fs organized with a meta data service.
    pub local_fs: LocalFS,
    pub meta_node: Arc<MetaNode>,
}

impl Dfs {
    pub fn create(local_fs: LocalFS, meta_node: Arc<MetaNode>) -> Dfs {
        Dfs {
            local_fs,
            meta_node,
        }
    }
}

impl Dfs {}

#[async_trait]
impl IFileSystem for Dfs {
    async fn add(&self, path: String, data: &[u8]) -> anyhow::Result<()> {
        // add the file to local fs

        self.local_fs.add(path.clone(), data).await?;

        // update meta, other store nodes will be informed about this change and then pull the data to complete replication.

        let req = ClientRequest {
            txid: None,
            cmd: Cmd::AddFile {
                key: path,
                value: "".into(),
            },
        };
        let _resp = self.meta_node.write(req).await?;
        Ok(())
    }

    async fn read_all(&self, path: String) -> anyhow::Result<Vec<u8>> {
        // TODO read local cached meta first
        self.local_fs.read_all(path).await
    }

    async fn list(&self, path: String) -> anyhow::Result<ListResult> {
        let _key = path;

        // TODO read local meta cache to list

        // let meta = self.meta.lock().await;
        // let mut files = vec![];
        // for (k, _v) in meta.keys.range(key.clone()..) {
        //     if !k.starts_with(key.as_str()) {
        //         break;
        //     }
        //     files.push(k.to_string());
        // }

        // Ok(ListResult {
        //     dirs: vec![],
        //     files,
        // })

        todo!("dirs and files")
    }
}
