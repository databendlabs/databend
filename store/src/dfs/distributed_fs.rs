// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::ops::Bound::Included;
use std::ops::Bound::Unbounded;
use std::sync::Arc;

use async_trait::async_trait;
use common_exception::exception;
use common_exception::ErrorCode;
use common_tracing::tracing;

use crate::fs::FileSystem;
use crate::fs::ListResult;
use crate::localfs::LocalFS;
use crate::meta_service::Cmd;
use crate::meta_service::LogEntry;
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
impl FileSystem for Dfs {
    #[tracing::instrument(level = "debug", skip(self, data))]
    async fn add(&self, path: &str, data: &[u8]) -> anyhow::Result<()> {
        // add the file to local fs

        self.local_fs.add(path, data).await?;

        // update meta, other store nodes will be informed about this change and then pull the data to complete replication.

        let req = LogEntry {
            txid: None,
            cmd: Cmd::AddFile {
                key: path.to_string(),
                value: "".into(),
            },
        };
        let _resp = self.meta_node.write(req).await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_all(&self, key: &str) -> exception::Result<Vec<u8>> {
        // TODO read from remote if file is not in local fs
        // TODO(xp): week consistency, meta may not have been replicated to this node.

        // meanwhile, file meta is empty string
        let _file_meta = self.meta_node.get_file(key).await.ok_or_else(|| {
            ErrorCode::FileMetaNotFound(format!("dfs/meta: key not found: {:?}", key))
        })?;

        self.local_fs.read_all(key).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list(&self, prefix: &str) -> anyhow::Result<ListResult> {
        let sm = self.meta_node.sto.get_state_machine().await;

        let mut files: Vec<String> = Vec::new();
        for (k, _v) in sm.keys.range((Included(prefix.to_string()), Unbounded)) {
            if !k.starts_with(prefix) {
                break;
            }
            files.push(k.clone());
        }

        Ok(ListResult {
            dirs: vec![],
            files,
        })
    }
}
