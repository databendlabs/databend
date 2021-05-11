// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use tonic::transport::channel::Channel;

use crate::fs::IFileSystem;
use crate::fs::ListResult;
use crate::localfs::LocalFS;
use crate::meta_service::ClientRequest;
use crate::meta_service::Cmd;
use crate::meta_service::MetaServiceClient;

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
    pub meta_service_addr: String
}

impl Dfs {
    pub fn create(local_fs: LocalFS, meta_service_addr: String) -> Dfs {
        Dfs {
            local_fs,
            meta_service_addr
        }
    }
}

impl Dfs {
    pub async fn make_client(&self) -> anyhow::Result<MetaServiceClient<Channel>> {
        let client =
            MetaServiceClient::connect(format!("http://{}", self.meta_service_addr)).await?;
        Ok(client)
    }
}

#[async_trait]
impl IFileSystem for Dfs {
    async fn add<'a>(&'a self, path: String, data: &[u8]) -> anyhow::Result<()> {
        // add the file to local fs

        self.local_fs.add(path.clone(), data).await?;

        // update meta, other store nodes will be informed about this change and then pull the data to complete replication.

        let mut client = self.make_client().await?;
        let req = ClientRequest {
            txid: None,
            cmd: Cmd::AddFile {
                key: path,
                value: "".into()
            }
        };
        client.write(req).await?;
        Ok(())
    }

    async fn read_all<'a>(&'a self, path: String) -> anyhow::Result<Vec<u8>> {
        // TODO read local cached meta first
        self.local_fs.read_all(path).await
    }

    async fn list<'a>(&'a self, path: String) -> anyhow::Result<ListResult> {
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
