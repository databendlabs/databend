// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

#![allow(warnings, unused)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_exception::Result;
use common_infallible::RwLock;
use common_runtime::tokio;
use common_runtime::tokio::runtime::Builder;
use common_runtime::tokio::select;
use common_runtime::tokio::sync::mpsc::channel;
use common_runtime::tokio::sync::mpsc::Receiver;
use common_runtime::tokio::sync::mpsc::Sender;
use common_store_api::DatabaseMeta;
use common_store_api::MetaApi;

use crate::catalog::datasource_meta::MetaId;
use crate::catalog::datasource_meta::MetaVersion;
use crate::catalog::datasource_meta::TableMeta;
use crate::datasources::remote::StoreClientProvider;
use crate::datasources::Database;
use crate::datasources::RemoteFactory;
use crate::datasources::Table;

type DatabaseMetaSnapshot = Arc<Vec<Arc<dyn Database>>>;

pub struct CachedRemoteMetaStoreClient {
    sync_period: Duration,
    remote_factory: StoreClientProvider,

    databases: RwLock<Option<DatabaseMetaSnapshot>>,
    fetch_req: Receiver<Sender<Arc<Vec<Arc<dyn Database>>>>>,
    fetch: Sender<Sender<Arc<Vec<Arc<dyn Database>>>>>,

    shutdown_receiver: Receiver<()>,
    shutdown_sender: Sender<()>,
}

struct Syncer {
    sync_period: Duration,
    remote_factory: StoreClientProvider,
    databases: RwLock<Option<DatabaseMetaSnapshot>>,
    //fetch: Arc<std::sync::mpsc::Receiver<std::sync::mpsc::SyncSender<DatabaseMetaSnapshot>>>,
}

impl Syncer {
    fn kickoff(&self) {
        use std::sync::mpsc;

        use crossbeam::channel;
        let (s, r) = std::sync::mpsc::channel::<mpsc::Sender<DatabaseMetaSnapshot>>();
        let provider = self.remote_factory.clone();
        let handle = std::thread::Builder::new()
            .name("remote-metastore-syncer".to_string())
            .spawn(move || {
                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                let mut databases: Option<DatabaseMetaSnapshot> = None;
                loop {
                    let s = r.recv();
                    s.unwrap().send(Arc::new(vec![]));
                    let r: common_exception::Result<DatabaseMetaSnapshot> = rt.block_on(async {
                        let mut client = provider.try_get_client().await?;
                        let databases = client.get_databases(None).await?;
                        Ok(Arc::new(from_database_meta(databases)))
                    });
                }
            });
        let (ts, tr) = mpsc::channel();
        s.send(ts);
        let _ = tr.recv();
    }
}

impl CachedRemoteMetaStoreClient {
    pub fn new() -> CachedRemoteMetaStoreClient {
        todo!()
    }

    pub fn create(
        sync_period: Duration,
        remote_factory: StoreClientProvider,
    ) -> CachedRemoteMetaStoreClient {
        todo!()
    }

    async fn worker(&mut self) {
        while let Some(tx) = self.fetch_req.recv().await {
            let db = self.databases.read();
            match &*db {
                Some(dbs) => {
                    // ignore errors here
                    let _ = tx.send(dbs.clone()).await;
                }
                None => {
                    let res = self.fetch().await.unwrap();
                    // send-error ignored
                    let _ = tx.send(res).await;
                }
            }
        }
    }

    async fn syncer(&mut self) {
        loop {
            select! {
                _ = self.shutdown_receiver.recv() => {
                    break;
                },
                _ = tokio::time::sleep(self.sync_period) =>  {
                    let mut dbs = self.databases.write();
                    let new_snapshot = self.fetch().await.unwrap();
                    *dbs = Some(new_snapshot.clone())
                },
            };
        }
    }

    async fn fetch(&self) -> Result<Arc<Vec<Arc<dyn Database>>>> {
        let mut client = self.remote_factory.try_get_client().await?;
        let db_meta = client.get_databases(None).await?;
        let dbs = from_database_meta(db_meta);
        Ok(Arc::new(dbs))
    }

    pub fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        todo!()
    }

    pub fn get_databases(&self) -> Result<Arc<Vec<Arc<dyn Database>>>> {
        let dbs = self.databases.read();
        match &*dbs {
            Some(v) => Ok(v.clone()),
            None => {
                todo!()
            }
        }
    }

    pub fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>> {
        todo!()
    }

    pub fn get_table_by_id(
        &self,
        _tbl_id: MetaId,
        _tbl_version: MetaVersion,
    ) -> Result<Arc<dyn Table>> {
        todo!()
    }
}

fn from_database_meta(db_meta: DatabaseMeta) -> Vec<Arc<dyn Database>> {
    todo!()
}
