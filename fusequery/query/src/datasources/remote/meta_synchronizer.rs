// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_exception::Result;
use common_infallible::RwLock;
use common_runtime::tokio;
use common_runtime::tokio::select;
use common_runtime::tokio::sync::mpsc::Receiver;
use common_runtime::tokio::sync::mpsc::Sender;
use common_store_api::DatabaseMeta;
use common_store_api::MetaApi;

use crate::datasources::database_catalog::MetaId;
use crate::datasources::database_catalog::MetaVersion;
use crate::datasources::database_catalog::TableMeta;
use crate::datasources::Database;
use crate::datasources::RemoteFactory;
use crate::datasources::Table;

pub struct Synchronizer {
    databases: RwLock<Option<Arc<Vec<Arc<dyn Database>>>>>,
    remote_factory: RemoteFactory,
    fetch_req: Receiver<Sender<Arc<Vec<Arc<dyn Database>>>>>,
    shutdown_receiver: Receiver<()>,
    sync_period: Duration,
}

impl Synchronizer {
    pub fn new() -> Synchronizer {
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
        let mut client = self
            .remote_factory
            .store_client_provider()
            .try_get_client()
            .await?;
        let db_meta = client.get_databases(None).await?;
        let dbs = from_database_meta(db_meta);
        Ok(Arc::new(dbs))
    }

    //    fn get_snapshot() -> Result<Arc<>>

    pub fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        todo!()
    }

    pub fn get_databases(&self) -> Result<Arc<Vec<Arc<dyn Database>>>> {
        let dbs = self.databases.read();
        match &*dbs {
            Some(v) => Ok(v.clone()),
            None => todo!(),
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
