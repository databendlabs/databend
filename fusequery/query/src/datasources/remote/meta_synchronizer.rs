// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

#![allow(warnings, unused)]

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::TrySendError;
use std::sync::Arc;
use std::time::Duration;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Condvar;
use common_infallible::Mutex;
use common_infallible::RwLock;
use common_runtime::tokio;
use common_runtime::tokio::runtime::Builder;
use common_runtime::tokio::select;
use common_store_api::DatabaseMeta;
use common_store_api::MetaApi;
use futures::TryFutureExt;

use crate::catalog::datasource_meta::MetaId;
use crate::catalog::datasource_meta::MetaVersion;
use crate::catalog::datasource_meta::TableMeta;
use crate::datasources::remote::StoreClientProvider;
use crate::datasources::Database;
use crate::datasources::RemoteFactory;
use crate::datasources::Table;

type DatabaseMetaSnapshot = Arc<Vec<Arc<dyn Database>>>;

pub struct CachedRemoteMetaStoreClient {
    databases: Mutex<Option<DatabaseMetaSnapshot>>,
    cond: Condvar,
    fetch_hinter: mpsc::SyncSender<()>,
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

    pub fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        todo!()
    }

    pub fn get_db_meta(&self) -> Result<Arc<Vec<Arc<dyn Database>>>> {
        todo!()
        //let mut dbs = self.databases.lock();
        //let opt = &*dbs;
        //// Although we are using parking_lot, technically we may use `if` here,
        //// but if we switching sync primitives, this may be a rather subtle bug to diagnose
        //while dbs.is_none() {
        //    // tells syncer that we need a adhoc update
        //    let r = self.fetch_hinter.try_send(());
        //    match r {
        //        Err(TrySendError::Disconnected(_)) => {
        //            log::error!("huston, syncer is down");
        //            // TODO add an specific error code
        //            return Err(ErrorCode::UnknownException(""));
        //        }
        //        _ => {
        //            // since it is just a hint, we can safely ignore other cases
        //        }
        //    }
        //    self.cond.wait(&mut dbs);
        //}
        //Ok(dbs.unwrap())
    }

    pub fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableMeta>> {
        todo!()
    }

    pub fn get_table_by_id(
        &self,
        _tbl_id: MetaId,
        _tbl_version: Option<MetaVersion>,
    ) -> Result<Arc<dyn Table>> {
        todo!()
    }
}

fn from_database_meta(db_meta: DatabaseMeta) -> Vec<Arc<dyn Database>> {
    todo!()
}

struct Syncer {
    sync_period: Duration,
    remote_factory: StoreClientProvider,
    cond: Condvar,
    fetch_hinter: mpsc::Receiver<mpsc::Sender<DatabaseMetaSnapshot>>,
}

impl Syncer {
    fn kickoff(&self) {
        let provider = self.remote_factory.clone();
        let (sender, receiver) = mpsc::sync_channel::<mpsc::Sender<DatabaseMetaSnapshot>>(10);

        let handle = std::thread::Builder::new()
            .name("remote-metastore-syncer".to_string())
            .spawn(move || {
                let rt = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Huston, unable to bring up syncer runtime");

                let mut databases: Option<DatabaseMetaSnapshot> = None;
                'main: loop {
                    let requester = loop {
                        let s = receiver.recv_timeout(Duration::from_secs(1));
                        match s {
                            Ok(sender) => {
                                if databases.as_ref().is_some() {
                                    sender.send(databases.as_ref().unwrap().clone());
                                    continue;
                                } else {
                                    // no available db meta, we break out of while, and do fetch db meta from remote
                                    break Some(sender);
                                }
                            }
                            Err(_) => {
                                // no waiting requester, or requester are dropped
                                break None;
                            }
                            _ => {
                                break 'main;
                            }
                        }
                    };

                    let r: common_exception::Result<DatabaseMetaSnapshot> = rt.block_on(async {
                        let mut client = provider.try_get_client().await?;
                        let databases = client.get_databases(None).await?;
                        Ok(Arc::new(from_database_meta(databases)))
                    });

                    match r {
                        Ok(res) => {
                            databases = Some(res.clone());
                            requester.map(|req| {
                                req.send(res);
                            });
                        }
                        Err(e) => {
                            log::warn!("fetch remote db meta failed. {}", e)
                        }
                    };
                }
            });
    }
}
