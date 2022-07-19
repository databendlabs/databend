// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use common_base::base::tokio;
use common_base::base::tokio::sync::mpsc::channel;
use common_base::base::tokio::sync::mpsc::error::SendError;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::base::TrySpawn;
use common_exception::ErrorCode;
use common_meta_api::deserialize_struct;
use common_meta_api::get_start_and_end_of_prefix;
use common_meta_api::KVApiKey;
use common_meta_api::PREFIX_TABLE_BY_ID;
use common_meta_app::schema::TableId;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_grpc::MetaGrpcClient;
use common_meta_types::protobuf::watch_request::FilterType;
use common_meta_types::protobuf::WatchRequest;
use common_planners::OptimizeTableAction;
use common_planners::OptimizeTablePlan;
use futures::ready;
use futures::Stream;
use tokio_stream::StreamExt;
use tokio_util::time::delay_queue;
use tokio_util::time::DelayQueue;

use crate::catalogs::CATALOG_DEFAULT;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::storages::Table;
use crate::Config;

pub async fn compaction(ctx: Arc<QueryContext>, conf: &Config) -> common_exception::Result<()> {
    let rpc_conf = conf.meta.to_meta_grpc_client_conf();
    if rpc_conf.local_mode() {
        return Err(ErrorCode::CompactionError(
            "Auto compaction is not supported in local mode".to_string(),
        ));
    }

    let (key, key_end) = get_start_and_end_of_prefix(PREFIX_TABLE_BY_ID)?;
    let watch = WatchRequest {
        key,
        key_end: Some(key_end),
        filter_type: FilterType::Update.into(),
    };
    // Create a new client of metasrv.
    let client = MetaGrpcClient::try_new(&rpc_conf)?;

    // Watch the table meta update.
    let mut client_stream = client.request(watch).await?;
    let task = CompactionQueue::new(Duration::from_secs(3600));
    let (mut delay_queue_insert, mut delay_queue_read) = task.create_channel(ctx.clone());
    ctx.get_storage_runtime().spawn(async move {
        while let Ok(Some(resp)) = client_stream.message().await {
            if let Some(event) = resp.event {
                if event.prev.is_none() || event.current.is_none() {
                    continue;
                }

                let key = event.key;
                let id = TableId::from_key(&key).unwrap();

                let current_seqv = event.current.unwrap();
                let seq = current_seqv.seq;
                let meta: TableMeta = deserialize_struct(&current_seqv.data).unwrap();
                if meta.drop_on.is_some() || meta.default_cluster_key.is_some() {
                    continue;
                }

                let ident = TableIdent::new(id.table_id, seq);
                let table_info = TableInfo {
                    ident,
                    desc: "".to_owned(),
                    name: "".to_owned(),
                    meta,
                };
                delay_queue_insert
                    .insert(id.table_id, table_info)
                    .await
                    .unwrap();
            }
        }
    });

    let catalog = ctx.get_catalog(CATALOG_DEFAULT)?;
    while let Some(info) = delay_queue_read.next().await {
        let table = catalog.get_table_by_info(&info)?;
        let ctx = Arc::clone(&ctx);
        ctx.get_storage_runtime()
            .spawn(async move { do_compaction(ctx.clone(), table, "".to_owned()).await });
    }

    Ok(())
}

pub async fn do_compaction(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    database: String,
) -> common_exception::Result<()> {
    if !table.auto_compaction() {
        return Ok(());
    }

    let plan = OptimizeTablePlan {
        catalog: ctx.get_current_catalog(),
        database,
        table: table.name().to_string(),
        action: OptimizeTableAction::Compact,
    };

    table.compact(ctx.clone(), plan).await
}

struct CompactionQueue {
    timeout: Duration,

    entries: HashMap<u64, (TableInfo, delay_queue::Key)>,
    expirations: DelayQueue<u64>,
}

impl CompactionQueue {
    fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            entries: HashMap::new(),
            expirations: DelayQueue::new(),
        }
    }

    fn insert(&mut self, table_id: u64, table_info: TableInfo) {
        if let Some((_, key)) = self.entries.get(&table_id) {
            self.expirations.reset(key, self.timeout);
            self.entries.insert(table_id, (table_info, *key));
        } else {
            let key = self.expirations.insert(table_id, self.timeout);
            self.entries.insert(table_id, (table_info, key));
        }
    }

    fn poll_expired(&mut self, cx: &mut Context) -> Poll<Option<TableInfo>> {
        match ready!(self.expirations.poll_expired(cx)) {
            Some(key) => {
                let table_info = self.entries.remove(key.get_ref()).unwrap().0;
                Poll::Ready(Some(table_info))
            }
            None => Poll::Pending,
        }
    }

    fn create_channel(self, ctx: Arc<QueryContext>) -> (DelayQueueInsert, DelayQueueRead) {
        let (queue_add, mut queue_add_rx) = channel::<(u64, TableInfo)>(1);
        let (queue_read_tx, queue_read) = channel::<Option<TableInfo>>(1);

        ctx.get_storage_runtime().spawn(async move {
            let mut task = self;
            let mut queue_add_eof = false;
            loop {
                tokio::select! {
                    // delayed item from the DelayQueue becomes available.
                    delayed_item = task.next() => {
                        match delayed_item {
                            Some(item) => {
                                // forward it to the queue_read channel.
                                if queue_read_tx.send(Some(item)).await.is_err() {
                                    // queue_read channel receiver side was dropped.
                                    break;
                                }
                            },
                            None => {
                                // if the queue_add channel is gone, we're done.
                                if queue_add_eof {
                                    break;
                                }
                            },
                        }
                    }
                    // an item is received from the queue_add channel.
                    new_item = queue_add_rx.recv(), if !queue_add_eof => {
                        match new_item {
                            Some(item) => {
                                // insert it into the DelayQueue.
                                task.insert(item.0, item.1);
                            },
                            None => {
                                // the queue_add channel was dropped.
                                queue_add_eof = true;
                            },
                        }
                    }
                }
            }
        });
        (DelayQueueInsert(queue_add), DelayQueueRead(queue_read))
    }
}

impl Stream for CompactionQueue {
    type Item = TableInfo;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let self_mut = self.get_mut();
        self_mut.poll_expired(ctx)
    }
}

pub struct DelayQueueInsert(Sender<(u64, TableInfo)>);

impl DelayQueueInsert {
    /// Insert an item into the DelayQueue.
    pub async fn insert(
        &mut self,
        table_id: u64,
        table_info: TableInfo,
    ) -> Result<(), SendError<(u64, TableInfo)>> {
        self.0.send((table_id, table_info)).await
    }
}

pub struct DelayQueueRead(Receiver<Option<TableInfo>>);

impl DelayQueueRead {
    /// Read the next item from the DelayQueue.
    pub async fn next(&mut self) -> Option<TableInfo> {
        match self.0.recv().await {
            Some(Some(item)) => Some(item),
            Some(None) => None,
            None => None,
        }
    }
}
