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
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_grpc::RpcClientConf;
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
use common_tracing::tracing;
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

pub(crate) struct CompactionTask {
    rpc_conf: RpcClientConf,
    delay_seconds: u64,
    ctx: Arc<QueryContext>,
}

impl CompactionTask {
    pub fn try_create(conf: Config, ctx: Arc<QueryContext>) -> Result<Self> {
        let rpc_conf = conf.meta.to_meta_grpc_client_conf();
        if rpc_conf.local_mode() {
            tracing::error!("Auto compaction is not supported in local mode");
            return Err(ErrorCode::BackgroundTaskError(
                "Auto compaction is not supported in local mode".to_string(),
            ));
        }
        let delay_seconds = conf.task.delay_seconds;
        Ok(Self {
            rpc_conf,
            delay_seconds,
            ctx,
        })
    }

    pub async fn start(&self) -> Result<()> {
        let (key, key_end) = get_start_and_end_of_prefix(PREFIX_TABLE_BY_ID)?;
        let watch = WatchRequest {
            key,
            key_end: Some(key_end),
            filter_type: FilterType::Update.into(),
        };
        // Create a new client of metasrv.
        let client = MetaGrpcClient::try_new(&self.rpc_conf)?;

        // Watch the table meta update.
        let mut client_stream = client.request(watch).await?;
        let task = CompactionQueue::new(Duration::from_secs(self.delay_seconds));
        let delay_queue_insert = task.create_channel(self.ctx.clone());
        while let Ok(Some(resp)) = client_stream.message().await {
            if let Some(event) = resp.event {
                if event.prev.is_none() || event.current.is_none() {
                    continue;
                }

                let key = event.key;
                let id = TableId::from_key(&key).unwrap();

                let current_seqv = event.current.unwrap();
                let seq = current_seqv.seq;
                let current: TableMeta = deserialize_struct(&current_seqv.data).unwrap();
                if current.drop_on.is_some() || current.default_cluster_key.is_some() {
                    continue;
                }

                let prev_seqv = event.prev.unwrap();
                let prev: TableMeta = deserialize_struct(&prev_seqv.data).unwrap();
                if current.statistics.number_of_rows == prev.statistics.number_of_rows {
                    continue;
                }

                let ident = TableIdent::new(id.table_id, seq);
                let table_info = TableInfo {
                    ident,
                    desc: "".to_owned(),
                    name: "".to_owned(),
                    meta: current,
                };

                delay_queue_insert
                    .send((id.table_id, table_info))
                    .await
                    .unwrap();
            }
        }

        Ok(())
    }

    async fn do_compaction(
        ctx: Arc<QueryContext>,
        table: Arc<dyn Table>,
        database: String,
    ) -> Result<()> {
        if !table.auto_compaction() {
            tracing::info!("Table {} is not auto compaction", table.get_id());
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

    fn create_channel(self, ctx: Arc<QueryContext>) -> Sender<(u64, TableInfo)> {
        let (queue_add_tx, mut queue_add_rx) = channel::<(u64, TableInfo)>(1);
        let catalog = ctx.get_catalog(CATALOG_DEFAULT).unwrap();

        ctx.get_storage_runtime().spawn(async move {
            let mut task = self;
            let mut queue_add_eof = false;
            loop {
                tokio::select! {
                    // delayed item from the DelayQueue becomes available.
                    delayed_item = task.next() => {
                        match delayed_item {
                            Some(item) => {
                                let table = catalog.get_table_by_info(&item).unwrap();
                                let ctx = Arc::clone(&ctx);
                                ctx.get_storage_runtime()
                                .spawn(async move { CompactionTask::do_compaction(ctx.clone(), table, "".to_owned()).await });
                            },
                            None => {
                                // if the queue_add_tx channel is gone, we're done.
                                if queue_add_eof {
                                    break;
                                }
                            },
                        }
                    }
                    // an item is received from the queue_add_tx channel.
                    new_item = queue_add_rx.recv(), if !queue_add_eof => {
                        match new_item {
                            Some(item) => {
                                // insert it into the DelayQueue.
                                task.insert(item.0, item.1);
                            },
                            None => {
                                // the queue_add_tx channel was dropped.
                                queue_add_eof = true;
                            },
                        }
                    }
                }
            }
        });
        queue_add_tx
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
