// Copyright 2021 Datafuse Labs
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

use async_channel::Sender;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::TopK;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_storage::ColumnNodes;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use parking_lot::Mutex;

use crate::pruning_pipeline::block_prune_result_meta::BlockPruneResult;
use crate::FuseBlockPartInfo;

pub struct SendPartState {
    partitions: Partitions,
    statistics: PartStatistics,
    derterministic_cache_key: Option<String>,
}

impl SendPartState {
    pub fn create(derterministic_cache_key: Option<String>) -> Self {
        SendPartState {
            partitions: Partitions::default(),
            statistics: PartStatistics::default(),
            derterministic_cache_key,
        }
    }

    pub fn set_cache(&self) {
        type CacheItem = (PartStatistics, Partitions);
        if let Some(cache_key) = &self.derterministic_cache_key {
            if let Some(cache) = CacheItem::cache() {
                cache.insert(
                    cache_key.clone(),
                    (self.statistics.clone(), self.partitions.clone()),
                );
            }
        }
    }

    // Every SendPartInfoSink processor need call detach_sinker to
    pub fn detach_sinker(&mut self, partitions: &Partitions, statistics: &PartStatistics) {
        // concat partitions and statistics
        self.partitions
            .partitions
            .extend(partitions.partitions.clone());
        self.statistics.merge(statistics);
        // the kind is determined by the push_downs, should be same for all partitions
        self.partitions.kind = partitions.kind.clone();
    }
}

pub struct SendPartInfoSink {
    sender: Option<Sender<Result<PartInfoPtr>>>,
    push_downs: Option<PushDownInfo>,
    top_k: Option<(TopK, Scalar)>,
    schema: TableSchemaRef,
    partitions: Partitions,
    statistics: PartStatistics,
    send_part_state: Arc<Mutex<SendPartState>>,
}

impl SendPartInfoSink {
    pub fn create(
        input: Arc<InputPort>,
        sender: Sender<Result<PartInfoPtr>>,
        push_downs: Option<PushDownInfo>,
        top_k: Option<(TopK, Scalar)>,
        schema: TableSchemaRef,
        send_part_state: Arc<Mutex<SendPartState>>,
    ) -> Result<ProcessorPtr> {
        let partitions = Partitions::default();
        let statistics = PartStatistics::default();
        Ok(ProcessorPtr::create(AsyncSinker::create(
            input,
            SendPartInfoSink {
                sender: Some(sender),
                push_downs,
                top_k,
                schema,
                partitions,
                statistics,
                send_part_state,
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncSink for SendPartInfoSink {
    const NAME: &'static str = "SendPartInfoSink";

    async fn on_finish(&mut self) -> Result<()> {
        self.send_part_state
            .lock()
            .detach_sinker(&self.partitions, &self.statistics);
        drop(self.sender.take());
        Ok(())
    }

    async fn consume(&mut self, mut data_block: DataBlock) -> Result<bool> {
        if let Some(meta) = data_block.take_meta() {
            if let Some(data) = BlockPruneResult::downcast_from(meta) {
                let arrow_schema = self.schema.as_ref().into();
                let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));
                let block_metas = &data.block_metas;
                let info_ptr = match self.push_downs.clone() {
                    None => self.all_columns_partitions(block_metas),
                    Some(extras) => match &extras.projection {
                        None => self.all_columns_partitions(block_metas),
                        Some(projection) => {
                            self.projection_partitions(block_metas, projection, &column_nodes)
                        }
                    },
                };

                self.partitions.partitions.extend(info_ptr.clone());

                for info in info_ptr {
                    if let Some(sender) = &self.sender {
                        let _ = sender.send(Ok(info)).await;
                    }
                }

                return Ok(false);
            }
        }
        Err(ErrorCode::Internal(
            "Cannot downcast data block meta to BlockPruneResult".to_string(),
        ))
    }
}

impl SendPartInfoSink {
    fn all_columns_partitions(
        &mut self,
        block_metas: &[(BlockMetaIndex, Arc<BlockMeta>)],
    ) -> Vec<PartInfoPtr> {
        self.partitions.kind = PartitionsShuffleKind::Mod;
        block_metas
            .iter()
            .map(|(block_meta_index, block_meta)| {
                let mut columns_meta = HashMap::with_capacity(block_meta.col_metas.len());
                let mut columns_stats = HashMap::with_capacity(block_meta.col_stats.len());

                for column_id in block_meta.col_metas.keys() {
                    // ignore all deleted field
                    if self.schema.is_column_deleted(*column_id) {
                        continue;
                    }

                    // ignore column this block dose not exist
                    if let Some(meta) = block_meta.col_metas.get(column_id) {
                        columns_meta.insert(*column_id, meta.clone());
                    }

                    if let Some(stat) = block_meta.col_stats.get(column_id) {
                        columns_stats.insert(*column_id, stat.clone());
                    }
                }

                let rows_count = block_meta.row_count;
                let location = block_meta.location.0.clone();
                let create_on = block_meta.create_on;

                let sort_min_max = self.top_k.as_ref().map(|(top_k, default)| {
                    block_meta
                        .col_stats
                        .get(&top_k.field.column_id)
                        .map(|stat| (stat.min().clone(), stat.max().clone()))
                        .unwrap_or((default.clone(), default.clone()))
                });

                FuseBlockPartInfo::create(
                    location,
                    rows_count,
                    columns_meta,
                    Some(columns_stats),
                    block_meta.compression(),
                    sort_min_max,
                    Some(block_meta_index.to_owned()),
                    create_on,
                )
            })
            .collect()
    }

    fn projection_partitions(
        &mut self,
        block_metas: &[(BlockMetaIndex, Arc<BlockMeta>)],
        projection: &Projection,
        column_nodes: &ColumnNodes,
    ) -> Vec<PartInfoPtr> {
        self.partitions.kind = PartitionsShuffleKind::Seq;
        block_metas
            .iter()
            .map(|(block_meta_index, block_meta)| {
                let mut columns_meta = HashMap::with_capacity(projection.len());
                let mut columns_stat = HashMap::with_capacity(projection.len());

                let columns = projection.project_column_nodes(column_nodes).unwrap();
                for column in &columns {
                    for column_id in &column.leaf_column_ids {
                        // ignore column this block dose not exist
                        if let Some(column_meta) = block_meta.col_metas.get(column_id) {
                            columns_meta.insert(*column_id, column_meta.clone());
                        }
                        if let Some(column_stat) = block_meta.col_stats.get(column_id) {
                            columns_stat.insert(*column_id, column_stat.clone());
                        }
                    }
                }

                let rows_count = block_meta.row_count;
                let location = block_meta.location.0.clone();
                let create_on = block_meta.create_on;

                let sort_min_max = self.top_k.clone().map(|(top_k, default)| {
                    let stat = block_meta.col_stats.get(&top_k.field.column_id);
                    stat.map(|stat| (stat.min().clone(), stat.max().clone()))
                        .unwrap_or((default.clone(), default))
                });

                // TODO
                // row_count should be a hint value of  LIMIT,
                // not the count the rows in this partition
                FuseBlockPartInfo::create(
                    location,
                    rows_count,
                    columns_meta,
                    Some(columns_stat),
                    block_meta.compression(),
                    sort_min_max,
                    Some(block_meta_index.to_owned()),
                    create_on,
                )
            })
            .collect()
    }
}
