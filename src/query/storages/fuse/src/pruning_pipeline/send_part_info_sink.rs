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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_channel::Sender;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::TopK;
use databend_common_catalog::plan::VirtualColumnInfo;
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
use databend_common_storage::StorageMetrics;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use parking_lot::Mutex;

use crate::pruning::FusePruner;
use crate::pruning_pipeline::block_prune_result_meta::BlockPruneResult;
use crate::FuseTable;

pub struct SendPartCache {
    partitions: Partitions,
    statistics: PartStatistics,
    derterministic_cache_key: Option<String>,
    fuse_pruner: Arc<FusePruner>,
}

pub struct SendPartState {
    cache: Mutex<SendPartCache>,
    limit: AtomicUsize,
    data_metrics: Arc<StorageMetrics>,
}

impl SendPartState {
    pub fn create(
        derterministic_cache_key: Option<String>,
        limit: Option<usize>,
        fuse_pruner: Arc<FusePruner>,
        data_metrics: Arc<StorageMetrics>,
        partitions_total: usize,
    ) -> Self {
        let mut statistics = PartStatistics::default_exact();
        statistics.partitions_total = partitions_total;
        SendPartState {
            cache: Mutex::new(SendPartCache {
                partitions: Partitions::default(),
                statistics,
                derterministic_cache_key,
                fuse_pruner,
            }),
            limit: AtomicUsize::new(limit.unwrap_or(usize::MAX)),
            data_metrics,
        }
    }

    pub fn populating_cache(&self) {
        type CacheItem = (PartStatistics, Partitions);
        let mut send_part_cache = self.cache.lock();
        let pruning_stats = send_part_cache.fuse_pruner.pruning_stats();
        send_part_cache.statistics.pruning_stats = pruning_stats;
        if let Some(cache_key) = &send_part_cache.derterministic_cache_key {
            if let Some(cache) = CacheItem::cache() {
                cache.insert(
                    cache_key.clone(),
                    (
                        send_part_cache.statistics.clone(),
                        send_part_cache.partitions.clone(),
                    ),
                );
            }
        }
    }

    pub fn detach_sinker(&self, partitions: &Partitions, statistics: &PartStatistics) {
        let mut send_part_cache = self.cache.lock();
        // concat partitions and statistics
        send_part_cache
            .partitions
            .partitions
            .extend(partitions.partitions.clone());
        send_part_cache.statistics.merge(statistics);
        if !statistics.is_exact {
            send_part_cache.statistics.is_exact = false;
        }
        // the kind is determined by the push_downs, should be same for all partitions
        send_part_cache.partitions.kind = partitions.kind.clone();

        // Update context statistics
        self.data_metrics
            .inc_partitions_total(send_part_cache.statistics.partitions_total as u64);
        self.data_metrics
            .inc_partitions_scanned(send_part_cache.statistics.partitions_scanned as u64);
    }

    pub fn set_pruning_stats(&self, stat: PartStatistics) {
        let mut send_part_cache = self.cache.lock();
        send_part_cache.statistics = stat;
    }

    pub fn get_pruning_stats(&self) -> PartStatistics {
        let send_part_cache = self.cache.lock();
        send_part_cache.statistics.clone()
    }
}

pub struct SendPartInfoSink {
    sender: Option<Sender<Result<PartInfoPtr>>>,
    push_downs: Option<PushDownInfo>,
    top_k: Option<(TopK, Scalar)>,
    schema: TableSchemaRef,
    partitions: Partitions,
    statistics: PartStatistics,
    send_part_state: Arc<SendPartState>,
    dry_run: bool,
    enable_cache: bool,
}

impl SendPartInfoSink {
    pub fn create(
        input: Arc<InputPort>,
        sender: Sender<Result<PartInfoPtr>>,
        push_downs: Option<PushDownInfo>,
        top_k: Option<(TopK, Scalar)>,
        schema: TableSchemaRef,
        send_part_state: Arc<SendPartState>,
        dry_run: bool,
        enable_cache: bool,
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
                dry_run,
                enable_cache,
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncSink for SendPartInfoSink {
    const NAME: &'static str = "SendPartInfoSink";

    async fn on_finish(&mut self) -> Result<()> {
        self.send_part_state
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

                self.statistics.partitions_scanned += block_metas.len();

                let info_ptr = match self.push_downs.clone() {
                    None => self.all_columns_partitions(block_metas),
                    Some(extras) => match &extras.projection {
                        None => self.all_columns_partitions(block_metas),
                        Some(projection) => self.projection_partitions(
                            block_metas,
                            projection,
                            &column_nodes,
                            &extras.output_columns,
                            &extras.virtual_column,
                        ),
                    },
                };
                // if dry_run, we don't send the data to the next pipeline
                // we only want to get the pruning statistics
                if self.dry_run {
                    return Ok(false);
                }

                if self.enable_cache {
                    self.partitions.partitions.extend(info_ptr.clone());
                }

                for info in info_ptr {
                    if let Some(sender) = &self.sender {
                        if let Err(_e) = sender.send(Ok(info)).await {
                            break;
                        }
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
        if self.send_part_state.limit.load(Ordering::SeqCst) == 0 {
            return vec![];
        }
        let mut parts = Vec::with_capacity(block_metas.len());

        for (block_meta_index, block_meta) in block_metas.iter() {
            let rows = block_meta.row_count as usize;
            let previous_limit = self.send_part_state.limit.fetch_sub(
                rows.min(self.send_part_state.limit.load(Ordering::SeqCst)),
                Ordering::SeqCst,
            );
            parts.push(FuseTable::all_columns_part(
                Some(&self.schema),
                &Some(block_meta_index.to_owned()),
                &self.top_k,
                block_meta,
            ));
            self.statistics.read_rows += rows;
            self.statistics.read_bytes += block_meta.block_size as usize;

            if rows >= previous_limit {
                if rows != previous_limit {
                    self.statistics.is_exact = false;
                }
                break;
            }
        }
        parts
    }

    fn projection_partitions(
        &mut self,
        block_metas: &[(BlockMetaIndex, Arc<BlockMeta>)],
        projection: &Projection,
        column_nodes: &ColumnNodes,
        output_columns: &Option<Projection>,
        virtual_column: &Option<VirtualColumnInfo>,
    ) -> Vec<PartInfoPtr> {
        if self.send_part_state.limit.load(Ordering::SeqCst) == 0 {
            return vec![];
        }

        let mut parts = Vec::with_capacity(block_metas.len());

        // Output columns don't have source columns of virtual columns,
        // which can be ignored if all virtual columns are generated.
        let columns = if let Some(output_columns) = output_columns {
            output_columns.project_column_nodes(column_nodes).unwrap()
        } else {
            projection.project_column_nodes(column_nodes).unwrap()
        };

        for (block_meta_index, block_meta) in block_metas.iter() {
            let rows = block_meta.row_count as usize;
            let previous_limit = self.send_part_state.limit.fetch_sub(
                rows.min(self.send_part_state.limit.load(Ordering::SeqCst)),
                Ordering::SeqCst,
            );
            parts.push(FuseTable::projection_part(
                block_meta,
                &Some(block_meta_index.to_owned()),
                column_nodes,
                self.top_k.clone(),
                projection,
            ));
            self.statistics.read_rows += rows;
            for column in &columns {
                for column_id in &column.leaf_column_ids {
                    // ignore all deleted field
                    if let Some(col_metas) = &block_meta.col_metas.get(column_id) {
                        let (_, len) = col_metas.offset_length();
                        self.statistics.read_bytes += len as usize;
                    }
                }
            }
            let virtual_block_meta = &block_meta_index.virtual_block_meta;
            if let Some(virtual_column) = virtual_column {
                if let Some(virtual_block_meta) = virtual_block_meta {
                    // Add bytes of virtual columns
                    for virtual_column_meta in virtual_block_meta.virtual_column_metas.values() {
                        let (_, len) = virtual_column_meta.offset_length();
                        self.statistics.read_bytes += len as usize;
                    }

                    // Check whether source columns can be ignored.
                    // If not, add bytes of source columns.
                    for source_column_id in &virtual_column.source_column_ids {
                        if virtual_block_meta
                            .ignored_source_column_ids
                            .contains(source_column_id)
                        {
                            continue;
                        }
                        if let Some(col_metas) = &block_meta.col_metas.get(source_column_id) {
                            let (_, len) = col_metas.offset_length();
                            self.statistics.read_bytes += len as usize;
                        }
                    }
                } else {
                    // If virtual column meta not exist, all source columns are needed.
                    for source_column_id in &virtual_column.source_column_ids {
                        if let Some(col_metas) = &block_meta.col_metas.get(source_column_id) {
                            let (_, len) = col_metas.offset_length();
                            self.statistics.read_bytes += len as usize;
                        }
                    }
                }
            }

            if rows >= previous_limit {
                if rows != previous_limit {
                    self.statistics.is_exact = false;
                }
                break;
            }
        }

        parts
    }
}
