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

// Logs from this module will show up as "[FUSE-PARTITIONS] ...".
databend_common_tracing::register_module_tag!("[FUSE-PARTITIONS]");

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PruningStatistics;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::TopK;
use databend_common_catalog::plan::VirtualColumnInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_sql::DefaultExprBinder;
use databend_common_storage::ColumnNodes;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_pruner::TopNPruner;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::column_oriented_segment::BLOCK_SIZE;
use databend_storages_common_table_meta::meta::column_oriented_segment::BLOOM_FILTER_INDEX_LOCATION;
use databend_storages_common_table_meta::meta::column_oriented_segment::BLOOM_FILTER_INDEX_SIZE;
use databend_storages_common_table_meta::meta::column_oriented_segment::CLUSTER_STATS;
use databend_storages_common_table_meta::meta::column_oriented_segment::COMPRESSION;
use databend_storages_common_table_meta::meta::column_oriented_segment::CREATE_ON;
use databend_storages_common_table_meta::meta::column_oriented_segment::FILE_SIZE;
use databend_storages_common_table_meta::meta::column_oriented_segment::INVERTED_INDEX_SIZE;
use databend_storages_common_table_meta::meta::column_oriented_segment::LOCATION;
use databend_storages_common_table_meta::meta::column_oriented_segment::NGRAM_FILTER_INDEX_SIZE;
use databend_storages_common_table_meta::meta::column_oriented_segment::ROW_COUNT;
use databend_storages_common_table_meta::meta::column_oriented_segment::meta_name;
use databend_storages_common_table_meta::meta::column_oriented_segment::stat_name;
use databend_storages_common_table_meta::table::ChangeType;
use databend_storages_common_table_meta::table::ClusterType;
use itertools::Itertools;
use log::info;
use opendal::Operator;
use sha2::Digest;
use sha2::Sha256;

use crate::FuseLazyPartInfo;
use crate::FuseSegmentFormat;
use crate::FuseTable;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::BloomIndexRebuilder;
use crate::pruning::BlockPruner;
use crate::pruning::FusePruner;
use crate::pruning::SegmentLocation;
use crate::pruning::SegmentPruner;
use crate::pruning::VectorIndexPruner;
use crate::pruning::create_segment_location_vector;
use crate::pruning::table_sample;
use crate::pruning_pipeline::AsyncBlockPruneTransform;
use crate::pruning_pipeline::ColumnOrientedBlockPruneSink;
use crate::pruning_pipeline::ExtractSegmentTransform;
use crate::pruning_pipeline::LazySegmentReceiverSource;
use crate::pruning_pipeline::PrunedColumnOrientedSegmentMeta;
use crate::pruning_pipeline::PrunedCompactSegmentMeta;
use crate::pruning_pipeline::SampleBlockMetasTransform;
use crate::pruning_pipeline::SegmentPruneTransform;
use crate::pruning_pipeline::SendPartInfoSink;
use crate::pruning_pipeline::SendPartState;
use crate::pruning_pipeline::SyncBlockPruneTransform;
use crate::pruning_pipeline::TopNPruneTransform;
use crate::pruning_pipeline::VectorIndexPruneTransform;
use crate::segment_format_from_location;

const DEFAULT_GRAM_SIZE: usize = 3;
const DEFAULT_BLOOM_SIZE: u64 = 1024 * 1024;

impl FuseTable {
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let distributed_pruning = ctx.get_settings().get_enable_distributed_pruning()?;
        if let Some(changes_desc) = &self.changes_desc {
            // For "ANALYZE TABLE" statement, we need set the default change type to "Insert".
            let change_type = push_downs.as_ref().map_or(ChangeType::Insert, |v| {
                v.change_type.clone().unwrap_or(ChangeType::Insert)
            });
            return self
                .do_read_changes_partitions(ctx, push_downs, change_type, &changes_desc.location)
                .await;
        }

        let snapshot = self.read_table_snapshot().await?;

        info!(
            "Reading partitions for table {}, push downs: {:?}, snapshot: {:?}",
            self.name(),
            push_downs,
            snapshot.as_ref().map(|sn| sn.snapshot_id)
        );
        match snapshot {
            Some(snapshot) => {
                // To optimize the Hilbert clustering logic, it is necessary to pre-set the selected segments.
                // Since the recluster logic requires scanning the table twice, fetching the segments directly
                // can avoid redundant selection logic and ensure that the same data is accessed during both scans.
                // TODO(zhyass): refactor if necessary.
                let selected_segment = ctx.get_selected_segment_locations();
                let segment_locs = if !selected_segment.is_empty() {
                    selected_segment
                } else {
                    snapshot.segments.clone()
                };
                let segment_len = segment_locs.len();

                // snapshot.summary.block_count
                let snapshot_loc = self.meta_location_generator.gen_snapshot_location(
                    self.get_branch_id(),
                    &snapshot.snapshot_id,
                    snapshot.format_version,
                )?;

                let mut nodes_num = 1;
                let cluster = ctx.get_cluster();

                if !cluster.is_empty() {
                    nodes_num = cluster.nodes.len();
                }

                if self.is_column_oriented() || (segment_len > nodes_num && distributed_pruning) {
                    let mut segments = Vec::with_capacity(segment_locs.len());
                    for (idx, segment_location) in segment_locs.into_iter().enumerate() {
                        segments.push(FuseLazyPartInfo::create(idx, segment_location))
                    }

                    return Ok((
                        PartStatistics::new_estimated(
                            Some(snapshot_loc),
                            snapshot.summary.row_count as usize,
                            snapshot.summary.compressed_byte_size as usize,
                            segment_len,
                            segment_len,
                        ),
                        Partitions::create(PartitionsShuffleKind::Mod, segments),
                    ));
                }

                let snapshot_loc = Some(snapshot_loc);
                let table_schema = self.schema_with_stream();
                let summary = snapshot.summary.block_count as usize;
                let segments_location = create_segment_location_vector(segment_locs, snapshot_loc);

                self.prune_snapshot_blocks(
                    ctx.clone(),
                    push_downs.clone(),
                    table_schema,
                    segments_location,
                    summary,
                )
                .await
            }
            None => Ok((PartStatistics::default(), Partitions::default())),
        }
    }

    pub fn do_build_prune_pipeline(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        source_pipeline: &mut Pipeline,
        plan_id: u32,
    ) -> Result<Option<Pipeline>> {
        let snapshot = plan.statistics.snapshot.clone();
        let table_schema = self.schema_with_stream();
        let dal = self.operator.clone();
        let mut lazy_init_segments = Vec::with_capacity(plan.parts.len());
        let mut segment_format = FuseSegmentFormat::Row;

        for part in &plan.parts.partitions {
            if let Some(lazy_part_info) = part.as_any().downcast_ref::<FuseLazyPartInfo>() {
                lazy_init_segments.push(SegmentLocation {
                    segment_idx: lazy_part_info.segment_index,
                    location: lazy_part_info.segment_location.clone(),
                    snapshot_loc: snapshot.clone(),
                });
                segment_format = segment_format_from_location(&lazy_part_info.segment_location.0);
            }
        }
        // If there is no lazy part, we don't need to prune
        if lazy_init_segments.is_empty() {
            return Ok(None);
        }
        let push_downs = plan.push_downs.clone();
        let max_io_requests = self.adjust_io_request(&ctx)?;
        let (part_info_tx, part_info_rx) = async_channel::bounded(max_io_requests);
        self.pruned_result_receiver.lock().replace(part_info_rx);

        // If we can get the pruning result from cache, we don't need to prune again, but need sent
        // the parts to the next pipeline
        let derterministic_cache_key =
            push_downs
                .as_ref()
                .filter(|p| p.is_deterministic)
                .map(|push_downs| {
                    format!(
                        "{:x}",
                        Sha256::digest(format!("{:?}_{:?}", lazy_init_segments, push_downs))
                    )
                });

        if ctx.get_settings().get_enable_prune_cache()? {
            if let Some((stat, part)) = Self::check_prune_cache(&derterministic_cache_key) {
                ctx.set_pruned_partitions_stats(plan_id, stat);
                let sender = part_info_tx.clone();
                info!("Retrieved pruning result from cache");
                source_pipeline.set_on_init(move || {
                    // We cannot use the runtime associated with the query to avoid increasing its lifetime.
                    GlobalIORuntime::instance().spawn(async move {
                        // avoid block global io runtime
                        let runtime =
                            Runtime::with_worker_threads(2, Some("send-parts".to_string()))?;

                        let join_handler = runtime.spawn(async move {
                            for part in part.partitions {
                                // the sql may be killed or early stop, ignore the error
                                if let Err(_e) = sender.send(Ok(part)).await {
                                    break;
                                }
                            }
                        });

                        if let Err(cause) = join_handler.await {
                            log::warn!("Join error in prune pipeline: {:?}", cause);
                        }

                        Result::Ok(())
                    });

                    Ok(())
                });
                return Ok(None);
            }
        }

        let mut prune_pipeline = Pipeline::create();
        let pruner =
            Arc::new(self.build_fuse_pruner(ctx.clone(), push_downs, table_schema.clone(), dal)?);

        let (segment_tx, segment_rx) = async_channel::bounded(max_io_requests);

        match segment_format {
            FuseSegmentFormat::Row => {
                self.prune_segments_with_pipeline(
                    pruner.clone(),
                    &mut prune_pipeline,
                    ctx.clone(),
                    segment_rx,
                    part_info_tx,
                    derterministic_cache_key.clone(),
                    lazy_init_segments.len(),
                    plan_id,
                )?;
            }
            FuseSegmentFormat::Column => {
                self.prune_column_oriented_segments_with_pipeline(
                    pruner.clone(),
                    &mut prune_pipeline,
                    ctx.clone(),
                    segment_rx,
                    part_info_tx,
                    derterministic_cache_key.clone(),
                    table_schema.clone(),
                )?;
            }
        }
        prune_pipeline.set_on_init(move || {
            // We cannot use the runtime associated with the query to avoid increasing its lifetime.
            GlobalIORuntime::instance().spawn(async move {
                // avoid block global io runtime
                let runtime = Runtime::with_worker_threads(2, Some("prune-pipeline".to_string()))?;
                let join_handler = runtime.spawn(async move {
                    for segment in lazy_init_segments {
                        // the sql may be killed or early stop, ignore the error
                        if let Err(_e) = segment_tx.send(segment).await {
                            break;
                        }
                    }
                    Ok::<_, ErrorCode>(())
                });

                if let Err(cause) = join_handler.await {
                    log::warn!("Join error in prune pipeline: {:?}", cause);
                }
                Ok::<_, ErrorCode>(())
            });
            Ok(())
        });

        Ok(Some(prune_pipeline))
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn prune_snapshot_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        table_schema: TableSchemaRef,
        segments_location: Vec<SegmentLocation>,
        summary: usize,
    ) -> Result<(PartStatistics, Partitions)> {
        let num_segments_to_prune = segments_location.len();
        let start = Instant::now();
        info!(
            "prune snapshot block start, {} segment to be processed, at node {}",
            num_segments_to_prune,
            ctx.get_cluster().local_id,
        );

        let dal = self.operator.clone();

        type CacheItem = (PartStatistics, Partitions);

        let derterministic_cache_key =
            push_downs
                .as_ref()
                .filter(|p| p.is_deterministic)
                .map(|push_downs| {
                    format!(
                        "{:x}",
                        Sha256::digest(format!("{:?}_{:?}", segments_location, push_downs))
                    )
                });

        if let Some(cached_result) = Self::check_prune_cache(&derterministic_cache_key) {
            info!("Retrieved snapshot block pruning result from cache");
            return Ok(cached_result);
        }

        let mut pruner =
            self.build_fuse_pruner(ctx.clone(), push_downs.clone(), table_schema.clone(), dal)?;

        let block_metas = pruner.read_pruning(segments_location).await?;
        let pruning_stats = pruner.pruning_stats();

        info!(
            "prune snapshot block end, final block numbers:{}, out of {} segments, cost:{:?}, at node {}",
            block_metas.len(),
            num_segments_to_prune,
            start.elapsed(),
            ctx.get_cluster().local_id,
        );

        let block_metas = block_metas
            .into_iter()
            .map(|(block_meta_index, block_meta)| (Some(block_meta_index), block_meta))
            .collect::<Vec<_>>();

        let schema = self.schema_with_stream();
        let result = self.read_partitions_with_metas(
            ctx.clone(),
            schema,
            push_downs,
            &block_metas,
            summary,
            pruning_stats,
        )?;

        if let Some(cache_key) = derterministic_cache_key {
            if let Some(cache) = CacheItem::cache() {
                cache.insert(cache_key, result.clone());
            }
        }
        Ok(result)
    }

    pub fn prune_segments_with_pipeline(
        &self,
        pruner: Arc<FusePruner>,
        prune_pipeline: &mut Pipeline,
        ctx: Arc<dyn TableContext>,
        segment_rx: Receiver<SegmentLocation>,
        part_info_tx: Sender<Result<PartInfoPtr>>,
        derterministic_cache_key: Option<String>,
        partitions_total: usize,
        plan_id: u32,
    ) -> Result<()> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        prune_pipeline.add_source(
            |output| LazySegmentReceiverSource::create(ctx.clone(), segment_rx.clone(), output),
            max_threads,
        )?;
        let segment_pruner = SegmentPruner::create(
            pruner.pruning_ctx.clone(),
            pruner.table_schema.clone(),
            Default::default(),
        )?;

        prune_pipeline.add_transform(|input, output| {
            SegmentPruneTransform::<PrunedCompactSegmentMeta>::create(
                input,
                output,
                segment_pruner.clone(),
                pruner.pruning_ctx.clone(),
            )
        })?;

        prune_pipeline
            .add_transform(|input, output| ExtractSegmentTransform::create(input, output, true))?;
        let sample_probability = table_sample(&pruner.push_down)?;
        if let Some(probability) = sample_probability {
            prune_pipeline.add_transform(|input, output| {
                SampleBlockMetasTransform::create(input, output, probability)
            })?;
        }
        let block_pruner = Arc::new(BlockPruner::create(pruner.pruning_ctx.clone())?);
        if pruner.pruning_ctx.bloom_pruner.is_some()
            || pruner.pruning_ctx.inverted_index_pruner.is_some()
            || pruner.pruning_ctx.virtual_column_pruner.is_some()
        {
            // async pruning with bloom index or inverted index.
            prune_pipeline.add_transform(|input, output| {
                AsyncBlockPruneTransform::create(input, output, block_pruner.clone())
            })?;
        } else {
            // sync pruning without a bloom index and inverted index.
            prune_pipeline.add_transform(|input, output| {
                SyncBlockPruneTransform::create(input, output, block_pruner.clone())
            })?;
        }

        let push_down = pruner.push_down.clone();
        if push_down
            .as_ref()
            .filter(|p| {
                (!p.order_by.is_empty() && p.limit.is_some() && p.filters.is_none())
                    || (p.limit.is_some() && p.filter_only_use_index())
            })
            .is_some()
        {
            // if there are ordering + limit clause and no filter, use topn pruner
            let schema = pruner.table_schema.clone();
            let push_down = push_down.as_ref().unwrap();
            let limit = push_down.limit.unwrap();
            let sort = push_down.order_by.clone();
            let filter_only_use_index = push_down.filter_only_use_index();
            let topn_pruner = TopNPruner::create(schema, sort, limit, filter_only_use_index);
            let pruning_ctx = pruner.pruning_ctx.clone();
            prune_pipeline.resize(1, false)?;
            prune_pipeline.add_transform(move |input, output| {
                TopNPruneTransform::create(input, output, pruning_ctx.clone(), topn_pruner.clone())
            })?;
        }

        if push_down
            .as_ref()
            .filter(|p| p.vector_index.is_some())
            .is_some()
        {
            let pruning_ctx = pruner.pruning_ctx.clone();
            let schema = pruner.table_schema.clone();
            let push_down = push_down.as_ref().unwrap();
            let filters = push_down.filters.clone();
            let sort = push_down.order_by.clone();
            let limit = push_down.limit;
            let vector_index = push_down.vector_index.clone().unwrap();

            let vector_index_pruner =
                VectorIndexPruner::create(pruning_ctx, schema, vector_index, filters, sort, limit)?;
            prune_pipeline.resize(1, false)?;
            prune_pipeline.add_transform(move |input, output| {
                VectorIndexPruneTransform::create(input, output, vector_index_pruner.clone())
            })?;
        }

        let top_k = push_down
            .as_ref()
            .filter(|_| self.is_native()) // Only native format supports topk push down.
            .and_then(|p| p.top_k(self.schema().as_ref()))
            .map(|topk| {
                DefaultExprBinder::try_new(ctx.clone())?
                    .get_scalar(&topk.field)
                    .map(|d| (topk, d))
            })
            .transpose()?;

        let limit = push_down
            .as_ref()
            .filter(|p| p.order_by.is_empty() && p.filters.is_none())
            .and_then(|p| p.limit);
        let enable_prune_cache = ctx.get_settings().get_enable_prune_cache()?;
        let send_part_state = Arc::new(SendPartState::create(
            derterministic_cache_key,
            limit,
            pruner.clone(),
            self.data_metrics.clone(),
            partitions_total,
        ));
        prune_pipeline.add_sink(|input| {
            SendPartInfoSink::create(
                input,
                part_info_tx.clone(),
                push_down.clone(),
                top_k.clone(),
                pruner.table_schema.clone(),
                send_part_state.clone(),
                enable_prune_cache,
            )
        })?;

        prune_pipeline.set_on_finished(move |info: &ExecutionInfo| {
            if let Ok(()) = info.res {
                // only populating cache when the pipeline is finished successfully
                let pruned_part_stats = send_part_state.get_pruned_stats();
                ctx.set_pruned_partitions_stats(plan_id, pruned_part_stats);
                if enable_prune_cache {
                    info!("Prune cache enabled");
                    send_part_state.populating_cache();
                }
            }
            Ok(())
        });

        Ok(())
    }

    pub fn prune_column_oriented_segments_with_pipeline(
        &self,
        pruner: Arc<FusePruner>,
        prune_pipeline: &mut Pipeline,
        ctx: Arc<dyn TableContext>,
        segment_rx: Receiver<SegmentLocation>,
        part_info_tx: Sender<Result<PartInfoPtr>>,
        _derterministic_cache_key: Option<String>,
        table_schema: TableSchemaRef,
    ) -> Result<()> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let push_down = &pruner.push_down;
        let block_pruner = Arc::new(BlockPruner::create(pruner.pruning_ctx.clone())?);

        // Only the columns that are used in the push down will be read, cached and passed to the next pipeline.
        let projection_column_ids = {
            let arrow_schema = self.schema().as_ref().into();
            let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema()));
            let column_nodes = match push_down.as_ref().and_then(|p| p.projection.as_ref()) {
                Some(projection) => {
                    match push_down.as_ref().and_then(|p| p.output_columns.as_ref()) {
                        Some(output_columns) => {
                            output_columns.project_column_nodes(&column_nodes)?
                        }
                        None => projection.project_column_nodes(&column_nodes)?,
                    }
                }
                None => column_nodes.column_nodes.iter().collect(),
            };
            column_nodes
                .iter()
                .flat_map(|c| c.leaf_column_ids.clone())
                .collect::<Vec<_>>()
        };
        let filter_column_ids = match push_down.as_ref().and_then(|p| p.filters.as_ref()) {
            Some(filters) => {
                let mut column_ids = HashSet::new();
                let filter = &filters.filter.as_expr(&BUILTIN_FUNCTIONS);
                let column_refs = filter.column_refs();
                for (column_name, _) in column_refs {
                    let field = table_schema.field_with_name(&column_name)?;
                    for column_id in field.leaf_column_ids() {
                        column_ids.insert(column_id);
                    }
                }
                column_ids
            }
            None => HashSet::new(),
        };

        let mut segment_column_projection = HashSet::new();
        for column_id in projection_column_ids.iter() {
            segment_column_projection.insert(meta_name(*column_id));
        }
        for column_id in filter_column_ids {
            segment_column_projection.insert(stat_name(column_id));
        }
        segment_column_projection.insert(ROW_COUNT.to_string());
        segment_column_projection.insert(BLOCK_SIZE.to_string());
        segment_column_projection.insert(FILE_SIZE.to_string());
        segment_column_projection.insert(CLUSTER_STATS.to_string());
        segment_column_projection.insert(LOCATION.to_string());
        segment_column_projection.insert(BLOOM_FILTER_INDEX_LOCATION.to_string());
        segment_column_projection.insert(BLOOM_FILTER_INDEX_SIZE.to_string());
        segment_column_projection.insert(NGRAM_FILTER_INDEX_SIZE.to_string());
        segment_column_projection.insert(INVERTED_INDEX_SIZE.to_string());
        segment_column_projection.insert(COMPRESSION.to_string());
        segment_column_projection.insert(CREATE_ON.to_string());
        let segment_pruner = SegmentPruner::create(
            pruner.pruning_ctx.clone(),
            pruner.table_schema.clone(),
            segment_column_projection.clone(),
        )?;

        prune_pipeline.add_source(
            |output| LazySegmentReceiverSource::create(ctx.clone(), segment_rx.clone(), output),
            max_threads,
        )?;
        prune_pipeline.add_transform(|input, output| {
            SegmentPruneTransform::<PrunedColumnOrientedSegmentMeta>::create(
                input,
                output,
                segment_pruner.clone(),
                pruner.pruning_ctx.clone(),
            )
        })?;
        // TODO(Sky): deal with sample
        prune_pipeline.add_sink(|input| {
            ColumnOrientedBlockPruneSink::create(
                input,
                block_pruner.clone(),
                part_info_tx.clone(),
                projection_column_ids.clone(),
            )
        })?;
        // TODO(Sky): populate prune cache , deal with topn prune
        Ok(())
    }

    pub fn build_fuse_pruner(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        table_schema: TableSchemaRef,
        dal: Operator,
    ) -> Result<FusePruner> {
        let ngram_args =
            Self::create_ngram_index_args(&self.table_info.meta.indexes, &self.schema(), false)?;
        let bloom_index_builder = if ctx
            .get_settings()
            .get_enable_auto_fix_missing_bloom_index()?
        {
            let storage_format = self.storage_format;

            let bloom_columns_map = self
                .bloom_index_cols()
                .bloom_index_fields(table_schema.clone(), BloomIndex::supported_type)?;

            Some(BloomIndexRebuilder {
                table_ctx: ctx.clone(),
                table_schema: table_schema.clone(),
                table_dal: dal.clone(),
                storage_format,
                bloom_columns_map,
                ngram_args: ngram_args.clone(),
            })
        } else {
            None
        };

        let pruner =
            if !self.is_native() || self.cluster_type().is_none_or(|v| v != ClusterType::Linear) {
                FusePruner::create(
                    &ctx,
                    dal,
                    table_schema.clone(),
                    &push_downs,
                    self.bloom_index_cols(),
                    ngram_args,
                    bloom_index_builder,
                )?
            } else {
                let cluster_keys = self.linear_cluster_keys(ctx.clone());

                FusePruner::create_with_pages(
                    &ctx,
                    dal,
                    table_schema,
                    &push_downs,
                    self.cluster_key_meta(),
                    cluster_keys,
                    self.bloom_index_cols(),
                    ngram_args,
                    bloom_index_builder,
                )?
            };
        Ok(pruner)
    }

    pub fn create_ngram_index_args(
        indexes: &BTreeMap<String, TableIndex>,
        table_schema: &TableSchema,
        is_sync_write: bool,
    ) -> Result<Vec<NgramArgs>> {
        let mut ngram_index_args = Vec::with_capacity(indexes.len());
        for index in indexes.values() {
            if !matches!(index.index_type, TableIndexType::Ngram) {
                continue;
            }
            if is_sync_write && !index.sync_creation {
                continue;
            }

            let gram_size = match index.options.get("gram_size") {
                None => DEFAULT_GRAM_SIZE,
                Some(s) => s.parse::<usize>()?,
            };
            let bloom_size = match index.options.get("bloom_size") {
                None => DEFAULT_BLOOM_SIZE,
                Some(s) => s.parse::<u64>()?,
            };

            for column_id in &index.column_ids {
                let Some((pos, field)) = table_schema
                    .fields()
                    .iter()
                    .find_position(|field| field.column_id() == *column_id)
                else {
                    continue;
                };
                ngram_index_args.push(NgramArgs::new(pos, field.clone(), gram_size, bloom_size));
            }
        }
        Ok(ngram_index_args)
    }

    pub fn check_prune_cache(
        derterministic_cache_key: &Option<String>,
    ) -> Option<(PartStatistics, Partitions)> {
        type CacheItem = (PartStatistics, Partitions);

        if let Some(cache_key) = derterministic_cache_key.as_ref() {
            if let Some(cache) = CacheItem::cache() {
                if let Some(data) = cache.get(cache_key) {
                    return Some((data.0.clone(), data.1.clone()));
                }
            }
        }
        None
    }

    pub fn read_partitions_with_metas(
        &self,
        ctx: Arc<dyn TableContext>,
        schema: TableSchemaRef,
        push_downs: Option<PushDownInfo>,
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        partitions_total: usize,
        pruning_stats: PruningStatistics,
    ) -> Result<(PartStatistics, Partitions)> {
        let arrow_schema = schema.as_ref().into();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&schema));

        let partitions_scanned = block_metas.len();

        let top_k = push_downs
            .as_ref()
            .filter(|_| self.is_native()) // Only native format supports topk push down.
            .and_then(|p| p.top_k(self.schema().as_ref()))
            .map(|topk| {
                DefaultExprBinder::try_new(ctx.clone())?
                    .get_scalar(&topk.field)
                    .map(|d| (topk, d))
            })
            .transpose()?;

        let (mut statistics, parts) =
            Self::to_partitions(Some(&schema), block_metas, &column_nodes, top_k, push_downs);

        // Update planner statistics.
        statistics.partitions_total = partitions_total;
        statistics.partitions_scanned = partitions_scanned;
        statistics.pruning_stats = pruning_stats;

        // Update context statistics.
        self.data_metrics
            .inc_partitions_total(partitions_total as u64);
        self.data_metrics
            .inc_partitions_scanned(partitions_scanned as u64);

        Ok((statistics, parts))
    }

    pub fn to_partitions(
        schema: Option<&TableSchemaRef>,
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        column_nodes: &ColumnNodes,
        top_k: Option<(TopK, Scalar)>,
        push_downs: Option<PushDownInfo>,
    ) -> (PartStatistics, Partitions) {
        let limit = push_downs
            .as_ref()
            .filter(|p| p.order_by.is_empty() && p.filters.is_none())
            .and_then(|p| p.limit)
            .unwrap_or(usize::MAX);

        let mut block_metas = block_metas.to_vec();
        if let Some((top_k, default)) = &top_k {
            let default_stats = ColumnStatistics {
                min: default.clone(),
                max: default.clone(),
                // These fields will not be used in the following sorting.
                null_count: 0,
                in_memory_size: 0,
                distinct_of_values: None,
            };
            if top_k.asc {
                block_metas.sort_by(|a, b| {
                    let a =
                        a.1.col_stats
                            .get(&top_k.field.column_id)
                            .unwrap_or(&default_stats);
                    let b =
                        b.1.col_stats
                            .get(&top_k.field.column_id)
                            .unwrap_or(&default_stats);
                    (a.min().as_ref(), a.max().as_ref()).cmp(&(b.min().as_ref(), b.max().as_ref()))
                });
            } else {
                block_metas.sort_by(|a, b| {
                    let a =
                        a.1.col_stats
                            .get(&top_k.field.column_id)
                            .unwrap_or(&default_stats);
                    let b =
                        b.1.col_stats
                            .get(&top_k.field.column_id)
                            .unwrap_or(&default_stats);
                    (b.max().as_ref(), b.min().as_ref()).cmp(&(a.max().as_ref(), a.min().as_ref()))
                });
            }
        }

        let (mut statistics, mut partitions) = match &push_downs {
            None => Self::all_columns_partitions(schema, &block_metas, top_k.clone(), limit),
            Some(extras) => match &extras.projection {
                None => Self::all_columns_partitions(schema, &block_metas, top_k.clone(), limit),
                Some(projection) => Self::projection_partitions(
                    &block_metas,
                    column_nodes,
                    projection,
                    &extras.output_columns,
                    &extras.virtual_column,
                    top_k.clone(),
                    limit,
                ),
            },
        };

        if top_k.is_some() {
            partitions.kind = PartitionsShuffleKind::Seq;
        }

        statistics.is_exact = statistics.is_exact && Self::is_exact(&push_downs);
        (statistics, partitions)
    }

    fn is_exact(push_downs: &Option<PushDownInfo>) -> bool {
        push_downs
            .as_ref()
            .is_none_or(|extra| extra.filters.is_none())
    }

    fn all_columns_partitions(
        schema: Option<&TableSchemaRef>,
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        top_k: Option<(TopK, Scalar)>,
        limit: usize,
    ) -> (PartStatistics, Partitions) {
        let mut statistics = PartStatistics::default_exact();
        let mut partitions = Partitions::create(PartitionsShuffleKind::Mod, vec![]);

        if limit == 0 {
            return (statistics, partitions);
        }

        let mut remaining = limit;
        for (block_meta_index, block_meta) in block_metas.iter() {
            let rows = block_meta.row_count as usize;
            partitions.partitions.push(Self::all_columns_part(
                schema,
                block_meta_index,
                &top_k,
                block_meta,
            ));
            statistics.read_rows += rows;
            statistics.read_bytes += block_meta.block_size as usize;

            if remaining > rows {
                remaining -= rows;
            } else {
                // the last block we shall take
                if remaining != rows {
                    statistics.is_exact = false;
                }
                break;
            }
        }

        (statistics, partitions)
    }

    fn projection_partitions(
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        column_nodes: &ColumnNodes,
        projection: &Projection,
        output_columns: &Option<Projection>,
        virtual_column: &Option<VirtualColumnInfo>,
        top_k: Option<(TopK, Scalar)>,
        limit: usize,
    ) -> (PartStatistics, Partitions) {
        let mut statistics = PartStatistics::default_exact();
        let mut partitions = Partitions::default();

        if limit == 0 {
            return (statistics, partitions);
        }

        // Output columns don't have source columns of virtual columns,
        // which can be ignored if all virtual columns are generated.
        let columns = if let Some(output_columns) = output_columns {
            output_columns.project_column_nodes(column_nodes).unwrap()
        } else {
            projection.project_column_nodes(column_nodes).unwrap()
        };
        let mut remaining = limit;

        for (block_meta_index, block_meta) in block_metas.iter() {
            partitions.partitions.push(Self::projection_part(
                block_meta,
                block_meta_index,
                column_nodes,
                top_k.clone(),
                projection,
            ));

            let rows = block_meta.row_count as usize;

            statistics.read_rows += rows;
            for column in &columns {
                for column_id in &column.leaf_column_ids {
                    // ignore all deleted field
                    if let Some(col_metas) = &block_meta.col_metas.get(column_id) {
                        let (_, len) = col_metas.offset_length();
                        statistics.read_bytes += len as usize;
                    }
                }
            }

            let virtual_block_meta = if let Some(block_meta_index) = block_meta_index {
                &block_meta_index.virtual_block_meta
            } else {
                &None
            };
            if let Some(virtual_column) = virtual_column {
                if let Some(virtual_block_meta) = virtual_block_meta {
                    // Add bytes of virtual columns
                    for virtual_column_meta in virtual_block_meta.virtual_column_metas.values() {
                        let (_, len) = virtual_column_meta.offset_length();
                        statistics.read_bytes += len as usize;
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
                            statistics.read_bytes += len as usize;
                        }
                    }
                } else {
                    // If virtual column meta not exist, all source columns are needed.
                    for source_column_id in &virtual_column.source_column_ids {
                        if let Some(col_metas) = &block_meta.col_metas.get(source_column_id) {
                            let (_, len) = col_metas.offset_length();
                            statistics.read_bytes += len as usize;
                        }
                    }
                }
            }

            if remaining > rows {
                remaining -= rows;
            } else {
                // the last block we shall take
                if remaining != rows {
                    statistics.is_exact = false;
                }
                break;
            }
        }
        (statistics, partitions)
    }

    pub fn all_columns_part(
        schema: Option<&TableSchemaRef>,
        block_meta_index: &Option<BlockMetaIndex>,
        top_k: &Option<(TopK, Scalar)>,
        meta: &BlockMeta,
    ) -> PartInfoPtr {
        let mut columns_meta = HashMap::with_capacity(meta.col_metas.len());
        let mut columns_stats = HashMap::with_capacity(meta.col_stats.len());

        for column_id in meta.col_metas.keys() {
            // ignore all deleted field
            if let Some(schema) = schema {
                if schema.is_column_deleted(*column_id) {
                    continue;
                }
            }

            // ignore column this block dose not exist
            if let Some(meta) = meta.col_metas.get(column_id) {
                columns_meta.insert(*column_id, meta.clone());
            }

            if let Some(stat) = meta.col_stats.get(column_id) {
                columns_stats.insert(*column_id, stat.clone());
            }
        }

        let rows_count = meta.row_count;
        let location = meta.location.0.clone();
        let create_on = meta.create_on;

        let sort_min_max = top_k.as_ref().map(|(top_k, default)| {
            meta.col_stats
                .get(&top_k.field.column_id)
                .map(|stat| (stat.min().clone(), stat.max().clone()))
                .unwrap_or((default.clone(), default.clone()))
        });

        FuseBlockPartInfo::create(
            location,
            rows_count,
            columns_meta,
            Some(columns_stats),
            meta.compression(),
            sort_min_max,
            block_meta_index.to_owned(),
            create_on,
        )
    }

    pub fn projection_part(
        meta: &BlockMeta,
        block_meta_index: &Option<BlockMetaIndex>,
        column_nodes: &ColumnNodes,
        top_k: Option<(TopK, Scalar)>,
        projection: &Projection,
    ) -> PartInfoPtr {
        let mut columns_meta = HashMap::with_capacity(projection.len());
        let mut columns_stat = HashMap::with_capacity(projection.len());

        let columns = projection.project_column_nodes(column_nodes).unwrap();
        for column in &columns {
            for column_id in &column.leaf_column_ids {
                // ignore column this block dose not exist
                if let Some(column_meta) = meta.col_metas.get(column_id) {
                    columns_meta.insert(*column_id, column_meta.clone());
                }
                if let Some(column_stat) = meta.col_stats.get(column_id) {
                    columns_stat.insert(*column_id, column_stat.clone());
                }
            }
        }

        let rows_count = meta.row_count;
        let location = meta.location.0.clone();
        let create_on = meta.create_on;

        let sort_min_max = top_k.map(|(top_k, default)| {
            let stat = meta.col_stats.get(&top_k.field.column_id);
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
            meta.compression(),
            sort_min_max,
            block_meta_index.to_owned(),
            create_on,
        )
    }
}
