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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::SEGMENT_NAME_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::field_default_value;
use databend_common_sql::BloomIndexColumns;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_pruner::InternalColumnPruner;
use databend_storages_common_pruner::Limiter;
use databend_storages_common_pruner::LimiterPrunerCreator;
use databend_storages_common_pruner::PagePruner;
use databend_storages_common_pruner::PagePrunerCreator;
use databend_storages_common_pruner::RangePruner;
use databend_storages_common_pruner::RangePrunerCreator;
use databend_storages_common_pruner::TopNPrunner;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use log::warn;
use opendal::Operator;

use crate::io::BloomIndexBuilder;
use crate::operations::DeletedSegmentInfo;
use crate::pruning::segment_pruner::SegmentPruner;
use crate::pruning::BlockPruner;
use crate::pruning::BloomPruner;
use crate::pruning::BloomPrunerCreator;
use crate::pruning::FusePruningStatistics;
use crate::pruning::InvertedIndexPruner;
use crate::pruning::SegmentLocation;

#[derive(Clone)]
pub struct PruningContext {
    pub ctx: Arc<dyn TableContext>,
    pub dal: Operator,
    pub pruning_runtime: Arc<Runtime>,
    pub pruning_semaphore: Arc<Semaphore>,

    pub limit_pruner: Arc<dyn Limiter + Send + Sync>,
    pub range_pruner: Arc<dyn RangePruner + Send + Sync>,
    pub bloom_pruner: Option<Arc<dyn BloomPruner + Send + Sync>>,
    pub page_pruner: Arc<dyn PagePruner + Send + Sync>,
    pub internal_column_pruner: Option<Arc<InternalColumnPruner>>,
    pub inverted_index_pruner: Option<Arc<InvertedIndexPruner>>,

    pub pruning_stats: Arc<FusePruningStatistics>,
}

impl PruningContext {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        ctx: &Arc<dyn TableContext>,
        dal: Operator,
        table_schema: TableSchemaRef,
        push_down: &Option<PushDownInfo>,
        cluster_key_meta: Option<ClusterKey>,
        cluster_keys: Vec<RemoteExpr<String>>,
        bloom_index_cols: BloomIndexColumns,
        max_concurrency: usize,
        bloom_index_builder: Option<BloomIndexBuilder>,
    ) -> Result<Arc<PruningContext>> {
        let func_ctx = ctx.get_function_context()?;

        let filter_expr = push_down.as_ref().and_then(|extra| {
            extra
                .filters
                .as_ref()
                .map(|f| f.filter.as_expr(&BUILTIN_FUNCTIONS))
        });

        // Limit pruner.
        // if there are ordering/filter clause, ignore limit, even it has been pushed down
        let limit = push_down
            .as_ref()
            .filter(|p| p.order_by.is_empty() && p.filters.is_none())
            .and_then(|p| p.limit);
        // prepare the limiter. in case that limit is none, an unlimited limiter will be returned
        let limit_pruner = LimiterPrunerCreator::create(limit);

        let default_stats: StatisticsOfColumns = filter_expr
            .as_ref()
            .map(|f| f.column_refs())
            .into_iter()
            .flatten()
            .filter_map(|(name, _)| {
                let field = table_schema.field_with_name(&name).ok()?;
                let default_scalar = field_default_value(ctx.clone(), field).ok()?;

                let stats =
                    ColumnStatistics::new(default_scalar.clone(), default_scalar, 0, 0, Some(1));
                Some((field.column_id(), stats))
            })
            .collect();

        // Range filter.
        // if filter_expression is none, an dummy pruner will be returned, which prunes nothing
        let range_pruner = RangePrunerCreator::try_create_with_default_stats(
            func_ctx.clone(),
            &table_schema,
            filter_expr.as_ref(),
            default_stats,
        )?;

        // Bloom pruner.
        // None will be returned, if filter is not applicable (e.g. unsuitable filter expression, index not available, etc.)
        let bloom_pruner = BloomPrunerCreator::create(
            func_ctx.clone(),
            &table_schema,
            dal.clone(),
            filter_expr.as_ref(),
            bloom_index_cols,
            bloom_index_builder,
        )?;

        // Page pruner, used in native format
        let page_pruner = PagePrunerCreator::try_create(
            func_ctx.clone(),
            &table_schema,
            filter_expr.as_ref(),
            cluster_key_meta,
            cluster_keys,
        )?;

        // inverted index pruner, used to search matched rows in block
        let inverted_index_pruner = InvertedIndexPruner::try_create(dal.clone(), push_down)?;

        // Internal column pruner, if there are predicates using internal columns,
        // we can use them to prune segments and blocks.
        let internal_column_pruner =
            InternalColumnPruner::try_create(func_ctx, filter_expr.as_ref());

        // Constraint the degree of parallelism
        let max_threads = ctx.get_settings().get_max_threads()? as usize;

        // Pruning runtime.
        let pruning_runtime = Arc::new(Runtime::with_worker_threads(
            max_threads,
            Some("pruning-worker".to_owned()),
        )?);
        let pruning_semaphore = Arc::new(Semaphore::new(max_concurrency));
        let pruning_stats = Arc::new(FusePruningStatistics::default());

        let pruning_ctx = Arc::new(PruningContext {
            ctx: ctx.clone(),
            dal,
            pruning_runtime,
            pruning_semaphore,
            limit_pruner,
            range_pruner,
            bloom_pruner,
            page_pruner,
            internal_column_pruner,
            inverted_index_pruner,
            pruning_stats,
        });
        Ok(pruning_ctx)
    }
}

pub struct FusePruner {
    max_concurrency: usize,
    pub table_schema: TableSchemaRef,
    pub pruning_ctx: Arc<PruningContext>,
    pub push_down: Option<PushDownInfo>,
    pub inverse_range_index: Option<RangeIndex>,
    pub deleted_segments: Vec<DeletedSegmentInfo>,
}

impl FusePruner {
    // Create normal fuse pruner.
    pub fn create(
        ctx: &Arc<dyn TableContext>,
        dal: Operator,
        table_schema: TableSchemaRef,
        push_down: &Option<PushDownInfo>,
        bloom_index_cols: BloomIndexColumns,
        bloom_index_builder: Option<BloomIndexBuilder>,
    ) -> Result<Self> {
        Self::create_with_pages(
            ctx,
            dal,
            table_schema,
            push_down,
            None,
            vec![],
            bloom_index_cols,
            bloom_index_builder,
        )
    }

    // Create fuse pruner with pages.
    pub fn create_with_pages(
        ctx: &Arc<dyn TableContext>,
        dal: Operator,
        table_schema: TableSchemaRef,
        push_down: &Option<PushDownInfo>,
        cluster_key_meta: Option<ClusterKey>,
        cluster_keys: Vec<RemoteExpr<String>>,
        bloom_index_cols: BloomIndexColumns,
        bloom_index_builder: Option<BloomIndexBuilder>,
    ) -> Result<Self> {
        let max_concurrency = {
            let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;
            // Prevent us from miss-configured max_storage_io_requests setting, e.g. 0
            let v = std::cmp::max(max_io_requests, 10);
            if v > max_io_requests {
                warn!(
                    "max_storage_io_requests setting is too low {}, increased to {}",
                    max_io_requests, v
                )
            }
            v
        };

        let pruning_ctx = PruningContext::try_create(
            ctx,
            dal,
            table_schema.clone(),
            push_down,
            cluster_key_meta,
            cluster_keys,
            bloom_index_cols,
            max_concurrency,
            bloom_index_builder,
        )?;

        Ok(FusePruner {
            max_concurrency,
            table_schema,
            push_down: push_down.clone(),
            pruning_ctx,
            inverse_range_index: None,
            deleted_segments: vec![],
        })
    }

    #[async_backtrace::framed]
    pub async fn read_pruning(
        &mut self,
        segment_locs: Vec<SegmentLocation>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        self.pruning(segment_locs, false).await
    }

    #[async_backtrace::framed]
    pub async fn delete_pruning(
        &mut self,
        segment_locs: Vec<SegmentLocation>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        self.pruning(segment_locs, true).await
    }

    // Pruning chain:
    // segment pruner -> block pruner -> topn pruner
    #[async_backtrace::framed]
    pub async fn pruning(
        &mut self,
        mut segment_locs: Vec<SegmentLocation>,
        delete_pruning: bool,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        // Segment pruner.
        let segment_pruner =
            SegmentPruner::create(self.pruning_ctx.clone(), self.table_schema.clone())?;
        let block_pruner = Arc::new(BlockPruner::create(self.pruning_ctx.clone())?);

        let mut remain = segment_locs.len() % self.max_concurrency;
        let batch_size = segment_locs.len() / self.max_concurrency;
        let mut works = Vec::with_capacity(self.max_concurrency);

        let enable_bloom_filter_ignore_invalid_key = self
            .pruning_ctx
            .ctx
            .get_function_context()?
            .enable_bloom_filter_ignore_invalid_key;

        let inverse_range_index = self.get_inverse_range_index();
        if !enable_bloom_filter_ignore_invalid_key {
            while !segment_locs.is_empty() {
                let gap_size = std::cmp::min(1, remain);
                let batch_size = batch_size + gap_size;
                remain -= gap_size;

                let batch = segment_locs.drain(0..batch_size).collect::<Vec<_>>();
                works.push(self.pruning_ctx.pruning_runtime.spawn({
                    let block_pruner = block_pruner.clone();
                    let segment_pruner = segment_pruner.clone();
                    let pruning_ctx = self.pruning_ctx.clone();
                    let inverse_range_index = inverse_range_index.clone();

                    async move {
                        let (segment_block_metas, deleted_segments) = Self::segment_pruning(
                            pruning_ctx,
                            segment_pruner,
                            inverse_range_index,
                            batch,
                            delete_pruning,
                        )
                        .await?;

                        let (res, _) =
                            Self::block_pruning(block_pruner, segment_block_metas, false).await?;

                        Result::<_, ErrorCode>::Ok((res, deleted_segments))
                    }
                }));
            }
        } else {
            let mut segment_works = Vec::with_capacity(self.max_concurrency);
            while !segment_locs.is_empty() {
                let gap_size = std::cmp::min(1, remain);
                let batch_size = batch_size + gap_size;
                remain -= gap_size;

                let batch = segment_locs.drain(0..batch_size).collect::<Vec<_>>();
                segment_works.push(self.pruning_ctx.pruning_runtime.spawn({
                    let segment_pruner = segment_pruner.clone();
                    let pruning_ctx = self.pruning_ctx.clone();
                    let inverse_range_index = inverse_range_index.clone();

                    async move {
                        let (segment_block_metas, deleted_segments) = Self::segment_pruning(
                            pruning_ctx,
                            segment_pruner,
                            inverse_range_index,
                            batch,
                            delete_pruning,
                        )
                        .await?;
                        Result::<_, ErrorCode>::Ok((segment_block_metas, deleted_segments))
                    }
                }));
            }

            let mut segment_block_metas = vec![];
            let mut deleted_segments = vec![];
            match futures::future::try_join_all(segment_works).await {
                Err(e) => {
                    return Err(ErrorCode::StorageOther(format!(
                        "segment pruning failure, {}",
                        e
                    )));
                }
                Ok(workers) => {
                    for worker in workers {
                        let res = worker?;
                        segment_block_metas.extend(res.0);
                        deleted_segments.extend(res.1);
                    }
                }
            }

            let block_count: usize = segment_block_metas
                .iter()
                .map(|(_, block_metas)| block_metas.len())
                .sum();

            let sample_num = if block_count > 100 {
                std::cmp::min(block_count / 10, 15)
            } else {
                0
            };

            let (sample_block_metas, mut remainder_block_metas) =
                Self::split_sample_block_metas(sample_num, segment_block_metas);

            let (sample_result_block_metas, invalid_keys_map) =
                Self::block_pruning(block_pruner.clone(), sample_block_metas, true).await?;

            let ratio = 0.7;
            let mut ignored_keys = HashSet::new();
            if let Some(invalid_keys_map) = invalid_keys_map {
                for (invalid_key, invalid_num) in invalid_keys_map.into_iter() {
                    // invalid_num is the number of bloom filters that return Uncertain.
                    // If the ratio of these invalid filters to all blocks exceeds the threshold set by the user,
                    // we put the filter key into the cache and the filter key is ignored in following queries.
                    let invalid_ratio = invalid_num as f32 / sample_num as f32;
                    if invalid_ratio > ratio {
                        ignored_keys.insert(invalid_key);
                    }
                }
            }

            let pruning_ctx = if ignored_keys.is_empty() {
                self.pruning_ctx.clone()
            } else {
                let mut pruning_ctx = Arc::unwrap_or_clone(self.pruning_ctx.clone());
                let bloom_pruner = match &pruning_ctx.bloom_pruner {
                    Some(bloom_pruner) => bloom_pruner.update_index_fields(&ignored_keys),
                    None => None,
                };
                pruning_ctx.bloom_pruner = bloom_pruner;
                Arc::new(pruning_ctx)
            };

            let block_pruner = Arc::new(BlockPruner::create(pruning_ctx.clone())?);

            works.push(
                pruning_ctx.pruning_runtime.spawn({
                    async move {
                        Result::<_, ErrorCode>::Ok((sample_result_block_metas, deleted_segments))
                    }
                }),
            );

            let mut remain = remainder_block_metas.len() % self.max_concurrency;
            let batch_size = remainder_block_metas.len() / self.max_concurrency;

            while !remainder_block_metas.is_empty() {
                let gap_size = std::cmp::min(1, remain);
                let batch_size = batch_size + gap_size;
                remain -= gap_size;

                let batch = remainder_block_metas
                    .drain(0..batch_size)
                    .collect::<Vec<_>>();
                works.push(pruning_ctx.pruning_runtime.spawn({
                    let block_pruner = block_pruner.clone();

                    async move {
                        let (remainder_result_block_metas, _) =
                            Self::block_pruning(block_pruner, batch, false).await?;

                        Result::<_, ErrorCode>::Ok((remainder_result_block_metas, vec![]))
                    }
                }));
            }
        }

        match futures::future::try_join_all(works).await {
            Err(e) => Err(ErrorCode::StorageOther(format!(
                "segment pruning failure, {}",
                e
            ))),
            Ok(workers) => {
                let mut metas = vec![];
                for worker in workers {
                    let mut res = worker?;
                    metas.extend(res.0);
                    self.deleted_segments.append(&mut res.1);
                }
                if delete_pruning {
                    Ok(metas)
                } else {
                    // Todo:: for now, all operation (contains other mutation other than delete, like select,update etc.)
                    // will get here, we can prevent other mutations like update and so on.
                    // TopN pruner.
                    self.topn_pruning(metas)
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn segment_pruning(
        pruning_ctx: Arc<PruningContext>,
        segment_pruner: Arc<SegmentPruner>,
        inverse_range_index: Option<RangeIndex>,
        mut segment_locs: Vec<SegmentLocation>,
        delete_pruning: bool,
    ) -> Result<(
        Vec<(SegmentLocation, Vec<Arc<BlockMeta>>)>,
        Vec<DeletedSegmentInfo>,
    )> {
        // Build pruning tasks.
        if let Some(internal_column_pruner) = &pruning_ctx.internal_column_pruner {
            segment_locs = segment_locs
                .into_iter()
                .filter(|segment| {
                    internal_column_pruner.should_keep(SEGMENT_NAME_COL_NAME, &segment.location.0)
                })
                .collect::<Vec<_>>();
        }
        let pruned_segments = segment_pruner.pruning(segment_locs).await?;
        let mut pruned_block_metas = Vec::with_capacity(pruned_segments.len());
        let mut deleted_segments = vec![];
        if delete_pruning {
            // inverse prun
            for (location, segment_info) in pruned_segments.into_iter() {
                // for delete_prune
                if let Some(range_index) = inverse_range_index.as_ref() {
                    if !range_index.should_keep(&segment_info.summary.col_stats, None) {
                        deleted_segments.push(DeletedSegmentInfo {
                            index: location.segment_idx,
                            summary: segment_info.summary.clone(),
                        });
                        continue;
                    }
                }
                let block_metas = segment_info.block_metas()?;
                pruned_block_metas.push((location, block_metas));
            }
        } else {
            for (location, segment_info) in pruned_segments.into_iter() {
                let block_metas = segment_info.block_metas()?;
                pruned_block_metas.push((location, block_metas));
            }
        }
        Ok((pruned_block_metas, deleted_segments))
    }

    async fn block_pruning(
        block_pruner: Arc<BlockPruner>,
        segment_block_metas: Vec<(SegmentLocation, Vec<Arc<BlockMeta>>)>,
        is_sample: bool,
    ) -> Result<(
        Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
        Option<HashMap<String, usize>>,
    )> {
        let mut result_block_metas = vec![];
        let mut invalid_keys_map = if is_sample {
            Some(HashMap::new())
        } else {
            None
        };
        for (location, block_metas) in segment_block_metas {
            let res = block_pruner
                .pruning(location, block_metas, is_sample)
                .await?;
            result_block_metas.extend(res.0);
            if let Some(ref mut invalid_keys_map) = invalid_keys_map {
                if let Some(keys_map) = res.1 {
                    for (invalid_key, num) in keys_map.into_iter() {
                        let val_ref = invalid_keys_map.entry(invalid_key).or_insert(0);
                        *val_ref += num;
                    }
                }
            }
        }
        Ok((result_block_metas, invalid_keys_map))
    }

    #[allow(clippy::type_complexity)]
    fn split_sample_block_metas(
        mut sample_num: usize,
        segment_block_metas: Vec<(SegmentLocation, Vec<Arc<BlockMeta>>)>,
    ) -> (
        Vec<(SegmentLocation, Vec<Arc<BlockMeta>>)>,
        Vec<(SegmentLocation, Vec<Arc<BlockMeta>>)>,
    ) {
        if sample_num == 0 {
            return (vec![], segment_block_metas);
        }
        let mut sample_block_metas = vec![];
        let mut remainder_block_metas = vec![];
        for (segment_location, mut block_metas) in segment_block_metas.into_iter() {
            let block_num = block_metas.len();
            if sample_num >= block_num {
                sample_num -= block_num;
                sample_block_metas.push((segment_location, block_metas));
            } else if sample_num > 0 {
                let remain_block_metas = block_metas.split_off(sample_num);
                sample_num = 0;
                sample_block_metas.push((segment_location.clone(), block_metas));
                remainder_block_metas.push((segment_location, remain_block_metas));
            } else {
                remainder_block_metas.push((segment_location, block_metas));
            }
        }
        (sample_block_metas, remainder_block_metas)
    }

    #[async_backtrace::framed]
    pub async fn stream_pruning(
        &mut self,
        mut block_metas: Vec<Arc<BlockMeta>>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let mut remain = block_metas.len() % self.max_concurrency;
        let batch_size = block_metas.len() / self.max_concurrency;
        let mut works = Vec::with_capacity(self.max_concurrency);
        let block_pruner = Arc::new(BlockPruner::create(self.pruning_ctx.clone())?);
        let mut segment_idx = 0;

        while !block_metas.is_empty() {
            let gap_size = std::cmp::min(1, remain);
            let batch_size = batch_size + gap_size;
            remain -= gap_size;

            let batch = block_metas.drain(0..batch_size).collect::<Vec<_>>();
            works.push(self.pruning_ctx.pruning_runtime.spawn({
                let block_pruner = block_pruner.clone();
                async move {
                    // Build pruning tasks.
                    let res = block_pruner
                        .pruning(
                            // unused segment location.
                            SegmentLocation {
                                segment_idx,
                                location: ("".to_string(), 0),
                                snapshot_loc: None,
                            },
                            batch,
                            false,
                        )
                        .await?;

                    Result::<_, ErrorCode>::Ok(res.0)
                }
            }));
            segment_idx += 1;
        }

        match futures::future::try_join_all(works).await {
            Err(e) => Err(ErrorCode::StorageOther(format!(
                "segment pruning failure, {}",
                e
            ))),
            Ok(workers) => {
                let mut metas = vec![];
                for worker in workers {
                    let res = worker?;
                    metas.extend(res);
                }
                // Todo:: for now, all operation (contains other mutation other than delete, like select,update etc.)
                // will get here, we can prevent other mutations like update and so on.
                // TopN pruner.
                self.topn_pruning(metas)
            }
        }
    }

    // topn pruner:
    // if there are ordering + limit clause and no filters, use topn pruner
    fn topn_pruning(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let push_down = self.push_down.clone();
        if push_down
            .as_ref()
            .filter(|p| !p.order_by.is_empty() && p.limit.is_some() && p.filters.is_none())
            .is_some()
        {
            let schema = self.table_schema.clone();
            let push_down = push_down.as_ref().unwrap();
            let limit = push_down.limit.unwrap();
            let sort = push_down.order_by.clone();
            let topn_pruner = TopNPrunner::create(schema, sort, limit);
            return Ok(topn_pruner.prune(metas.clone()).unwrap_or(metas));
        }
        Ok(metas)
    }

    // Pruning stats.
    pub fn pruning_stats(&self) -> databend_common_catalog::plan::PruningStatistics {
        let stats = self.pruning_ctx.pruning_stats.clone();

        let segments_range_pruning_before = stats.get_segments_range_pruning_before() as usize;
        let segments_range_pruning_after = stats.get_segments_range_pruning_after() as usize;

        let blocks_range_pruning_before = stats.get_blocks_range_pruning_before() as usize;
        let blocks_range_pruning_after = stats.get_blocks_range_pruning_after() as usize;

        let blocks_bloom_pruning_before = stats.get_blocks_bloom_pruning_before() as usize;
        let blocks_bloom_pruning_after = stats.get_blocks_bloom_pruning_after() as usize;

        let blocks_inverted_index_pruning_before =
            stats.get_blocks_inverted_index_pruning_before() as usize;
        let blocks_inverted_index_pruning_after =
            stats.get_blocks_inverted_index_pruning_after() as usize;

        databend_common_catalog::plan::PruningStatistics {
            segments_range_pruning_before,
            segments_range_pruning_after,
            blocks_range_pruning_before,
            blocks_range_pruning_after,
            blocks_bloom_pruning_before,
            blocks_bloom_pruning_after,
            blocks_inverted_index_pruning_before,
            blocks_inverted_index_pruning_after,
        }
    }

    pub fn set_inverse_range_index(&mut self, index: RangeIndex) {
        self.inverse_range_index = Some(index)
    }

    pub fn get_inverse_range_index(&self) -> Option<RangeIndex> {
        self.inverse_range_index.clone()
    }
}
