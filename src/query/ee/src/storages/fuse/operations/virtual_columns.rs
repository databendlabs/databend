// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Instant;

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_nums;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::OneBlockSource;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::plans::RefreshSelection;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::BlockReader;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::io::VirtualColumnBuilder;
use databend_common_storages_fuse::io::WriteSettings;
use databend_common_storages_fuse::io::read::read_segment_stats;
use databend_common_storages_fuse::io::write_data;
use databend_common_storages_fuse::operations::BlockMetaIndex;
use databend_common_storages_fuse::operations::CommitSink;
use databend_common_storages_fuse::operations::MutationGenerator;
use databend_common_storages_fuse::operations::MutationLogEntry;
use databend_common_storages_fuse::operations::MutationLogs;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_enterprise_virtual_column::VirtualColumnRefreshResult;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::Table;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::Statistics;
use log::debug;
use log::info;
use opendal::Operator;

// Refresh virtual columns in two phases:
// 1) Prepare virtual column files for selected blocks (slow path, no commit).
// 2) Re-read the latest snapshot and commit updated block metas (fast path).

// Prepare is intentionally lock-free: it only writes virtual files and returns draft metas.
#[async_backtrace::framed]
pub async fn prepare_refresh_virtual_column(
    ctx: Arc<dyn TableContext>,
    fuse_table: &FuseTable,
    limit: Option<u64>,
    overwrite: bool,
    selection: Option<RefreshSelection>,
) -> Result<Vec<VirtualColumnRefreshResult>> {
    let start = Instant::now();
    let Some(snapshot) = fuse_table.read_table_snapshot().await? else {
        // no snapshot
        info!(
            "Prepare virtual column refresh finished in {} ms (no snapshot)",
            start.elapsed().as_millis()
        );
        return Ok(vec![]);
    };
    let table_schema = fuse_table.schema();

    // Collect source fields used by virtual columns.
    let mut fields = Vec::new();
    let mut field_indices = Vec::new();
    for (i, f) in table_schema.fields().iter().enumerate() {
        if f.data_type().remove_nullable() != TableDataType::Variant
            || matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_)))
        {
            continue;
        }
        fields.push(f.clone());
        field_indices.push(i);
    }

    let source_schema = Arc::new(TableSchema {
        fields,
        ..table_schema.as_ref().clone()
    });

    if !fuse_table.support_virtual_columns() {
        return Err(ErrorCode::VirtualColumnError(format!(
            "Table don't support virtual column, storage_format: {} read_only: {}",
            fuse_table.get_storage_format(),
            fuse_table.is_read_only()
        )));
    }
    let virtual_column_builder = VirtualColumnBuilder::try_create(ctx.clone(), source_schema)?;

    let projection = Projection::Columns(field_indices);
    let block_reader =
        fuse_table.create_block_reader(ctx.clone(), projection, false, false, false)?;

    let segment_reader = MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema);

    let write_settings = fuse_table.get_write_settings();
    let storage_format = write_settings.storage_format;

    let operator = fuse_table.get_operator_ref();

    let limit = limit.unwrap_or_default() as usize;
    let segment_filter = selection.as_ref().and_then(|sel| match sel {
        RefreshSelection::SegmentLocation(loc) => Some(loc.clone()),
        _ => None,
    });
    let block_filter = selection.as_ref().and_then(|sel| match sel {
        RefreshSelection::BlockLocation(loc) => Some(loc.clone()),
        _ => None,
    });
    let mut matched_selection = false;
    let mut reached_limit = false;
    // Iterates through all segments and collect blocks don't have virtual block meta.
    let mut virtual_column_tasks = Vec::new();
    for (location, ver) in snapshot.segments.iter() {
        if reached_limit {
            break;
        }
        if let Some(target) = segment_filter.as_ref() {
            if location != target {
                continue;
            }
            matched_selection = true;
        }
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;
        let stats = match segment_info.summary.additional_stats_loc() {
            Some(loc) => Some(read_segment_stats(operator.clone(), loc).await?),
            _ => None,
        };

        for (block_idx, block_meta) in segment_info.block_metas()?.into_iter().enumerate() {
            let mut matched_block_filter = false;
            if let Some(target) = block_filter.as_ref() {
                if &block_meta.location.0 != target {
                    continue;
                }
                matched_selection = true;
                matched_block_filter = true;
            }

            let mut has_legacy_virtual = false;
            if let Some(virtual_block_meta) = &block_meta.virtual_block_meta {
                if TableMetaLocationGenerator::is_legacy_virtual_block_location(
                    &virtual_block_meta.virtual_location.0,
                ) {
                    has_legacy_virtual = true;
                }
            }

            if !overwrite && block_meta.virtual_block_meta.is_some() && !has_legacy_virtual {
                if matched_block_filter {
                    reached_limit = true;
                    break;
                }
                continue;
            }

            let column_hlls = stats
                .as_ref()
                .and_then(|v| v.block_hlls.get(block_idx))
                .cloned();
            virtual_column_tasks.push(VirtualColumnBuildTask {
                block_location: block_meta.location.0.clone(),
                block_meta,
                column_hlls,
            });

            if limit > 0 && virtual_column_tasks.len() >= limit {
                reached_limit = true;
                break;
            }
            if matched_block_filter {
                reached_limit = true;
                break;
            }
        }
    }

    if let (Some(sel), false) = (selection.as_ref(), matched_selection) {
        let message = match sel {
            RefreshSelection::SegmentLocation(loc) => {
                format!("segment_location '{loc}' not found")
            }
            RefreshSelection::BlockLocation(loc) => {
                format!("block_location '{loc}' not found")
            }
        };
        return Err(ErrorCode::VirtualColumnError(message));
    }

    if virtual_column_tasks.is_empty() {
        info!(
            "Prepare virtual column refresh finished in {} ms (no tasks)",
            start.elapsed().as_millis()
        );
        return Ok(vec![]);
    }

    let block_nums = virtual_column_tasks.len();
    info!(
        "Prepared {} blocks for virtual column refresh (limit={}, overwrite={})",
        block_nums, limit, overwrite
    );

    let result = build_virtual_columns(
        ctx,
        block_reader,
        storage_format,
        operator.clone(),
        write_settings.clone(),
        virtual_column_builder,
        virtual_column_tasks,
    )
    .await;
    if result.is_ok() {
        info!(
            "Prepare virtual column refresh finished in {} ms",
            start.elapsed().as_millis()
        );
    }
    result
}

// Commit is a short phase that only updates snapshot metadata using the prepared drafts.
#[async_backtrace::framed]
pub async fn commit_refresh_virtual_column(
    ctx: Arc<dyn TableContext>,
    fuse_table: &FuseTable,
    pipeline: &mut Pipeline,
    results: Vec<VirtualColumnRefreshResult>,
) -> Result<u64> {
    let start = Instant::now();
    if results.is_empty() {
        info!(
            "Commit virtual column refresh finished in {} ms (empty results)",
            start.elapsed().as_millis()
        );
        return Ok(0);
    }

    let Some(latest_snapshot) = fuse_table.read_table_snapshot().await? else {
        info!(
            "Commit virtual column refresh finished in {} ms (no snapshot)",
            start.elapsed().as_millis()
        );
        return Ok(0);
    };
    let table_schema = fuse_table.schema();
    let segment_reader = MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema);

    let mut results_by_block = HashMap::with_capacity(results.len());
    for result in results {
        results_by_block.insert(result.block_location.clone(), result);
    }

    let mut mutation_entries = Vec::new();
    let mut applied_blocks = 0;
    for (segment_idx, (location, ver)) in latest_snapshot.segments.iter().enumerate() {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;
        for (block_idx, block_meta) in segment_info.block_metas()?.into_iter().enumerate() {
            let block_location = &block_meta.location.0;
            let Some(result) = results_by_block.get(block_location) else {
                continue;
            };
            applied_blocks += 1;
            let extended_block_meta = ExtendedBlockMeta {
                block_meta: Arc::unwrap_or_clone(block_meta.clone()),
                draft_virtual_block_meta: Some(result.draft_virtual_block_meta.clone()),
                column_hlls: result.column_hlls.clone().map(BlockHLLState::Serialized),
            };
            let entry = MutationLogEntry::ReplacedBlock {
                index: BlockMetaIndex {
                    segment_idx,
                    block_idx,
                },
                block_meta: Arc::new(extended_block_meta),
            };
            mutation_entries.push(entry);
        }
    }

    if mutation_entries.is_empty() {
        info!(
            "Commit virtual column refresh finished in {} ms (no updates)",
            start.elapsed().as_millis()
        );
        return Ok(0);
    }

    info!(
        "Prepared {} block meta updates for virtual column refresh",
        applied_blocks
    );

    let meta = MutationLogs {
        entries: mutation_entries,
    };
    let block = DataBlock::from(meta);
    pipeline.add_source(
        move |output| OneBlockSource::create(output, block.clone()),
        1,
    )?;

    let table_meta_timestamps =
        ctx.get_table_meta_timestamps(fuse_table, Some(latest_snapshot.clone()))?;
    pipeline.add_async_accumulating_transformer(|| {
        TableMutationAggregator::create(
            fuse_table,
            ctx.clone(),
            latest_snapshot.segments.clone(),
            vec![],
            vec![],
            Statistics::default(),
            MutationKind::Refresh,
            table_meta_timestamps,
        )
    });

    let snapshot_gen = MutationGenerator::new(Some(latest_snapshot), MutationKind::Refresh);
    pipeline.add_sink(|input| {
        // Allow OCC retries so concurrent inserts do not fail refresh commits.
        CommitSink::try_create(
            fuse_table,
            ctx.clone(),
            None,
            vec![],
            snapshot_gen.clone(),
            input,
            None,
            None,
            None,
            table_meta_timestamps,
        )
    })?;

    let applied_blocks = applied_blocks as u64;
    info!(
        "Commit virtual column refresh finished in {} ms",
        start.elapsed().as_millis()
    );
    Ok(applied_blocks)
}

const VIRTUAL_COLUMN_PROGRESS_LOG_STEP: usize = 10;

struct VirtualColumnBuildTask {
    block_location: String,
    block_meta: Arc<BlockMeta>,
    column_hlls: Option<RawBlockHLL>,
}

#[async_backtrace::framed]
async fn build_virtual_columns(
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<BlockReader>,
    storage_format: FuseStorageFormat,
    operator: Operator,
    write_settings: WriteSettings,
    virtual_column_builder: VirtualColumnBuilder,
    tasks: Vec<VirtualColumnBuildTask>,
) -> Result<Vec<VirtualColumnRefreshResult>> {
    let block_nums = tasks.len();
    let max_threads = ctx.get_settings().get_max_threads()? as usize;
    let max_threads = std::cmp::min(block_nums, max_threads).max(1);
    info!(
        "Virtual column build will process {} blocks with {} async workers",
        block_nums, max_threads
    );
    let processed = Arc::new(AtomicUsize::new(0));
    let settings = ReadSettings::from_ctx(&ctx)?;

    let results: Vec<Result<_, _>> = execute_futures_in_parallel(
        tasks.into_iter().map(move |task| {
            let block_reader = block_reader.clone();
            let operator = operator.clone();
            let write_settings = write_settings.clone();
            let mut virtual_column_builder = virtual_column_builder.clone();
            let processed = processed.clone();
            let storage_format = storage_format;
            let settings = settings;
            async move {
                let block = block_reader
                    .read_by_meta(&settings, &task.block_meta, &storage_format)
                    .await?;
                virtual_column_builder.add_block(&block)?;
                let virtual_column_state =
                    virtual_column_builder.finalize(&write_settings, &task.block_meta.location)?;

                if virtual_column_state
                    .draft_virtual_block_meta
                    .virtual_column_size
                    > 0
                {
                    let start = Instant::now();

                    let virtual_column_size = virtual_column_state
                        .draft_virtual_block_meta
                        .virtual_column_size;
                    let location = &virtual_column_state
                        .draft_virtual_block_meta
                        .virtual_location
                        .0;

                    write_data(virtual_column_state.data, &operator, location).await?;

                    metrics_inc_block_virtual_column_write_nums(1);
                    metrics_inc_block_virtual_column_write_bytes(virtual_column_size);
                    metrics_inc_block_virtual_column_write_milliseconds(
                        start.elapsed().as_millis() as u64,
                    );
                    info!(
                        "Virtual column written for block {} at {} ({} bytes)",
                        task.block_location, location, virtual_column_size
                    );
                } else {
                    info!(
                        "No virtual column data produced for block {}",
                        task.block_location
                    );
                }

                let processed_blocks = processed.fetch_add(1, Ordering::Relaxed) + 1;
                if processed_blocks == 1
                    || processed_blocks == block_nums
                    || processed_blocks % VIRTUAL_COLUMN_PROGRESS_LOG_STEP == 0
                {
                    info!(
                        "Virtual column build progress: {}/{}",
                        processed_blocks, block_nums
                    );
                } else {
                    debug!(
                        "Virtual column build progress: {}/{}",
                        processed_blocks, block_nums
                    );
                }

                Ok(VirtualColumnRefreshResult {
                    block_location: task.block_location,
                    draft_virtual_block_meta: virtual_column_state.draft_virtual_block_meta,
                    column_hlls: task.column_hlls,
                })
            }
        }),
        max_threads,
        max_threads * 2,
        "virtual-column-refresh-worker".to_owned(),
    )
    .await?;

    let mut output = Vec::with_capacity(results.len());
    for result in results {
        output.push(result?);
    }
    Ok(output)
}
