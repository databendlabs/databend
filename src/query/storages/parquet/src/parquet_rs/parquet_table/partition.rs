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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::plan::FullParquetMeta;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::TopK;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_storage::CopyStatus;
use databend_common_storage::FileStatus;
use parquet::arrow::arrow_reader::RowSelector;

use super::table::ParquetRSTable;
use crate::parquet_part::collect_small_file_parts;
use crate::parquet_rs::partition::SerdePageLocation;
use crate::parquet_rs::partition::SerdeRowSelector;
use crate::parquet_rs::read_parquet_metas_batch;
use crate::parquet_rs::ParquetRSRowGroupPart;
use crate::ParquetPart;
use crate::ParquetRSPruner;

impl ParquetRSTable {
    #[inline]
    #[async_backtrace::framed]
    pub(super) async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let thread_num = ctx.get_settings().get_max_threads()? as usize;

        // Unwrap safety: no other thread will hold this lock.
        let parquet_metas = self.parquet_metas.try_lock().unwrap();
        let file_locations = if parquet_metas.is_empty() {
            match &self.files_to_read {
                Some(files) => files
                    .iter()
                    .map(|f| (f.path.clone(), f.size))
                    .collect::<Vec<_>>(),
                None => self
                    .files_info
                    .list(&self.operator, thread_num, None)
                    .await?
                    .into_iter()
                    .map(|f| (f.path, f.size))
                    .collect::<Vec<_>>(),
            }
        } else {
            // Already fetched the parquet metas when creating column statistics provider.
            parquet_metas
                .iter()
                .map(|p| (p.location.clone(), p.size))
                .collect()
        };

        // If a file size is less than `parquet_fast_read_bytes`,
        // we treat it as a small file and it will be totally loaded into memory.
        let fast_read_bytes = ctx.get_settings().get_parquet_fast_read_bytes()?;
        let mut large_files = vec![];
        let mut large_file_indices = vec![];
        let mut small_file_indices = vec![];
        let mut small_files = vec![];
        for (index, (location, size)) in file_locations.into_iter().enumerate() {
            if size > fast_read_bytes {
                large_files.push((location, size));
                large_file_indices.push(index);
            } else {
                small_files.push((location, size));
                small_file_indices.push(index);
            }
        }

        let pruner = Arc::new(ParquetRSPruner::try_create(
            ctx.get_function_context()?,
            self.schema(),
            self.leaf_fields.clone(),
            &push_down,
            self.read_options,
            vec![],
        )?);

        let copy_status = if matches!(ctx.get_query_kind(), QueryKind::CopyIntoTable) {
            Some(ctx.get_copy_status())
        } else {
            None
        };

        // Get columns needed to be read into memory.
        // It will be used to calculate the memory will be used in reading.
        let columns_to_read = if let Some(prewhere) =
            PushDownInfo::prewhere_of_push_downs(push_down.as_ref())
        {
            let (_, prewhere_columns) = prewhere
                .prewhere_columns
                .to_arrow_projection(&self.schema_descr);
            let (_, output_columns) = prewhere
                .output_columns
                .to_arrow_projection(&self.schema_descr);
            let mut columns = HashSet::with_capacity(prewhere_columns.len() + output_columns.len());
            columns.extend(prewhere_columns);
            columns.extend(output_columns);
            let mut columns = columns.into_iter().collect::<Vec<_>>();
            columns.sort();
            columns
        } else {
            let output_projection =
                PushDownInfo::projection_of_push_downs(&self.schema(), push_down.as_ref());
            let (_, columns) = output_projection.to_arrow_projection(&self.schema_descr);
            columns
        };

        let num_columns_to_read = columns_to_read.len();
        let topk = push_down.as_ref().and_then(|p| p.top_k(&self.schema()));

        let (mut stats, mut partitions) = if parquet_metas.is_empty() {
            self.read_and_prune_metas_in_parallel(
                ctx,
                large_files,
                pruner,
                columns_to_read,
                Arc::new(topk),
                copy_status,
            )
            .await?
        } else {
            prune_metas_in_parallel(
                ctx,
                &parquet_metas,
                large_file_indices,
                pruner,
                columns_to_read,
                Arc::new(topk),
                copy_status,
            )
            .await?
        };

        // If there are only row group parts, the `stats` is exact.
        // It will be changed to `false` if there are small files parts.
        if !small_files.is_empty() {
            let mut max_compression_ratio = self.compression_ratio;
            let mut max_compressed_size = 0u64;
            for part in partitions.partitions.iter() {
                let p = part.as_any().downcast_ref::<ParquetPart>().unwrap();
                max_compression_ratio = max_compression_ratio
                    .max(p.uncompressed_size() as f64 / p.compressed_size() as f64);
                max_compressed_size = max_compressed_size.max(p.compressed_size());
            }

            if parquet_metas.is_empty() {
                // If we don't get the parquet metas, we cannot get the exact stats.
                stats.is_exact = false;
                collect_small_file_parts(
                    small_files,
                    max_compression_ratio,
                    max_compressed_size,
                    &mut partitions,
                    &mut stats,
                    num_columns_to_read,
                );
            } else {
                // We have already got the parquet metas, we can compute the exact stats by the metas directly.
                stats.partitions_total += small_file_indices.len();
                stats.partitions_scanned += small_file_indices.len();
                for file in small_file_indices {
                    let meta = &parquet_metas[file];
                    stats.read_bytes += meta.size as usize;
                    stats.read_rows += meta.meta.file_metadata().num_rows() as usize;
                }

                let mut dummy_stats = PartStatistics::default(); // This will not be used.
                collect_small_file_parts(
                    small_files,
                    max_compression_ratio,
                    max_compressed_size,
                    &mut partitions,
                    &mut dummy_stats,
                    num_columns_to_read,
                );
            }
        }

        Ok((stats, partitions))
    }

    #[async_backtrace::framed]
    async fn read_and_prune_metas_in_parallel(
        &self,
        ctx: Arc<dyn TableContext>,
        file_infos: Vec<(String, u64)>,
        pruner: Arc<ParquetRSPruner>,
        columns_to_read: Vec<usize>,
        topk: Arc<Option<TopK>>,
        copy_status: Option<Arc<CopyStatus>>,
    ) -> Result<(PartStatistics, Partitions)> {
        let settings = ctx.get_settings();
        let num_files = file_infos.len();
        let num_threads = settings.get_max_threads()? as usize;
        let max_memory_usage = settings.get_max_memory_usage()?;

        let mut tasks = Vec::with_capacity(num_threads);

        // Equally distribute the tasks
        for i in 0..num_threads {
            let begin = num_files * i / num_threads;
            let end = num_files * (i + 1) / num_threads;
            if begin == end {
                continue;
            }
            let file_infos = file_infos[begin..end].to_vec();
            let pruner = pruner.clone();
            let op = self.operator.clone();
            let columns_to_read = columns_to_read.clone();
            let expect = self.schema_descr.clone();
            let schema_from = self.schema_from.clone();
            let copy_status = copy_status.clone();
            let leaf_fields = self.leaf_fields.clone();
            let topk = topk.clone();

            tasks.push(async move {
                let metas = read_parquet_metas_batch(
                    file_infos,
                    op,
                    expect,
                    leaf_fields,
                    schema_from,
                    max_memory_usage,
                )
                .await?;
                prune_and_generate_partitions(&pruner, metas, columns_to_read, &topk, copy_status)
            });
        }

        let (stats, parts) = execute_futures_in_parallel(
            tasks,
            num_threads,
            num_threads * 2,
            "read-and-prune-parquet-metas-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .reduce(|(mut stats_acc, mut parts_acc), (stats, parts)| {
            stats_acc.merge(&stats);
            parts_acc.extend(parts);
            (stats_acc, parts_acc)
        })
        .unwrap_or((PartStatistics::default_exact(), vec![]));

        Ok((stats, create_partitions(parts, &topk)))
    }
}

/// Call this method only if already collected parquet metas and stats.
#[async_backtrace::framed]
async fn prune_metas_in_parallel(
    ctx: Arc<dyn TableContext>,
    parquet_metas: &[Arc<FullParquetMeta>],
    files: Vec<usize>,
    pruner: Arc<ParquetRSPruner>,
    columns_to_read: Vec<usize>,
    topk: Arc<Option<TopK>>,
    copy_status: Option<Arc<CopyStatus>>,
) -> Result<(PartStatistics, Partitions)> {
    if files.is_empty() {
        return Ok((
            PartStatistics::default_exact(),
            Partitions::create(PartitionsShuffleKind::Mod, vec![]),
        ));
    }

    assert!(!parquet_metas.is_empty());
    let settings = ctx.get_settings();
    let num_files = files.len();
    let num_threads = settings.get_max_threads()? as usize;

    let mut tasks = Vec::with_capacity(num_threads);

    // Equally distribute the tasks
    for i in 0..num_threads {
        let begin = num_files * i / num_threads;
        let end = num_files * (i + 1) / num_threads;
        if begin == end {
            continue;
        }
        let files = &files[begin..end];
        let metas = files
            .iter()
            .map(|i| parquet_metas[*i].clone())
            .collect::<Vec<_>>();
        let pruner = pruner.clone();
        let columns_to_read = columns_to_read.clone();
        let copy_status = copy_status.clone();
        let topk = topk.clone();

        tasks.push(async move {
            prune_and_generate_partitions(&pruner, metas, columns_to_read, &topk, copy_status)
        });
    }

    let (stats, parts) = execute_futures_in_parallel(
        tasks,
        num_threads,
        num_threads * 2,
        "prune-parquet-metas-worker".to_owned(),
    )
    .await?
    .into_iter()
    .collect::<Result<Vec<_>>>()?
    .into_iter()
    .reduce(|(mut stats_acc, mut parts_acc), (stats, parts)| {
        stats_acc.merge(&stats);
        parts_acc.extend(parts);
        (stats_acc, parts_acc)
    })
    .unwrap_or((PartStatistics::default_exact(), vec![]));

    Ok((stats, create_partitions(parts, &topk)))
}

fn prune_and_generate_partitions(
    pruner: &ParquetRSPruner,
    parquet_metas: Vec<Arc<FullParquetMeta>>,
    columns_to_read: Vec<usize>,
    topk: &Option<TopK>,
    copy_status: Option<Arc<CopyStatus>>,
) -> Result<(PartStatistics, Vec<ParquetRSRowGroupPart>)> {
    let mut parts = vec![];
    let mut part_stats = PartStatistics::default_exact();
    for meta in parquet_metas {
        let FullParquetMeta {
            location,
            meta,
            row_group_level_stats,
            ..
        } = meta.as_ref();
        part_stats.partitions_total += meta.num_row_groups();
        let (rgs, omits) = pruner.prune_row_groups(meta, row_group_level_stats.as_deref(), None)?;
        let mut row_selections = if omits.iter().all(|x| *x) {
            None
        } else {
            pruner.prune_pages(meta, &rgs, None)?
        };

        let mut rows_read = 0; // Rows read in current file.

        for (rg, omit) in rgs.into_iter().zip(omits.into_iter()) {
            let rg_meta = meta.row_group(rg);
            let num_rows = rg_meta.num_rows() as usize;
            // Split rows belonging to current row group.
            let selection = row_selections.as_mut().map(|s| s.split_off(num_rows));
            if !selection.as_ref().map(|x| x.selects_any()).unwrap_or(true) {
                // All rows in current row group are filtered out.
                continue;
            }

            let serde_selection = selection.map(|s| {
                let selectors: Vec<RowSelector> = s.into();
                selectors
                    .iter()
                    .map(SerdeRowSelector::from)
                    .collect::<Vec<_>>()
            });

            rows_read += num_rows;
            part_stats.read_bytes += rg_meta.compressed_size() as usize;
            part_stats.partitions_scanned += 1;

            let page_locations = meta.offset_index().map(|x| {
                x[rg]
                    .iter()
                    .map(|x| x.iter().map(SerdePageLocation::from).collect())
                    .collect()
            });

            let mut compressed_size = 0;
            let mut uncompressed_size = 0;
            for col in columns_to_read.iter() {
                compressed_size += rg_meta.column(*col).compressed_size() as u64;
                uncompressed_size += rg_meta.column(*col).uncompressed_size() as u64;
            }

            let sort_min_max =
                topk.as_ref()
                    .zip(row_group_level_stats.as_ref())
                    .map(|(t, stats)| {
                        let stat = &stats[rg][&(t.leaf_id as u32)];
                        (stat.min.clone(), stat.max.clone())
                    });

            parts.push(ParquetRSRowGroupPart {
                location: location.clone(),
                selectors: serde_selection,
                meta: rg_meta.clone(),
                page_locations,
                compressed_size,
                uncompressed_size,
                sort_min_max,
                omit_filter: omit,
                schema_index: 0,
            });
        }

        part_stats.read_rows += rows_read;
        if let Some(copy_status) = &copy_status {
            copy_status.add_chunk(location, FileStatus {
                num_rows_loaded: rows_read,
                error: None,
            });
        }
    }

    Ok((part_stats, parts))
}

fn create_partitions(mut parts: Vec<ParquetRSRowGroupPart>, topk: &Option<TopK>) -> Partitions {
    if let Some(topk) = topk {
        if topk.asc {
            parts.sort_by(|a, b| {
                let (a_min, a_max) = a.sort_min_max.as_ref().unwrap();
                let (b_min, b_max) = b.sort_min_max.as_ref().unwrap();
                (a_min, a_max).cmp(&(b_min, b_max))
            });
        } else {
            parts.sort_by(|a, b| {
                let (a_min, a_max) = a.sort_min_max.as_ref().unwrap();
                let (b_min, b_max) = b.sort_min_max.as_ref().unwrap();
                (b_max, b_min).cmp(&(a_max, a_min))
            });
        }
    }

    let parts = parts
        .into_iter()
        .map(|p| Arc::new(Box::new(ParquetPart::ParquetRSRowGroup(p)) as Box<dyn PartInfo>))
        .collect();

    Partitions::create(PartitionsShuffleKind::Mod, parts)
}
