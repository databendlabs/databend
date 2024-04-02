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
use std::time::Duration;

use backoff::ExponentialBackoff;
use backoff::ExponentialBackoffBuilder;
use databend_common_base::base::tokio::sync::OwnedSemaphorePermit;
use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use opendal::Operator;

use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::SegmentsIO;
use crate::io::SnapshotsIO;
use crate::FuseStorageFormat;

const OCC_DEFAULT_BACKOFF_INIT_DELAY_MS: Duration = Duration::from_millis(5);
const OCC_DEFAULT_BACKOFF_MAX_DELAY_MS: Duration = Duration::from_millis(20 * 1000);
const OCC_DEFAULT_BACKOFF_MAX_ELAPSED_MS: Duration = Duration::from_millis(120 * 1000);

#[inline]
pub fn set_backoff(
    init_retry_delay: Option<Duration>,
    max_retry_delay: Option<Duration>,
    max_retry_elapsed: Option<Duration>,
) -> ExponentialBackoff {
    // The initial retry delay in millisecond. By default,  it is 5 ms.
    let init_delay = init_retry_delay.unwrap_or(OCC_DEFAULT_BACKOFF_INIT_DELAY_MS);

    // The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing.
    // By default, it is 20 seconds.
    let max_delay = max_retry_delay.unwrap_or(OCC_DEFAULT_BACKOFF_MAX_DELAY_MS);

    // The maximum elapsed time after the occ starts, beyond which there will be no more retries.
    // By default, it is 2 minutes
    let max_elapsed = max_retry_elapsed.unwrap_or(OCC_DEFAULT_BACKOFF_MAX_ELAPSED_MS);

    // TODO(xuanwo): move to backon instead.
    //
    // To simplify the settings, using fixed common values for randomization_factor and multiplier
    ExponentialBackoffBuilder::new()
        .with_initial_interval(init_delay)
        .with_max_interval(max_delay)
        .with_randomization_factor(0.5)
        .with_multiplier(2.0)
        .with_max_elapsed_time(Some(max_elapsed))
        .build()
}

pub fn column_parquet_metas(
    file_meta: &parquet_rs::format::FileMetaData,
    schema: &TableSchemaRef,
) -> Result<HashMap<ColumnId, ColumnMeta>> {
    // currently we use one group only
    let num_row_groups = file_meta.row_groups.len();
    if num_row_groups != 1 {
        return Err(ErrorCode::ParquetFileInvalid(format!(
            "invalid parquet file, expects only one row group, but got {}",
            num_row_groups
        )));
    }
    // use `to_leaf_column_ids` instead of `to_column_ids` to handle nested type column ids.
    let column_ids = schema.to_leaf_column_ids();
    let row_group = &file_meta.row_groups[0];
    // Make sure that schema and row_group has the same number column, or else it is a panic error.
    assert_eq!(column_ids.len(), row_group.columns.len());
    let mut col_metas = HashMap::with_capacity(row_group.columns.len());
    for (idx, col_chunk) in row_group.columns.iter().enumerate() {
        match &col_chunk.meta_data {
            Some(chunk_meta) => {
                let col_start = if let Some(dict_page_offset) = chunk_meta.dictionary_page_offset {
                    dict_page_offset
                } else {
                    chunk_meta.data_page_offset
                };
                let col_len = chunk_meta.total_compressed_size;
                assert!(
                    col_start >= 0 && col_len >= 0,
                    "column start and length should not be negative"
                );
                let num_values = chunk_meta.num_values as u64;
                let res = SingleColumnMeta {
                    offset: col_start as u64,
                    len: col_len as u64,
                    num_values,
                };
                // use column id as key instead of index
                let column_id = column_ids[idx];
                col_metas.insert(column_id, ColumnMeta::Parquet(res));
            }
            None => {
                return Err(ErrorCode::ParquetFileInvalid(format!(
                    "invalid parquet file, meta data of column idx {} is empty",
                    idx
                )));
            }
        }
    }
    Ok(col_metas)
}

#[async_backtrace::framed]
pub async fn acquire_task_permit(
    io_request_semaphore: Arc<Semaphore>,
) -> Result<OwnedSemaphorePermit> {
    let permit = io_request_semaphore.acquire_owned().await.map_err(|e| {
        ErrorCode::Internal("unexpected, io request semaphore is closed. {}")
            .add_message_back(e.to_string())
    })?;
    Ok(permit)
}

pub async fn read_block(
    storage_format: FuseStorageFormat,
    reader: &BlockReader,
    block_meta: &BlockMeta,
    read_settings: &ReadSettings,
    query_id: String,
) -> Result<DataBlock> {
    let merged_io_read_result = reader
        .read_columns_data_by_merge_io(
            read_settings,
            &block_meta.location.0,
            &block_meta.col_metas,
            &None,
        )
        .await?;

    // deserialize block data
    // cpu intensive task, send them to dedicated thread pool
    let block_meta_ptr = block_meta.clone();
    let reader = reader.clone();

    GlobalIORuntime::instance()
        .spawn(query_id, async move {
            let column_chunks = merged_io_read_result.columns_chunks()?;
            reader.deserialize_chunks(
                block_meta_ptr.location.0.as_str(),
                block_meta_ptr.row_count as usize,
                &block_meta_ptr.compression,
                &block_meta_ptr.col_metas,
                column_chunks,
                &storage_format,
            )
        })
        .await
        .map_err(|e| {
            ErrorCode::Internal("unexpected, failed to read block for merge into")
                .add_message_back(e.to_string())
        })?
}

pub async fn collect_incremental_blocks(
    ctx: Arc<dyn TableContext>,
    fuse_segment_io: SegmentsIO,
    op: Operator,
    latest: &Option<String>,
    base: &Option<String>,
) -> Result<(Vec<(String, u64)>, Vec<Arc<BlockMeta>>, Vec<Arc<BlockMeta>>)> {
    let latest_segments = if let Some(snapshot) = latest {
        let (sn, _) = SnapshotsIO::read_snapshot(snapshot.to_string(), op.clone()).await?;
        HashSet::from_iter(sn.segments.clone())
    } else {
        HashSet::new()
    };

    let base_segments = if let Some(snapshot) = base {
        let (sn, _) = SnapshotsIO::read_snapshot(snapshot.to_string(), op.clone()).await?;
        HashSet::from_iter(sn.segments.clone())
    } else {
        HashSet::new()
    };

    let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;

    let mut base_blocks = HashMap::new();
    let diff_in_base = base_segments
        .difference(&latest_segments)
        .cloned()
        .collect::<Vec<_>>();
    for chunk in diff_in_base.chunks(chunk_size) {
        let segments = fuse_segment_io
            .read_segments::<SegmentInfo>(chunk, true)
            .await?;
        for segment in segments {
            let segment = segment?;
            segment.blocks.into_iter().for_each(|block| {
                base_blocks.insert(block.location.clone(), block);
            })
        }
    }

    let mut add_blocks = Vec::new();
    let diff_in_latest = latest_segments
        .difference(&base_segments)
        .cloned()
        .collect::<Vec<_>>();
    for chunk in diff_in_latest.chunks(chunk_size) {
        let segments = fuse_segment_io
            .read_segments::<SegmentInfo>(chunk, true)
            .await?;

        for segment in segments {
            let segment = segment?;
            segment.blocks.into_iter().for_each(|block| {
                if base_blocks.contains_key(&block.location) {
                    base_blocks.remove(&block.location);
                } else {
                    add_blocks.push(block);
                }
            });
        }
    }

    let del_blocks = base_blocks.into_values().collect::<Vec<_>>();
    Ok((diff_in_latest, del_blocks, add_blocks))
}
