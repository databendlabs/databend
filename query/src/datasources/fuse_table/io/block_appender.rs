// Copyright 2020 Datafuse Labs.
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
//
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read::read_metadata;
use common_arrow::arrow::io::parquet::write::write_file;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::arrow::io::parquet::write::*;
use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::parquet::statistics::serialize_statistics;
use common_arrow::parquet::statistics::Statistics as ParquetStats;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_store_api::MetaApi;
use futures::StreamExt;
use uuid::Uuid;

use crate::datasources::fuse_table::meta::table_snapshot::BlockLocation;
use crate::datasources::fuse_table::meta::table_snapshot::BlockMeta;
use crate::datasources::fuse_table::meta::table_snapshot::ColStats;
use crate::datasources::fuse_table::meta::table_snapshot::SegmentInfo;
use crate::datasources::fuse_table::meta::table_snapshot::Stats;
use crate::datasources::fuse_table::table::FuseTable;
use crate::datasources::fuse_table::util;
use crate::sessions::DatafuseQueryContextRef;

pub type BlockStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = DataBlock> + Sync + Send + 'static>>;

impl<T> FuseTable<T>
where T: MetaApi + Send + Sync + 'static
{
    pub async fn append_blocks(
        &self,
        ctx: DatafuseQueryContextRef,
        mut stream: BlockStream,
    ) -> Result<SegmentInfo> {
        let mut blocks = vec![];
        let mut block_stats = vec![];

        let mut summary_row_count = 0u64;
        let mut summary_block_count = 0u64;
        let mut summary_uncompressed_byte_size = 0u64;
        let mut summary_compressed_byte_size = 0u64;

        while let Some(block) = stream.next().await {
            let schema = block.schema().to_arrow();

            // 1. At present, for each block, we create a parquet
            let buffer = write_in_memory(&schema, block)?;
            let block_size = buffer.len() as u64;

            // 2. extract statistics
            // TODO : read it again? we need some love from parquet2
            let mut cursor = Cursor::new(buffer);
            let parquet_meta = read_metadata(&mut cursor)?;
            let row_count = parquet_meta.num_rows as u64;

            // We arrange exactly one row group, and one page insides the parquet file
            let rg_meta = &parquet_meta.row_groups[0];

            //let compressed_size = rg_meta.compressed_size() as u64;
            //let total_byte_size = rg_meta.total_byte_size() as u64;

            let mut col_dyn_stats = HashMap::new();
            for (id, item) in rg_meta.columns().iter().enumerate() {
                let col_id = id as u32; // fake id, TODO mapping to real column id
                col_dyn_stats.insert(col_id, item.statistics().unwrap().unwrap().clone());
            }

            let part_uuid = Uuid::new_v4().to_simple().to_string() + ".parquet";
            let location = format!("_t/{}", part_uuid);
            // TODO, we need some love from parquet2 (accumulate size and stats while writing)
            let meta_size = 0u64;

            let col_stats = col_dyn_stats
                .iter()
                .map(|(id, stats)| {
                    let s = serialize_statistics(stats.as_ref());
                    (*id, ColStats {
                        min: s.min_value,
                        max: s.max_value,
                        null_count: s.null_count.map(|v| v as u64),
                    })
                })
                .collect::<HashMap<_, _>>();

            let block_info = BlockMeta {
                location: BlockLocation {
                    location: location.clone(),
                    meta_size,
                },
                row_count,
                block_size,
                col_stats,
            };

            blocks.push(block_info);

            summary_block_count += 1;
            summary_row_count += row_count;
            summary_compressed_byte_size += block_size;
            summary_uncompressed_byte_size += rg_meta.total_byte_size() as u64;
            block_stats.push(col_dyn_stats);

            // write to storage
            self.data_accessor(&ctx)?
                .put(&location, cursor.into_inner())
                .await?;
        }

        let summary = reduce(block_stats);

        let mut res = HashMap::new();
        for (col, col_stats) in summary {
            match col_stats {
                Ok(Some(stats)) => {
                    let s = serialize_statistics(stats.as_ref());
                    let c = ColStats {
                        min: s.min_value,
                        max: s.max_value,
                        null_count: s.null_count.map(|v| v as u64),
                    };
                    res.insert(col, c);
                }
                _ => todo!(),
            }
        }

        let segment_info = SegmentInfo {
            blocks,
            summary: Stats {
                row_count: summary_row_count,
                block_count: summary_block_count,
                uncompressed_byte_size: summary_uncompressed_byte_size,
                compressed_byte_size: summary_compressed_byte_size,
                col_stats: res,
            },
        };
        Ok(segment_info)
    }
}

pub(crate) fn write_in_memory(arrow_schema: &ArrowSchema, block: DataBlock) -> Result<Vec<u8>> {
    // TODO pick proper compression / encoding algos
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Uncompressed,
        version: Version::V2,
    };
    use std::iter::repeat;
    let encodings: Vec<_> = repeat(Encoding::Plain).take(block.num_columns()).collect();

    let memory_size = block.memory_size();
    let batch = RecordBatch::try_from(block)?;
    let iter = vec![Ok(batch)];
    let row_groups = RowGroupIterator::try_new(iter.into_iter(), arrow_schema, options, encodings)?;
    let writer = Vec::with_capacity(memory_size);
    let mut cursor = Cursor::new(writer);
    let parquet_schema = row_groups.parquet_schema().clone();
    write_file(
        &mut cursor,
        row_groups,
        arrow_schema,
        parquet_schema,
        options,
        None,
    )?;

    Ok(cursor.into_inner())
}

fn reduce(
    block_stats: Vec<HashMap<u32, Arc<dyn ParquetStats>>>,
) -> HashMap<u32, Result<Option<Arc<dyn ParquetStats>>>> {
    // accumulate stats by column
    let col_stat_list = block_stats.iter().fold(HashMap::new(), |acc, item| {
        item.iter().fold(
            acc,
            |mut acc: HashMap<u32, Vec<Arc<dyn ParquetStats>>>, (col_id, stats)| {
                let entry = acc.entry(*col_id);
                entry
                    .and_modify(|v| v.push(stats.clone()))
                    .or_insert_with(|| vec![stats.clone()]);
                acc
            },
        )
    });

    // for each col, reduce the vector of statistics
    col_stat_list
        .iter()
        .map(|(col_id, stat_list)| {
            let reduced = util::stats_aggregation::reduce(stat_list.as_slice()).map_err(|e| {
                ErrorCode::UnknownException(format!("TODO from parquet error, {}", e))
            });
            (*col_id, reduced)
        })
        .collect::<HashMap<u32, Result<Option<Arc<dyn ParquetStats>>>>>()
}
