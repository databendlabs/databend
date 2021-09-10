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
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::Cursor;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::write::write_file;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::arrow::io::parquet::write::*;
use common_arrow::arrow::record_batch::RecordBatch;
use common_datablocks::DataBlock;
use common_datavalues::columns::DataColumn;
use common_datavalues::DataType;
use common_exception::Result;
use common_store_api::BlockStream;
use common_store_api::MetaApi;
use futures::StreamExt;
use uuid::Uuid;

use crate::datasources::fuse_table::meta::table_snapshot::BlockLocation;
use crate::datasources::fuse_table::meta::table_snapshot::BlockMeta;
use crate::datasources::fuse_table::meta::table_snapshot::ColStats;
use crate::datasources::fuse_table::meta::table_snapshot::ColumnId;
use crate::datasources::fuse_table::meta::table_snapshot::SegmentInfo;
use crate::datasources::fuse_table::meta::table_snapshot::Stats;
use crate::datasources::fuse_table::table::FuseTable;
use crate::datasources::fuse_table::util::location_gen::block_location;
use crate::sessions::DatafuseQueryContextRef;

impl<T> FuseTable<T>
where T: MetaApi + Send + Sync + 'static
{
    pub async fn append_blocks(
        &self,
        ctx: DatafuseQueryContextRef,
        mut stream: BlockStream,
    ) -> Result<SegmentInfo> {
        let mut block_metas = vec![];
        let mut blocks_stats = vec![];
        let mut summary_row_count = 0u64;
        let mut summary_block_count = 0u64;
        let mut summary_uncompressed_byte_size = 0u64;
        let mut summary_compressed_byte_size = 0u64;

        while let Some(block) = stream.next().await {
            let schema = block.schema().to_arrow();
            let blk_stats = block_stats(&block)?;

            let row_count = block.num_rows() as u64;
            let block_size = block.memory_size() as u64;

            let buffer = write_in_memory(&schema, block)?;
            let buffer_size = buffer.len() as u64;

            let part_uuid = Uuid::new_v4().to_simple().to_string() + ".parquet";
            let location = block_location(&part_uuid);

            let meta_size = 0u64;

            let col_stats = blk_stats
                .iter()
                .map(|(idx, v)| (*idx, v.1.clone()))
                .collect::<HashMap<ColumnId, ColStats>>();

            let block_info = BlockMeta {
                location: BlockLocation {
                    location: location.clone(),
                    meta_size,
                },
                row_count,
                block_size,
                col_stats,
            };

            block_metas.push(block_info);
            blocks_stats.push(blk_stats);

            summary_block_count += 1;
            summary_row_count += row_count;
            summary_compressed_byte_size += buffer_size;
            summary_uncompressed_byte_size += block_size;

            // write to storage
            self.data_accessor(&ctx)?.put(&location, buffer).await?;
        }

        let summary = reduce(blocks_stats)?;
        let segment_info = SegmentInfo {
            blocks: block_metas,
            summary: Stats {
                row_count: summary_row_count,
                block_count: summary_block_count,
                uncompressed_byte_size: summary_uncompressed_byte_size,
                compressed_byte_size: summary_compressed_byte_size,
                col_stats: summary,
            },
        };
        Ok(segment_info)
    }
}

pub fn block_stats(data_block: &DataBlock) -> Result<HashMap<ColumnId, (DataType, ColStats)>> {
    // TODO column id is FAKED, this is OK as long as table schema is NOT changed, which is not realistic
    // we should extend DataField with column_id ...
    (0..).into_iter().zip(data_block.columns().iter()).try_fold(
        HashMap::new(),
        |mut res, (idx, col)| {
            let data_type = col.data_type();
            let min = match col {
                DataColumn::Array(s) => s.min(),
                DataColumn::Constant(v, _) => Ok(v.clone()),
            }?;

            let max = match col {
                DataColumn::Array(s) => s.max(),
                DataColumn::Constant(v, _) => Ok(v.clone()),
            }?;

            let null_count = match col {
                DataColumn::Array(s) => s.null_count(),
                DataColumn::Constant(v, _) => {
                    if v.is_null() {
                        1
                    } else {
                        0
                    }
                }
            };
            let col_stats = ColStats {
                min,
                max,
                null_count,
            };
            res.insert(idx, (data_type, col_stats));
            Ok(res)
        },
    )
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
    stats: Vec<HashMap<ColumnId, (DataType, ColStats)>>,
) -> Result<HashMap<ColumnId, ColStats>> {
    let len = stats.len();
    let col_stat_list = stats.into_iter().fold(HashMap::new(), |acc, item| {
        item.into_iter().fold(
            acc,
            |mut acc: HashMap<ColumnId, (DataType, Vec<ColStats>)>,
             (col_id, (data_type, stats))| {
                let entry = acc.entry(col_id);
                match entry {
                    Entry::Occupied(_) => {
                        entry.and_modify(|v| v.1.push(stats));
                    }
                    Entry::Vacant(_) => {
                        entry.or_insert((data_type, vec![stats]));
                    }
                }
                acc
            },
        )
    });

    col_stat_list.iter().try_fold(
        HashMap::with_capacity(len),
        |mut acc, (id, (data_type, stats))| {
            let mut min_stats = Vec::with_capacity(stats.len());
            let mut max_stats = Vec::with_capacity(stats.len());
            let mut null_count = 0;

            for col_stats in stats {
                min_stats.push(&col_stats.min);
                max_stats.push(&col_stats.max);
                null_count += col_stats.null_count;
            }

            let min = common_datavalues::DataValue::try_into_data_array_new(
                min_stats.as_slice(),
                data_type,
            )?
            .min()?;

            let max = common_datavalues::DataValue::try_into_data_array_new(
                min_stats.as_slice(),
                data_type,
            )?
            .max()?;

            acc.insert(*id, ColStats {
                min,
                max,
                null_count,
            });
            Ok(acc)
        },
    )
}
