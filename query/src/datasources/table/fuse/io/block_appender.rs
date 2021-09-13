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
use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::arrow::io::parquet::write::*;
use common_arrow::arrow::record_batch::RecordBatch;
use common_datablocks::DataBlock;
use common_datavalues::columns::DataColumn;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_store_api::BlockStream;
use futures::StreamExt;
use uuid::Uuid;

use crate::datasources::dal::DataAccessor;
use crate::datasources::table::fuse::block_location;
use crate::datasources::table::fuse::column_stats_reduce;
use crate::datasources::table::fuse::BlockLocation;
use crate::datasources::table::fuse::BlockMeta;
use crate::datasources::table::fuse::ColStats;
use crate::datasources::table::fuse::ColumnId;
use crate::datasources::table::fuse::FuseTable;
use crate::datasources::table::fuse::SegmentInfo;
use crate::datasources::table::fuse::Stats;
use crate::sessions::DatabendQueryContextRef;

impl FuseTable {
    pub async fn append_blocks(
        &self,
        ctx: DatabendQueryContextRef,
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
            let block_in_memory_size = block.memory_size() as u64;

            let data_accessor = self.data_accessor(&ctx)?;

            let part_uuid = Uuid::new_v4().to_simple().to_string() + ".parquet";
            let location = block_location(&part_uuid);

            let file_size = save_block(&schema, block, data_accessor, &location)?;

            // TODO gather parquet meta
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
                block_size: block_in_memory_size,
                col_stats,
            };

            block_metas.push(block_info);
            blocks_stats.push(blk_stats);

            summary_block_count += 1;
            summary_row_count += row_count;
            summary_compressed_byte_size += file_size;
            summary_uncompressed_byte_size += block_in_memory_size;
        }

        let summary = column_stats_reduce(blocks_stats)?;
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

pub(crate) fn save_block(
    arrow_schema: &ArrowSchema,
    block: DataBlock,
    data_accessor: Arc<dyn DataAccessor>,
    location: &str,
) -> Result<u64> {
    // TODO pick proper compression / encoding algos
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Uncompressed,
        version: Version::V2,
    };
    use std::iter::repeat;
    let encodings: Vec<_> = repeat(Encoding::Plain).take(block.num_columns()).collect();

    let batch = RecordBatch::try_from(block)?;
    let iter = vec![Ok(batch)];
    let row_groups = RowGroupIterator::try_new(iter.into_iter(), arrow_schema, options, encodings)?;
    let parquet_schema = row_groups.parquet_schema().clone();
    let mut writer = data_accessor.get_writer(location)?;

    // arrow2 convert schema to metadata, is it required?
    // -- let key_value_metadata = Some(vec![schema_to_metadata_key(schema)]);

    let len = common_arrow::parquet::write::write_file(
        &mut writer,
        row_groups,
        parquet_schema,
        options,
        None,
        None,
    )
    .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;

    Ok(len)
}
