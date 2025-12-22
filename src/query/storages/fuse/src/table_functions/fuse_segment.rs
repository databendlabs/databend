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

use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::TableMetaFunc;
use crate::table_functions::TableMetaFuncTemplate;

pub struct FuseSegment;

pub type FuseSegmentFunc = TableMetaFuncTemplate<FuseSegment>;

#[async_trait::async_trait]
impl TableMetaFunc for FuseSegment {
    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("file_location", TableDataType::String),
            TableField::new(
                "format_version",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("block_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "bytes_uncompressed",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "bytes_compressed",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("index_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "bloom_index_size",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "ngram_index_size",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "inverted_index_size",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "vector_index_size",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "virtual_column_size",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "virtual_block_count",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "segment_stats_size",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
        ])
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let segment_locations = &snapshot.segments;
        let limit = limit.unwrap_or(usize::MAX);
        let len = std::cmp::min(segment_locations.len(), limit);

        let mut format_versions: Vec<u64> = Vec::with_capacity(len);
        let mut block_count: Vec<u64> = Vec::with_capacity(len);
        let mut row_count: Vec<u64> = Vec::with_capacity(len);
        let mut compressed: Vec<u64> = Vec::with_capacity(len);
        let mut uncompressed: Vec<u64> = Vec::with_capacity(len);
        let mut index_size: Vec<u64> = Vec::with_capacity(len);
        let mut bloom_index_size: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut ngram_index_size: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut inverted_index_size: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut vector_index_size: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut virtual_column_size: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut virtual_block_count: Vec<Option<u64>> = Vec::with_capacity(len);
        let mut file_location: Vec<String> = Vec::with_capacity(len);
        let mut segment_stats_size: Vec<Option<u64>> = Vec::with_capacity(len);

        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), tbl.schema());

        let mut row_num = 0;
        let mut end_flag = false;
        let chunk_size =
            std::cmp::min(ctx.get_settings().get_max_threads()? as usize * 4, len).max(1);
        for chunk in segment_locations.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<Arc<CompactSegmentInfo>>(chunk, true)
                .await?;

            for (idx, segment) in segments.into_iter().enumerate() {
                let segment = segment?;
                format_versions.push(segment_locations[idx].1);
                block_count.push(segment.summary.block_count);
                row_count.push(segment.summary.row_count);
                compressed.push(segment.summary.compressed_byte_size);
                uncompressed.push(segment.summary.uncompressed_byte_size);
                index_size.push(segment.summary.index_size);
                bloom_index_size.push(segment.summary.bloom_index_size);
                ngram_index_size.push(segment.summary.ngram_index_size);
                inverted_index_size.push(segment.summary.inverted_index_size);
                vector_index_size.push(segment.summary.vector_index_size);
                virtual_column_size.push(segment.summary.virtual_column_size);
                virtual_block_count.push(segment.summary.virtual_block_count);
                file_location.push(segment_locations[idx].0.clone());
                segment_stats_size.push(
                    segment
                        .summary
                        .additional_stats_meta
                        .as_ref()
                        .map(|v| v.size),
                );

                row_num += 1;
                if row_num >= limit {
                    end_flag = true;
                    break;
                }
            }

            if end_flag {
                break;
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(file_location),
            UInt64Type::from_data(format_versions),
            UInt64Type::from_data(block_count),
            UInt64Type::from_data(row_count),
            UInt64Type::from_data(uncompressed),
            UInt64Type::from_data(compressed),
            UInt64Type::from_data(index_size),
            UInt64Type::from_opt_data(bloom_index_size),
            UInt64Type::from_opt_data(ngram_index_size),
            UInt64Type::from_opt_data(inverted_index_size),
            UInt64Type::from_opt_data(vector_index_size),
            UInt64Type::from_opt_data(virtual_column_size),
            UInt64Type::from_opt_data(virtual_block_count),
            UInt64Type::from_opt_data(segment_stats_size),
        ]))
    }
}
