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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_sql::parse_cluster_keys;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::statistics::calculate_block_overlap_depths;
use crate::statistics::get_min_max_stats;
use crate::table_functions::TableMetaFunc;
use crate::table_functions::TableMetaFuncTemplate;

pub struct ClusteringStatistics;

pub type ClusteringStatisticsFunc = TableMetaFuncTemplate<ClusteringStatistics>;

#[async_trait::async_trait]
impl TableMetaFunc for ClusteringStatistics {
    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("segment_name", TableDataType::String),
            TableField::new("block_name", TableDataType::String),
            TableField::new("min", TableDataType::String),
            TableField::new("max", TableDataType::String),
            TableField::new(
                "level",
                TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
            ),
            TableField::new("block_depth", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("pages", TableDataType::String.wrap_nullable()),
        ])
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        // NOTE (design choice):
        // Clustering statistics are only meaningful for the current cluster key definition.
        // Historical cluster information stored in snapshots is intentionally ignored.
        //
        // Once the cluster key changes, historical cluster_stats cannot be interpreted
        // or compared correctly, so snapshots are evaluated against the live table's
        // cluster key only.
        let Some(cluster_key_id) = tbl.cluster_key_id() else {
            return Err(ErrorCode::UnclusteredTable(format!(
                "Unclustered table {}",
                tbl.get_table_info().desc,
            )));
        };

        let limit = limit.unwrap_or(usize::MAX);
        let capacity = snapshot.summary.block_count as usize;
        let output_len = std::cmp::min(capacity, limit);

        let cluster_keys = tbl.resolve_cluster_keys().unwrap();
        let exprs = parse_cluster_keys(ctx.clone(), Arc::new(tbl.clone()), cluster_keys)?;
        let cluster_key_types = exprs
            .iter()
            .map(|v| {
                let data_type = v.data_type();
                if matches!(*data_type, DataType::String) {
                    data_type.wrap_nullable()
                } else {
                    data_type.clone()
                }
            })
            .collect::<Vec<_>>();

        let mut segment_names = Vec::with_capacity(capacity);
        let mut block_names = Vec::with_capacity(capacity);
        let mut ranges = Vec::with_capacity(capacity);
        let mut levels = Vec::with_capacity(capacity);
        let mut pages = Vec::with_capacity(capacity);

        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), tbl.schema());
        let schema = tbl.schema();

        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
        let format_vec = |v: &[Scalar]| -> String {
            format!(
                "[{}]",
                v.iter()
                    .map(|item| format!("{}", item))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        // block_depth is a global overlap metric, so all block ranges must be
        // collected before LIMIT can be applied to the final output rows.
        for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for (i, segment) in segments.into_iter().enumerate() {
                let segment = segment?;
                let segment_loc = &chunk[i].0;

                for block in segment.blocks.iter() {
                    let block = block.as_ref();
                    let (min, max) = get_min_max_stats(
                        &exprs,
                        &block.col_stats,
                        block.cluster_stats.as_ref(),
                        Some(cluster_key_id),
                        schema.as_ref(),
                    );
                    let current_cluster_stats = block
                        .cluster_stats
                        .as_ref()
                        .filter(|v| v.cluster_key_id == cluster_key_id);

                    segment_names.push(segment_loc.clone());
                    block_names.push(block.location.0.clone());
                    levels.push(current_cluster_stats.map(|v| v.level));
                    pages.push(
                        current_cluster_stats.and_then(|v| v.pages.as_ref().map(|v| format_vec(v))),
                    );
                    ranges.push((min, max));
                }
            }
        }

        let block_depths = calculate_block_overlap_depths(&ranges, &cluster_key_types)?;
        let mut segment_name = StringColumnBuilder::with_capacity(output_len);
        let mut block_name = StringColumnBuilder::with_capacity(output_len);
        let mut min = Vec::with_capacity(output_len);
        let mut max = Vec::with_capacity(output_len);
        let mut level = Vec::with_capacity(output_len);
        let mut block_depth = Vec::with_capacity(output_len);
        let mut output_pages = Vec::with_capacity(output_len);

        for row_idx in 0..output_len {
            segment_name.put_and_commit(&segment_names[row_idx]);
            block_name.put_and_commit(&block_names[row_idx]);

            min.push(format_vec(&ranges[row_idx].0));
            max.push(format_vec(&ranges[row_idx].1));
            level.push(levels[row_idx]);
            block_depth.push(block_depths[row_idx].depth as u64);
            output_pages.push(pages[row_idx].clone());
        }

        Ok(DataBlock::new(
            vec![
                Column::String(segment_name.build()).into(),
                Column::String(block_name.build()).into(),
                StringType::from_data(min).into(),
                StringType::from_data(max).into(),
                Int32Type::from_opt_data(level).into(),
                UInt64Type::from_data(block_depth).into(),
                StringType::from_opt_data(output_pages).into(),
            ],
            output_len,
        ))
    }
}
