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
use crate::operations::build_hilbert_candidates;
use crate::sessions::TableContext;
use crate::statistics::BlockOverlapDepth;
use crate::statistics::calculate_block_overlap_depths;
use crate::statistics::cluster_key_types_for_depth;
use crate::statistics::cluster_stats_for_hilbert_depth;
use crate::statistics::cluster_stats_scalar_overlap;
use crate::statistics::get_min_max_stats;
use crate::statistics::prepare_cluster_key_exprs;
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
        if output_len == 0 {
            return Ok(DataBlock::empty_with_schema(&Self::schema().into()));
        }

        let cluster_keys = tbl.resolve_cluster_keys().unwrap();
        let (stats_exprs, hilbert_len) =
            parse_cluster_keys(ctx.clone(), Arc::new(tbl.clone()), cluster_keys)?
                .into_cluster_stats_keys();
        let use_hilbert_stats = hilbert_len > 0;
        let require_scalar_overlap = stats_exprs.len() > hilbert_len;
        let scalar_cluster_key_types = if use_hilbert_stats {
            Vec::new()
        } else {
            cluster_key_types_for_depth(&stats_exprs)
        };

        let mut segment_names = Vec::with_capacity(output_len);
        let mut block_names = Vec::with_capacity(output_len);
        let mut ranges = if use_hilbert_stats {
            Vec::with_capacity(output_len)
        } else {
            Vec::with_capacity(capacity)
        };
        let mut levels = Vec::with_capacity(output_len);
        let mut pages = Vec::with_capacity(output_len);
        let mut hilbert_cluster_stats = if use_hilbert_stats {
            Vec::with_capacity(capacity)
        } else {
            Vec::new()
        };

        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), tbl.schema());
        let schema = tbl.schema();
        let prepared_cluster_key_exprs = prepare_cluster_key_exprs(&stats_exprs, schema.as_ref());

        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
        let format_vec = |v: &[Scalar]| -> String {
            use std::fmt::Write;

            let mut output = String::from("[");
            for (idx, item) in v.iter().enumerate() {
                if idx > 0 {
                    output.push_str(", ");
                }
                write!(&mut output, "{}", item).expect("write to String");
            }
            output.push(']');
            output
        };
        // block_depth is a global overlap metric. Scalar keys keep all ranges in
        // `ranges`; Hilbert keeps all depth input in `hilbert_cluster_stats` and
        // stores only displayed min/max rows in `ranges`.
        let mut row_idx = 0usize;
        for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for (i, segment) in segments.into_iter().enumerate() {
                let segment = segment?;
                let segment_loc = &chunk[i].0;

                for block in segment.blocks.iter() {
                    let block = block.as_ref();
                    let current_cluster_stats = block
                        .cluster_stats
                        .as_ref()
                        .filter(|v| v.cluster_key_id == cluster_key_id);
                    let keep_output_row = row_idx < output_len;
                    if use_hilbert_stats {
                        let stats = cluster_stats_for_hilbert_depth(
                            &prepared_cluster_key_exprs,
                            &block.col_stats,
                            current_cluster_stats,
                            cluster_key_id,
                            hilbert_len,
                        );
                        if keep_output_row {
                            ranges.push((stats.min().clone(), stats.max().clone()));
                        }
                        hilbert_cluster_stats.push(stats);
                    } else {
                        ranges.push(get_min_max_stats(
                            &prepared_cluster_key_exprs,
                            &block.col_stats,
                            current_cluster_stats,
                            Some(cluster_key_id),
                        ));
                    }

                    if keep_output_row {
                        segment_names.push(segment_loc.clone());
                        block_names.push(block.location.0.clone());
                        levels.push(current_cluster_stats.map(|v| v.level));
                        pages.push(
                            current_cluster_stats
                                .and_then(|v| v.pages.as_ref().map(|v| format_vec(v))),
                        );
                    }
                    row_idx += 1;
                }
            }
        }

        let block_depths = if use_hilbert_stats {
            build_hilbert_candidates(&hilbert_cluster_stats, |left, right| {
                !require_scalar_overlap
                    || cluster_stats_scalar_overlap(
                        &hilbert_cluster_stats[left],
                        &hilbert_cluster_stats[right],
                    )
            })
            .overlap_depths()
        } else if scalar_cluster_key_types.is_empty() {
            vec![BlockOverlapDepth::default(); ranges.len()]
        } else {
            calculate_block_overlap_depths(&ranges, &scalar_cluster_key_types)?
        };
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
