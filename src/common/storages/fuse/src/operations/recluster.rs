//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::cmp;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::SortColumnDescription;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::TableSnapshot;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SinkPipeBuilder;
use common_pipeline_transforms::processors::transforms::BlockCompactor;
use common_pipeline_transforms::processors::transforms::SortMergeCompactor;
use common_pipeline_transforms::processors::transforms::TransformCompact;
use common_pipeline_transforms::processors::transforms::TransformSortMerge;
use common_pipeline_transforms::processors::transforms::TransformSortPartial;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;

use crate::io::MetaReaders;
use crate::operations::FuseTableSink;
use crate::FuseTable;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD;
use crate::DEFAULT_ROW_PER_BLOCK;
use crate::FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_ROW_PER_BLOCK;

impl FuseTable {
    pub async fn select_blocks_for_recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        threshold: f64,
    ) -> Result<Vec<(usize, BlockMeta)>> {
        let snapshot_opt = self.read_table_snapshot(ctx.as_ref()).await?;
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no recluster.
            return Ok(vec![]);
        };

        if snapshot.summary.block_count <= 1 {
            return Ok(vec![]);
        }

        let default_cluster_key_id = snapshot
            .cluster_key_meta
            .clone()
            .ok_or_else(|| {
                ErrorCode::InvalidClusterKeys("Invalid clustering keys or table is not clustered")
            })?
            .0;

        let row_per_block =
            self.get_option(FUSE_OPT_KEY_ROW_PER_BLOCK, DEFAULT_ROW_PER_BLOCK) as u64;

        let mut blocks_map = BTreeMap::new();
        let reader = MetaReaders::segment_info_reader(ctx.as_ref());
        for (idx, segment_location) in snapshot.segments.iter().enumerate() {
            let (x, ver) = (segment_location.0.clone(), segment_location.1);
            let segment = reader.read(x, None, ver).await?;

            segment.blocks.iter().for_each(|b| {
                if let Some(stats) = &b.cluster_stats {
                    if stats.cluster_key_id == default_cluster_key_id && stats.level >= 0 {
                        blocks_map
                            .entry(stats.level)
                            .or_insert(Vec::new())
                            .push((idx, b.clone()));
                    }
                }
            });
        }

        for block_metas in blocks_map.values() {
            let mut total_row_count = 0;
            let mut points_map: BTreeMap<Vec<DataValue>, (Vec<usize>, Vec<usize>)> =
                BTreeMap::new();
            for (i, (_, meta)) in block_metas.iter().enumerate() {
                let stats = meta.cluster_stats.clone().unwrap();
                points_map
                    .entry(stats.min.clone())
                    .and_modify(|v| v.0.push(i))
                    .or_insert((vec![i], vec![]));
                points_map
                    .entry(stats.max.clone())
                    .and_modify(|v| v.1.push(i))
                    .or_insert((vec![], vec![i]));

                total_row_count += meta.row_count;
            }

            if total_row_count <= row_per_block {
                return Ok(block_metas.clone());
            }

            let mut max_depth = 0;
            let mut block_depths = Vec::new();
            let mut point_overlaps: Vec<Vec<usize>> = Vec::new();
            let mut unfinished_parts: HashMap<usize, usize> = HashMap::new();
            for (start, end) in points_map.values() {
                let point_depth = unfinished_parts.len() + start.len();
                if point_depth > max_depth {
                    max_depth = point_depth;
                }

                for (_, val) in unfinished_parts.iter_mut() {
                    *val = cmp::max(*val, point_depth);
                }

                start.iter().for_each(|&idx| {
                    unfinished_parts.insert(idx, point_depth);
                });

                point_overlaps.push(unfinished_parts.keys().cloned().collect());

                end.iter().for_each(|&idx| {
                    let stat = unfinished_parts.remove(&idx).unwrap();
                    block_depths.push(stat);
                });
            }
            assert_eq!(unfinished_parts.len(), 0);

            let sum_depth: usize = block_depths.iter().sum();
            // round the float to 4 decimal places.
            let average_depth =
                (10000.0 * sum_depth as f64 / block_depths.len() as f64).round() / 10000.0;
            if average_depth <= threshold {
                continue;
            }

            // find the max point, gather the blocks.
            let mut selected_idx = Vec::new();
            let mut find = false;
            for overlap in point_overlaps {
                if overlap.len() == max_depth {
                    let mut blocks = overlap.clone();
                    selected_idx.append(&mut blocks);
                    find = true;
                } else if find {
                    break;
                }
            }
            selected_idx.dedup();

            let selected_blocks = selected_idx
                .iter()
                .map(|idx| block_metas[*idx].clone())
                .collect::<Vec<_>>();
            return Ok(selected_blocks);
        }

        Ok(vec![])
    }

    pub fn do_recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: String,
        snapshot: &TableSnapshot,
        metas: Vec<(usize, BlockMeta)>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let partitions_total = snapshot.summary.block_count as usize;
        let (statistics, parts) =
            self.read_partitions_with_metas(ctx.clone(), None, metas, partitions_total)?;
        let table_info = self.get_table_info();
        let description = statistics.get_description(table_info);
        let plan = ReadDataSourcePlan {
            catalog,
            source_info: SourceInfo::TableSource(table_info.clone()),
            scan_fields: None,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs: None,
        };

        self.do_read2(ctx.clone(), &plan, pipeline)?;

        let cluster_stats_gen = self.get_cluster_stats_gen(ctx.clone(), pipeline)?;

        // sort
        let sort_descs: Vec<SortColumnDescription> = self
            .cluster_keys
            .iter()
            .map(|expr| SortColumnDescription {
                column_name: expr.column_name(),
                asc: true,
                nulls_first: false,
            })
            .collect();
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformSortPartial::try_create(
                transform_input_port,
                transform_output_port,
                None,
                sort_descs.clone(),
            )
        })?;
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformSortMerge::try_create(
                transform_input_port,
                transform_output_port,
                SortMergeCompactor::new(None, sort_descs.clone()),
            )
        })?;
        pipeline.resize(1)?;
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformSortMerge::try_create(
                transform_input_port,
                transform_output_port,
                SortMergeCompactor::new(None, sort_descs.clone()),
            )
        })?;

        let max_row_per_block = self.get_option(FUSE_OPT_KEY_ROW_PER_BLOCK, DEFAULT_ROW_PER_BLOCK);
        let min_rows_per_block = (max_row_per_block as f64 * 0.8) as usize;
        let max_bytes_per_block = self.get_option(
            FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD,
            DEFAULT_BLOCK_SIZE_IN_MEM_SIZE_THRESHOLD,
        );

        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        let da = ctx.get_storage_operator()?;

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformCompact::try_create(
                transform_input_port,
                transform_output_port,
                BlockCompactor::new(max_row_per_block, min_rows_per_block, max_bytes_per_block),
            )
        })?;

        let mut sink_pipeline_builder = SinkPipeBuilder::create();
        for _ in 0..pipeline.output_len() {
            let input_port = InputPort::create();
            sink_pipeline_builder.add_sink(
                input_port.clone(),
                FuseTableSink::try_create(
                    input_port,
                    ctx.clone(),
                    block_per_seg,
                    da.clone(),
                    self.meta_location_generator().clone(),
                    cluster_stats_gen.clone(),
                )?,
            );
        }

        pipeline.add_pipe(sink_pipeline_builder.finalize());
        Ok(())
    }
}
