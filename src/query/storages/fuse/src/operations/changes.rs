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
use std::time::Instant;

use chrono::Utc;
use databend_common_catalog::plan::block_id_from_location;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StreamTablePart;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::AbortChecker;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::Decimal128Type;
use databend_common_expression::FromData;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::BASE_BLOCK_IDS_COL_NAME;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::ChangeType;
use databend_storages_common_table_meta::table::StreamMode;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING_BEGIN_VER;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_VER;
use log::info;

use crate::io::SegmentsIO;
use crate::io::SnapshotsIO;
use crate::pruning::FusePruner;
use crate::FuseTable;

#[derive(Clone)]
pub struct ChangesDesc {
    pub mode: StreamMode,
    pub seq: u64,
    pub location: Option<String>,
    pub desc: String,
}

impl FuseTable {
    pub async fn get_change_descriptor(
        &self,
        append_only: bool,
        desc: String,
        navigation: Option<&NavigationPoint>,
        abort_checker: AbortChecker,
    ) -> Result<ChangesDesc> {
        // To support analyze table, we move the change tracking check out of the function.
        let source = if let Some(point) = navigation {
            self.navigate_to_point(point, abort_checker)
                .await?
                .as_ref()
                .clone()
        } else {
            self.clone()
        };
        let location = source.snapshot_loc();
        let seq = match navigation {
            Some(NavigationPoint::StreamInfo(info)) => info
                .options()
                .get(OPT_KEY_TABLE_VER)
                .ok_or_else(|| ErrorCode::Internal("table version must be set"))?
                .parse::<u64>()?,
            Some(_) => {
                if let Some(snapshot_loc) = &location {
                    let (snapshot, _) =
                        SnapshotsIO::read_snapshot(snapshot_loc.clone(), self.get_operator())
                            .await?;
                    let Some(prev_table_seq) = snapshot.prev_table_seq else {
                        return Err(ErrorCode::IllegalStream(
                            "The stream navigation at point has not table version".to_string(),
                        ));
                    };

                    // The table version is the version of the table when the snapshot was created.
                    // We need make sure the version greater than the table version,
                    // and less equal than the table version after the snapshot commit.
                    prev_table_seq + 1
                } else {
                    unreachable!()
                }
            }
            None => source.table_info.ident.seq,
        };

        let mode = if append_only {
            StreamMode::AppendOnly
        } else {
            StreamMode::Standard
        };
        Ok(ChangesDesc {
            mode,
            seq,
            location,
            desc,
        })
    }

    pub async fn get_changes_query(
        &self,
        ctx: Arc<dyn TableContext>,
        mode: &StreamMode,
        base_location: &Option<String>,
        table_desc: String,
        seq: u64,
    ) -> Result<String> {
        let cols = self
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();

        let suffix = format!("{:08x}", Utc::now().timestamp());

        let optimized_mode = self.optimize_stream_mode(mode, base_location).await?;
        let query = match optimized_mode {
            StreamMode::AppendOnly => {
                let append_alias = format!("_change_append${}", suffix);
                format!(
                    "select *, \
                            'INSERT' as change$action, \
                            false as change$is_update, \
                            if(is_not_null(_origin_block_id), \
                                concat(to_uuid(_origin_block_id), lpad(hex(_origin_block_row_num), 6, '0')), \
                                {append_alias}._base_row_id \
                            ) as change$row_id \
                    from {table_desc} as {append_alias} \
                    where not(is_not_null(_origin_version) and \
                              (_origin_version < {seq} or \
                               contains({append_alias}._base_block_ids, _origin_block_id)))",
                )
            }
            StreamMode::Standard => {
                let quote = ctx.get_settings().get_sql_dialect()?.default_ident_quote();

                let a_table_alias = format!("_change_insert${}", suffix);
                let d_table_alias = format!("_change_delete${}", suffix);

                let mut a_cols_vec = Vec::with_capacity(cols.len());
                let mut d_alias_vec = Vec::with_capacity(cols.len());
                let mut d_cols_vec = Vec::with_capacity(cols.len());
                for col in cols {
                    a_cols_vec.push(format!("{quote}{col}{quote}"));
                    d_alias_vec.push(format!("{quote}{col}{quote} as d_{col}"));
                    d_cols_vec.push(format!("d_{col}"));
                }
                let a_cols = a_cols_vec.join(", ");
                let d_cols_alias = d_alias_vec.join(", ");
                let d_cols = d_cols_vec.join(", ");

                let cte_name = format!("_change${}", suffix);
                format!(
                    "with {cte_name} as materialized \
                    ( \
                        select * \
                        from ( \
                            select {a_cols}, \
                                    _row_version, \
                                    'INSERT' as a_change$action, \
                                    if(is_not_null(_origin_block_id), \
                                        concat(to_uuid(_origin_block_id), lpad(hex(_origin_block_row_num), 6, '0')), \
                                        {a_table_alias}._base_row_id \
                                    ) as a_change$row_id \
                            from {table_desc} as {a_table_alias} \
                        ) as A \
                        FULL OUTER JOIN ( \
                            select {d_cols_alias}, \
                                    _row_version, \
                                    'DELETE' as d_change$action, \
                                    if(is_not_null(_origin_block_id), \
                                        concat(to_uuid(_origin_block_id), lpad(hex(_origin_block_row_num), 6, '0')), \
                                        {d_table_alias}._base_row_id \
                                    ) as d_change$row_id \
                            from {table_desc} as {d_table_alias} \
                        ) as D \
                        on A.a_change$row_id = D.d_change$row_id \
                        where A.a_change$row_id is null or D.d_change$row_id is null or A._row_version > D._row_version \
                    ) \
                    select {a_cols}, \
                            a_change$action as change$action, \
                            a_change$row_id as change$row_id, \
                            d_change$action is not null as change$is_update \
                    from {cte_name} \
                    where a_change$action is not null \
                    union all \
                    select {d_cols}, \
                            d_change$action, \
                            d_change$row_id, \
                            a_change$action is not null \
                    from {cte_name} \
                    where d_change$action is not null",
                )
            }
        };
        Ok(query)
    }

    async fn optimize_stream_mode(
        &self,
        mode: &StreamMode,
        base_location: &Option<String>,
    ) -> Result<StreamMode> {
        match mode {
            StreamMode::AppendOnly => Ok(StreamMode::AppendOnly),
            StreamMode::Standard => {
                if let Some(base_location) = base_location {
                    if let Some(latest_snapshot) = self.read_table_snapshot().await? {
                        let latest_segments: HashSet<&Location> =
                            HashSet::from_iter(&latest_snapshot.segments);

                        let base_snapshot =
                            self.changes_read_offset_snapshot(base_location).await?;
                        let base_segments = HashSet::from_iter(&base_snapshot.segments);

                        // If the base segments are a subset of the latest segments,
                        // then the stream is treated as append only.
                        if base_segments.is_subset(&latest_segments) {
                            Ok(StreamMode::AppendOnly)
                        } else {
                            Ok(StreamMode::Standard)
                        }
                    } else {
                        Ok(StreamMode::Standard)
                    }
                } else {
                    Ok(StreamMode::AppendOnly)
                }
            }
        }
    }

    pub async fn do_read_changes_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        change_type: ChangeType,
        base_location: &Option<String>,
    ) -> Result<(PartStatistics, Partitions)> {
        let start = Instant::now();

        let (del_blocks, add_blocks) = self
            .collect_incremental_blocks(ctx.clone(), base_location)
            .await?;

        let mut push_downs = push_downs;
        let (blocks, base_block_ids_scalar) = match change_type {
            ChangeType::Append => {
                let mut base_block_ids = Vec::with_capacity(del_blocks.len());
                for base_block in del_blocks {
                    let block_id = block_id_from_location(&base_block.location.0)?;
                    base_block_ids.push(block_id);
                }
                let base_block_ids_scalar =
                    Scalar::Array(Decimal128Type::from_data(base_block_ids));
                push_downs = replace_push_downs(push_downs, &base_block_ids_scalar)?;
                (add_blocks, Some(base_block_ids_scalar))
            }
            ChangeType::Insert => (add_blocks, None),
            ChangeType::Delete => (del_blocks, None),
        };

        let summary = blocks.len();
        if summary == 0 {
            return Ok((PartStatistics::default(), Partitions::default()));
        }

        let table_schema = self.schema_with_stream();
        let (cluster_keys, cluster_key_meta) =
            if !self.is_native() || self.cluster_key_meta().is_none() {
                (vec![], None)
            } else {
                (
                    self.linear_cluster_keys(ctx.clone()),
                    self.cluster_key_meta(),
                )
            };
        let bloom_index_cols = self.bloom_index_cols();
        let mut pruner = FusePruner::create_with_pages(
            &ctx,
            self.get_operator(),
            table_schema.clone(),
            &push_downs,
            cluster_key_meta,
            cluster_keys,
            bloom_index_cols,
            None,
        )?;

        let block_metas = pruner.stream_pruning(blocks).await?;
        let pruning_stats = pruner.pruning_stats();

        info!(
            "prune snapshot block end, final block numbers:{}, cost:{:?}",
            block_metas.len(),
            start.elapsed()
        );

        let block_metas = block_metas
            .into_iter()
            .map(|(block_meta_index, block_meta)| (Some(block_meta_index), block_meta))
            .collect::<Vec<_>>();

        let (stats, parts) = self.read_partitions_with_metas(
            ctx.clone(),
            table_schema,
            push_downs,
            &block_metas,
            summary,
            pruning_stats,
        )?;
        if let Some(base_block_ids_scalar) = base_block_ids_scalar {
            let wrapper =
                Partitions::create(PartitionsShuffleKind::Seq, vec![StreamTablePart::create(
                    parts,
                    base_block_ids_scalar,
                )]);
            Ok((stats, wrapper))
        } else {
            Ok((stats, parts))
        }
    }

    async fn collect_incremental_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        base: &Option<String>,
    ) -> Result<(Vec<Arc<BlockMeta>>, Vec<Arc<BlockMeta>>)> {
        let latest = self.snapshot_loc();

        let latest_segments = if let Some(snapshot) = latest {
            let (sn, _) =
                SnapshotsIO::read_snapshot(snapshot.to_string(), self.get_operator()).await?;
            HashSet::from_iter(sn.segments.clone())
        } else {
            HashSet::new()
        };

        let base_segments = if let Some(snapshot) = base {
            let sn = self.changes_read_offset_snapshot(snapshot).await?;
            HashSet::from_iter(sn.segments.clone())
        } else {
            HashSet::new()
        };

        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;

        let fuse_segment_io = SegmentsIO::create(ctx.clone(), self.get_operator(), self.schema());

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
        Ok((del_blocks, add_blocks))
    }

    pub fn check_changes_valid(&self, desc: &str, seq: u64) -> Result<()> {
        if !self.change_tracking_enabled() {
            return Err(ErrorCode::IllegalStream(format!(
                "Change tracking is not enabled on table {desc}",
            )));
        }

        if let Some(value) = self
            .table_info
            .options()
            .get(OPT_KEY_CHANGE_TRACKING_BEGIN_VER)
        {
            let begin_version = value.parse::<u64>()?;
            if begin_version > seq {
                return Err(ErrorCode::IllegalStream(format!(
                    "Change tracking has been missing for the time range requested on table {desc}",
                )));
            }
        }
        Ok(())
    }

    pub async fn changes_table_statistics(
        &self,
        ctx: Arc<dyn TableContext>,
        base_location: &Option<String>,
        change_type: ChangeType,
    ) -> Result<Option<TableStatistics>> {
        let Some(base_location) = base_location else {
            return self.table_statistics(ctx, true, None).await;
        };

        let base_snapshot = self.changes_read_offset_snapshot(base_location).await?;
        let base_summary = base_snapshot.summary.clone();
        let latest_summary = if let Some(snapshot) = self.read_table_snapshot().await? {
            snapshot.summary.clone()
        } else {
            return Ok(None);
        };

        let num_rows = latest_summary.row_count.abs_diff(base_summary.row_count);
        let data_size = latest_summary
            .uncompressed_byte_size
            .abs_diff(base_summary.uncompressed_byte_size);
        let data_size_compressed = latest_summary
            .compressed_byte_size
            .abs_diff(base_summary.compressed_byte_size);
        let index_size = latest_summary.index_size.abs_diff(base_summary.index_size);
        let number_of_blocks = latest_summary
            .block_count
            .abs_diff(base_summary.block_count);
        let max_stats = || {
            Some(TableStatistics {
                num_rows: Some(num_rows),
                data_size: Some(data_size),
                data_size_compressed: Some(data_size_compressed),
                index_size: Some(index_size),
                number_of_blocks: Some(number_of_blocks),
                number_of_segments: None,
            })
        };
        // The following statistics are predicted, which may have a large bias;
        // mainly used to determine the join order
        let min_stats = || {
            Some(TableStatistics {
                num_rows: Some(num_rows / 2),
                data_size: Some(data_size / 2),
                data_size_compressed: Some(data_size_compressed / 2),
                index_size: Some(index_size / 2),
                number_of_blocks: Some(number_of_blocks / 2),
                number_of_segments: None,
            })
        };
        match change_type {
            ChangeType::Append => Ok(max_stats()),
            ChangeType::Insert => {
                // If the number of rows is greater than the base,
                // it means that the insertion is more than the deletion.
                if latest_summary.row_count > base_summary.row_count {
                    Ok(max_stats())
                } else {
                    Ok(min_stats())
                }
            }
            ChangeType::Delete => {
                // If the number of rows is less than the base,
                // it means that the deletion is more than the insertion.
                if latest_summary.row_count < base_summary.row_count {
                    Ok(max_stats())
                } else {
                    Ok(min_stats())
                }
            }
        }
    }

    pub async fn changes_read_offset_snapshot(
        &self,
        base_location: &String,
    ) -> Result<Arc<TableSnapshot>> {
        match SnapshotsIO::read_snapshot(base_location.to_string(), self.get_operator()).await {
            Ok((base_snapshot, _)) => Ok(base_snapshot),
            Err(_) => Err(ErrorCode::IllegalStream(format!(
                "Failed to read the offset snapshot: {:?}, maybe purged",
                base_location
            ))),
        }
    }
}

fn replace_push_downs(
    push_downs: Option<PushDownInfo>,
    base_block_ids: &Scalar,
) -> Result<Option<PushDownInfo>> {
    fn visit_expr_column(expr: &mut RemoteExpr<String>, base_block_ids: &Scalar) -> Result<()> {
        match expr {
            RemoteExpr::ColumnRef {
                span,
                id,
                data_type,
                ..
            } => {
                if id == BASE_BLOCK_IDS_COL_NAME {
                    *expr = RemoteExpr::Constant {
                        span: *span,
                        scalar: base_block_ids.clone(),
                        data_type: data_type.clone(),
                    };
                }
            }
            RemoteExpr::Cast { expr, .. } => {
                visit_expr_column(expr, base_block_ids)?;
            }
            RemoteExpr::FunctionCall { args, .. } => {
                for arg in args.iter_mut() {
                    visit_expr_column(arg, base_block_ids)?;
                }
            }
            _ => (),
        }
        Ok(())
    }

    if let Some(mut push_downs) = push_downs {
        if let Some(filters) = &mut push_downs.filters {
            visit_expr_column(&mut filters.filter, base_block_ids)?;
            visit_expr_column(&mut filters.inverted_filter, base_block_ids)?;
        }
        Ok(Some(push_downs))
    } else {
        Ok(None)
    }
}
