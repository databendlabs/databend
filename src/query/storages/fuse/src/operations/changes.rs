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
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StreamTablePart;
use databend_common_catalog::plan::block_id_from_location;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BASE_BLOCK_IDS_COL_NAME;
use databend_common_expression::ColumnId;
use databend_common_expression::ORIGIN_BLOCK_ID_COLUMN_ID;
use databend_common_expression::ORIGIN_VERSION_COLUMN_ID;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::is_internal_column_id;
use databend_common_expression::is_stream_column_id;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::Decimal128Type;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::ChangeType;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING_BEGIN_VER;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_VER;
use databend_storages_common_table_meta::table::StreamMode;
use log::info;

use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::io::SnapshotsIO;
use crate::pruning::FusePruner;
use crate::statistics::reduce_block_statistics;

#[derive(Clone)]
pub struct ChangesDesc {
    pub mode: StreamMode,
    pub seq: u64,
    pub location: Option<String>,
    pub desc: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamBacklog {
    // Metadata-derived rows from latest-side changes. Standard mode reports
    // candidate block rows; append-only mode filters rewrite candidates when
    // block metadata is available.
    pub rows_added: u64,
    // Metadata-derived rows from base-side changes. Append-only streams do not
    // emit deletes, so their response keeps this at zero.
    pub rows_removed: u64,
    // Estimated rows the stream may need to process. This is not an exact stream
    // output row count; mixed updates and compaction/recluster can overstate or
    // understate it.
    pub estimated_rows: u64,
    // Estimated uncompressed bytes for estimated_rows.
    pub estimated_bytes: u64,
}

impl FuseTable {
    pub async fn get_change_descriptor(
        &self,
        ctx: &Arc<dyn TableContext>,
        append_only: bool,
        desc: String,
        navigation: Option<&NavigationPoint>,
    ) -> Result<ChangesDesc> {
        // To support analyze table, we move the change tracking check out of the function.
        let source = if let Some(point) = navigation {
            self.navigate_to_point(ctx, point).await?.as_ref().clone()
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
                        SnapshotsIO::read_snapshot(snapshot_loc.clone(), self.get_operator(), true)
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
                                concat(to_uuid(_origin_block_id), lpad(to_hex(_origin_block_row_num), 6, '0')), \
                                {append_alias}._base_row_id \
                            ) as change$row_id \
                    from {table_desc} as {append_alias} \
                    where not(is_not_null(_origin_version) and \
                              (_origin_version < {seq} or \
                               contains({append_alias}._base_block_ids, _origin_block_id)))",
                )
            }
            StreamMode::Standard => {
                let schema = self.schema();
                let fields = schema.fields();
                let quote = ctx.get_settings().get_sql_dialect()?.default_ident_quote();

                let a_table_alias = format!("_change_insert${}", suffix);
                let d_table_alias = format!("_change_delete${}", suffix);

                let mut a_cols_vec = Vec::with_capacity(fields.len());
                let mut d_alias_vec = Vec::with_capacity(fields.len());
                let mut d_cols_vec = Vec::with_capacity(fields.len());
                let mut exprs_vec = Vec::with_capacity(fields.len());
                for (idx, field) in fields.iter().enumerate() {
                    let quoted = format!("{quote}{}{quote}", field.name());
                    let d_alias = format!("d_{suffix}_{idx}");

                    a_cols_vec.push(quoted.clone());
                    d_alias_vec.push(format!("{quoted} as {d_alias}"));
                    d_cols_vec.push(d_alias.clone());

                    if field.data_type().is_nullable_or_null() {
                        exprs_vec.push(format!("not(equal_null({quoted}, {d_alias}))"));
                    } else {
                        exprs_vec.push(format!("{quoted} <> {d_alias}"));
                    }
                }
                let a_cols = a_cols_vec.join(", ");
                let d_cols_alias = d_alias_vec.join(", ");
                let d_cols = d_cols_vec.join(", ");
                let exprs = exprs_vec.join(" or ");

                let cte_name = format!("_change${}", suffix);
                format!(
                    "with {cte_name} as \
                    ( \
                        select * \
                        from ( \
                            select {a_cols}, \
                                    'INSERT' as a_change$action, \
                                    if(is_not_null(_origin_block_id), \
                                        concat(to_uuid(_origin_block_id), lpad(to_hex(_origin_block_row_num), 6, '0')), \
                                        {a_table_alias}._base_row_id \
                                    ) as a_change$row_id \
                            from {table_desc} as {a_table_alias} \
                        ) as A \
                        FULL OUTER JOIN ( \
                            select {d_cols_alias}, \
                                    'DELETE' as d_change$action, \
                                    if(is_not_null(_origin_block_id), \
                                        concat(to_uuid(_origin_block_id), lpad(to_hex(_origin_block_row_num), 6, '0')), \
                                        {d_table_alias}._base_row_id \
                                    ) as d_change$row_id \
                            from {table_desc} as {d_table_alias} \
                        ) as D \
                        on A.a_change$row_id = D.d_change$row_id \
                        where A.a_change$row_id is null or D.d_change$row_id is null or {exprs} \
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
                    Scalar::Array(Decimal128Type::from_data_with_size(base_block_ids, None));
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
        // Page-level cluster pruning was only used by the native format; for parquet
        // tables there are no cluster keys or cluster key meta to push down here.
        let cluster_keys = vec![];
        let cluster_key_meta = None;
        let bloom_index_cols = self.bloom_index_cols();
        let ngram_args =
            Self::create_ngram_index_args(&self.table_info.meta.indexes, &self.schema(), false)?;
        let spatial_index_columns =
            Self::create_spatial_index_columns(&self.table_info.meta.indexes);

        info!(
            "[FUSE-CHANGE-TRACKING] prune snapshot block start, at node {}",
            ctx.get_cluster().local_id,
        );

        let mut pruner = FusePruner::create_with_pages(
            &ctx,
            self.get_operator(),
            table_schema.clone(),
            &push_downs,
            cluster_key_meta,
            cluster_keys,
            bloom_index_cols,
            ngram_args,
            spatial_index_columns,
            None,
        )?;

        let block_metas = pruner.stream_pruning(blocks).await?;
        let pruning_stats = pruner.pruning_stats();

        info!(
            "[FUSE-CHANGE-TRACKING] prune snapshot block end, final block numbers:{}, cost:{:?}, at node {}",
            block_metas.len(),
            start.elapsed(),
            ctx.get_cluster().local_id,
        );

        let block_metas = block_metas
            .into_iter()
            .map(|(block_meta_index, block_meta)| (Some(block_meta_index), block_meta))
            .collect::<Vec<_>>();

        let (stats, parts) = self.read_partitions_with_metas(
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
                SnapshotsIO::read_snapshot(snapshot.to_string(), self.get_operator(), true).await?;
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

        let diff_in_base = base_segments
            .difference(&latest_segments)
            .cloned()
            .collect::<Vec<_>>();
        let diff_in_latest = latest_segments
            .difference(&base_segments)
            .cloned()
            .collect::<Vec<_>>();
        self.collect_incremental_blocks_from_diff(ctx, &diff_in_base, &diff_in_latest)
            .await
    }

    async fn collect_incremental_blocks_from_diff(
        &self,
        ctx: Arc<dyn TableContext>,
        diff_in_base: &[Location],
        diff_in_latest: &[Location],
    ) -> Result<(Vec<Arc<BlockMeta>>, Vec<Arc<BlockMeta>>)> {
        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
        let fuse_segment_io = SegmentsIO::create(ctx.clone(), self.get_operator(), self.schema());

        // Start from blocks that only exist in the base snapshot. Blocks with the
        // same location in the latest-side diff are unchanged blocks carried by
        // rewritten segments, so they are removed from the delete candidates.
        let mut base_blocks = HashMap::new();
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
        for chunk in diff_in_latest.chunks(chunk_size) {
            let segments = fuse_segment_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;

            for segment in segments {
                let segment = segment?;
                segment.blocks.into_iter().for_each(|block| {
                    if base_blocks.remove(&block.location).is_none() {
                        add_blocks.push(block);
                    }
                });
            }
        }

        let del_blocks = base_blocks.into_values().collect::<Vec<_>>();
        Ok((del_blocks, add_blocks))
    }

    #[async_backtrace::framed]
    pub async fn stream_backlog(
        &self,
        ctx: Arc<dyn TableContext>,
        mode: &StreamMode,
        base_location: &Option<String>,
        seq: u64,
    ) -> Result<StreamBacklog> {
        let latest_snapshot = self.read_table_snapshot().await?;
        let base_snapshot = match base_location {
            Some(location) => Some(self.changes_read_offset_snapshot(location).await?),
            None => None,
        };

        let latest_rows = latest_snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.summary.row_count);
        let base_rows = base_snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.summary.row_count);
        let latest_bytes = latest_snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.summary.uncompressed_byte_size);
        let base_bytes = base_snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.summary.uncompressed_byte_size);

        let latest_segments = latest_snapshot
            .as_ref()
            .map_or_else(HashSet::new, |snapshot| {
                HashSet::from_iter(snapshot.segments.clone())
            });
        let base_segments = base_snapshot
            .as_ref()
            .map_or_else(HashSet::new, |snapshot| {
                HashSet::from_iter(snapshot.segments.clone())
            });

        // If one snapshot's segments are a subset of the other, the change is
        // one-sided at segment level. Snapshot summary deltas are enough here,
        // and we can avoid reading segment metadata.
        if let Some(backlog) = stream_backlog_from_segment_subset(
            mode,
            latest_rows,
            base_rows,
            latest_bytes,
            base_bytes,
            base_segments.is_subset(&latest_segments),
            latest_segments.is_subset(&base_segments),
        ) {
            return Ok(backlog);
        }

        // Segment set differences bound the candidate change area. Base-only
        // segments may contain deleted rows, latest-only segments may contain
        // inserted rows, and common segments are definitely unchanged.
        let diff_in_base = base_segments
            .difference(&latest_segments)
            .cloned()
            .collect::<Vec<_>>();
        let diff_in_latest = latest_segments
            .difference(&base_segments)
            .cloned()
            .collect::<Vec<_>>();

        // Read only the changed segment metadata and compare block locations.
        // This filters out blocks that survived segment rewrites unchanged.
        let (del_blocks, add_blocks) = self
            .collect_incremental_blocks_from_diff(ctx, &diff_in_base, &diff_in_latest)
            .await?;

        // These are block-level candidate rows/bytes. Standard streams may emit
        // fewer rows after row-id joins and value comparison remove unchanged rows.
        let rows_added = add_blocks
            .iter()
            .fold(0_u64, |acc, block| acc.saturating_add(block.row_count));
        let rows_removed = del_blocks
            .iter()
            .fold(0_u64, |acc, block| acc.saturating_add(block.row_count));
        let bytes_added = add_blocks
            .iter()
            .fold(0_u64, |acc, block| acc.saturating_add(block.block_size));
        let bytes_removed = del_blocks
            .iter()
            .fold(0_u64, |acc, block| acc.saturating_add(block.block_size));

        if matches!(mode, StreamMode::AppendOnly) {
            let (rows_added, estimated_bytes) = estimate_append_only_rows_to_process(
                seq,
                latest_rows,
                base_rows,
                &add_blocks,
                &del_blocks,
            )?;
            return Ok(StreamBacklog {
                rows_added,
                rows_removed: 0,
                estimated_rows: rows_added,
                estimated_bytes,
            });
        }

        let candidate_rows = rows_added.saturating_add(rows_removed);
        let candidate_bytes = bytes_added.saturating_add(bytes_removed);
        let is_likely_physical_rewrite =
            latest_rows == base_rows && is_likely_physical_rewrite(&add_blocks, &del_blocks)?;
        let estimated_rows = estimate_standard_rows_to_process(
            latest_rows,
            base_rows,
            rows_added,
            rows_removed,
            add_blocks.len(),
            del_blocks.len(),
            is_likely_physical_rewrite,
        );
        let estimated_bytes =
            estimate_uncompressed_bytes(estimated_rows, candidate_rows, candidate_bytes);

        Ok(StreamBacklog {
            rows_added,
            rows_removed,
            estimated_rows,
            estimated_bytes,
        })
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

        let bloom_index_size = match (
            latest_summary.bloom_index_size,
            base_summary.bloom_index_size,
        ) {
            (Some(latest_size), base_size) => Some(latest_size.abs_diff(base_size.unwrap_or(0))),
            (None, base) => base,
        };
        let ngram_index_size = match (
            latest_summary.ngram_index_size,
            base_summary.ngram_index_size,
        ) {
            (Some(latest_size), base_size) => Some(latest_size.abs_diff(base_size.unwrap_or(0))),
            (None, base) => base,
        };
        let inverted_index_size = match (
            latest_summary.inverted_index_size,
            base_summary.inverted_index_size,
        ) {
            (Some(latest_size), base_size) => Some(latest_size.abs_diff(base_size.unwrap_or(0))),
            (None, base) => base,
        };
        let vector_index_size = match (
            latest_summary.vector_index_size,
            base_summary.vector_index_size,
        ) {
            (Some(latest_size), base_size) => Some(latest_size.abs_diff(base_size.unwrap_or(0))),
            (None, base) => base,
        };
        let virtual_column_size = match (
            latest_summary.virtual_column_size,
            base_summary.virtual_column_size,
        ) {
            (Some(latest_size), base_size) => Some(latest_size.abs_diff(base_size.unwrap_or(0))),
            (None, base) => base,
        };
        let number_of_blocks = latest_summary
            .block_count
            .abs_diff(base_summary.block_count);
        let max_stats = || {
            Some(TableStatistics {
                num_rows: Some(num_rows),
                data_size: Some(data_size),
                data_size_compressed: Some(data_size_compressed),
                index_size: Some(index_size),
                bloom_index_size,
                ngram_index_size,
                inverted_index_size,
                vector_index_size,
                virtual_column_size,
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
                bloom_index_size: bloom_index_size.map(|size| size / 2),
                ngram_index_size: ngram_index_size.map(|size| size / 2),
                inverted_index_size: inverted_index_size.map(|size| size / 2),
                vector_index_size: vector_index_size.map(|size| size / 2),
                virtual_column_size: virtual_column_size.map(|size| size / 2),
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
        match SnapshotsIO::read_snapshot(base_location.to_string(), self.get_operator(), true).await
        {
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
        if let Some(filters) = &mut push_downs.secure_filters {
            visit_expr_column(&mut filters.filter, base_block_ids)?;
            visit_expr_column(&mut filters.inverted_filter, base_block_ids)?;
        }
        Ok(Some(push_downs))
    } else {
        Ok(None)
    }
}

fn stream_backlog_from_segment_subset(
    mode: &StreamMode,
    latest_rows: u64,
    base_rows: u64,
    latest_bytes: u64,
    base_bytes: u64,
    base_is_subset_of_latest: bool,
    latest_is_subset_of_base: bool,
) -> Option<StreamBacklog> {
    if base_is_subset_of_latest {
        let rows_added = latest_rows.saturating_sub(base_rows);
        return Some(StreamBacklog {
            rows_added,
            rows_removed: 0,
            estimated_rows: rows_added,
            estimated_bytes: latest_bytes.saturating_sub(base_bytes),
        });
    }

    if latest_is_subset_of_base {
        if matches!(mode, StreamMode::AppendOnly) {
            return Some(StreamBacklog::default());
        }

        let rows_removed = base_rows.saturating_sub(latest_rows);
        return Some(StreamBacklog {
            rows_added: 0,
            rows_removed,
            estimated_rows: rows_removed,
            estimated_bytes: base_bytes.saturating_sub(latest_bytes),
        });
    }

    None
}

fn estimate_standard_rows_to_process(
    latest_rows: u64,
    base_rows: u64,
    rows_added: u64,
    rows_removed: u64,
    added_blocks: usize,
    removed_blocks: usize,
    is_likely_physical_rewrite: bool,
) -> u64 {
    let candidate_rows = rows_added.saturating_add(rows_removed);
    if rows_added == 0 || rows_removed == 0 {
        return candidate_rows;
    }

    if is_likely_physical_rewrite {
        return 0;
    }

    // Mixed add/delete diffs can be caused by updates or physical rewrites. The
    // exact stream row count needs row-level comparison, so use the table
    // row-count delta with at least one row per changed block, then cap it by
    // the block-level candidate count to avoid compact/recluster amplification.
    let changed_blocks = added_blocks.max(removed_blocks) as u64;
    latest_rows
        .abs_diff(base_rows)
        .max(changed_blocks)
        .min(candidate_rows)
}

fn is_likely_physical_rewrite(
    add_blocks: &[Arc<BlockMeta>],
    del_blocks: &[Arc<BlockMeta>],
) -> Result<bool> {
    if add_blocks.is_empty() || del_blocks.is_empty() {
        return Ok(false);
    }

    let rows_added = add_blocks
        .iter()
        .fold(0_u64, |acc, block| acc.saturating_add(block.row_count));
    let rows_removed = del_blocks
        .iter()
        .fold(0_u64, |acc, block| acc.saturating_add(block.row_count));
    if rows_added != rows_removed {
        return Ok(false);
    }

    let mut base_block_ids = HashSet::with_capacity(del_blocks.len());
    for block in del_blocks {
        base_block_ids.insert(block_id_from_location(&block.location.0)?);
    }

    let mut rewrites_block_layout = add_blocks.len() != del_blocks.len();

    // Pure compaction/recluster rewrites existing rows with non-null origin
    // stream columns. If a latest-side block is definitely not derived from the
    // changed base blocks, it is an inserted row candidate, not a physical
    // rewrite candidate.
    for block in add_blocks {
        let Some(version_stats) = block.col_stats.get(&ORIGIN_VERSION_COLUMN_ID) else {
            return Ok(false);
        };
        if version_stats.null_count > 0 {
            return Ok(false);
        }

        let Some((min_origin_block_id, max_origin_block_id)) = origin_block_id_range(block) else {
            return Ok(false);
        };
        rewrites_block_layout |= min_origin_block_id != max_origin_block_id;

        if origin_blocks_are_definitely_not_from_base(block, &base_block_ids) {
            return Ok(false);
        }
    }

    if !rewrites_block_layout {
        return Ok(false);
    }

    let Some(add_stats) = reduce_user_column_statistics(add_blocks) else {
        return Ok(false);
    };
    let Some(del_stats) = reduce_user_column_statistics(del_blocks) else {
        return Ok(false);
    };

    Ok(add_stats == del_stats)
}

fn reduce_user_column_statistics(
    blocks: &[Arc<BlockMeta>],
) -> Option<HashMap<ColumnId, ColumnStatistics>> {
    let mut has_user_stats = false;
    let user_stats = blocks
        .iter()
        .map(|block| {
            let stats = block
                .col_stats
                .iter()
                .filter(|(column_id, _)| {
                    !is_internal_column_id(**column_id) && !is_stream_column_id(**column_id)
                })
                .map(|(column_id, stats)| (*column_id, stats.clone()))
                .collect::<HashMap<_, _>>();
            has_user_stats |= !stats.is_empty();
            stats
        })
        .collect::<Vec<_>>();

    has_user_stats.then(|| reduce_block_statistics(&user_stats))
}

fn estimate_append_only_rows_to_process(
    seq: u64,
    latest_rows: u64,
    base_rows: u64,
    add_blocks: &[Arc<BlockMeta>],
    del_blocks: &[Arc<BlockMeta>],
) -> Result<(u64, u64)> {
    let mut base_block_ids = HashSet::with_capacity(del_blocks.len());
    for block in del_blocks {
        base_block_ids.insert(block_id_from_location(&block.location.0)?);
    }

    let mut definite_rows: u64 = 0;
    let mut definite_bytes: u64 = 0;
    let mut unknown_rows: u64 = 0;
    let mut unknown_bytes: u64 = 0;

    for block in add_blocks {
        let (block_definite_rows, block_unknown_rows) =
            estimate_append_only_block_rows(seq, &base_block_ids, block);
        definite_rows = definite_rows.saturating_add(block_definite_rows);
        definite_bytes = definite_bytes.saturating_add(estimate_uncompressed_bytes(
            block_definite_rows,
            block.row_count,
            block.block_size,
        ));
        unknown_rows = unknown_rows.saturating_add(block_unknown_rows);
        unknown_bytes = unknown_bytes.saturating_add(estimate_uncompressed_bytes(
            block_unknown_rows,
            block.row_count,
            block.block_size,
        ));
    }

    // Rows that metadata cannot classify are capped by the net row-count growth.
    // This avoids reporting UPDATE/delete-only rewrites as append-only backlog
    // while still counting pure inserted blocks above.
    let fallback_rows = latest_rows
        .saturating_sub(base_rows)
        .saturating_sub(definite_rows)
        .min(unknown_rows);
    let fallback_bytes = estimate_uncompressed_bytes(fallback_rows, unknown_rows, unknown_bytes);

    Ok((
        definite_rows.saturating_add(fallback_rows),
        definite_bytes.saturating_add(fallback_bytes),
    ))
}

fn estimate_append_only_block_rows(
    seq: u64,
    base_block_ids: &HashSet<i128>,
    block: &BlockMeta,
) -> (u64, u64) {
    let row_count = block.row_count;
    let Some(version_stats) = block.col_stats.get(&ORIGIN_VERSION_COLUMN_ID) else {
        return (row_count, 0);
    };

    let null_rows = version_stats.null_count.min(row_count);
    let non_null_rows = row_count.saturating_sub(null_rows);
    if non_null_rows == 0 {
        return (row_count, 0);
    }

    if scalar_as_u64(&version_stats.max).is_some_and(|max| max < seq) {
        return (null_rows, 0);
    }

    if origin_blocks_are_definitely_from_base(block, base_block_ids) {
        return (null_rows, 0);
    }

    if scalar_as_u64(&version_stats.min).is_some_and(|min| min >= seq)
        && origin_blocks_are_definitely_not_from_base(block, base_block_ids)
    {
        return (row_count, 0);
    }

    // Mixed-version block: min < seq <= max, and the origin block cannot be
    // resolved from metadata. Row-level `_origin_version < seq` filtering
    // cannot be replayed here, so treat non-null rows as unknown. The caller
    // caps unknown rows by net row-count growth.
    (null_rows, non_null_rows)
}

fn origin_blocks_are_definitely_from_base(
    block: &BlockMeta,
    base_block_ids: &HashSet<i128>,
) -> bool {
    if base_block_ids.is_empty() {
        return false;
    }

    let Some((min, max)) = origin_block_id_range(block) else {
        return false;
    };

    min == max && base_block_ids.contains(&min)
}

fn origin_blocks_are_definitely_not_from_base(
    block: &BlockMeta,
    base_block_ids: &HashSet<i128>,
) -> bool {
    if base_block_ids.is_empty() {
        return true;
    }

    let Some((min, max)) = origin_block_id_range(block) else {
        return false;
    };

    if min == max {
        return !base_block_ids.contains(&min);
    }

    base_block_ids.iter().all(|id| *id < min || *id > max)
}

fn origin_block_id_range(block: &BlockMeta) -> Option<(i128, i128)> {
    let stats = block.col_stats.get(&ORIGIN_BLOCK_ID_COLUMN_ID)?;
    if stats.null_count > 0 {
        return None;
    }

    Some((scalar_as_i128(&stats.min)?, scalar_as_i128(&stats.max)?))
}

fn scalar_as_u64(scalar: &Scalar) -> Option<u64> {
    let Scalar::Number(NumberScalar::UInt64(value)) = scalar else {
        return None;
    };
    Some(*value)
}

fn scalar_as_i128(scalar: &Scalar) -> Option<i128> {
    let Scalar::Decimal(DecimalScalar::Decimal128(value, _)) = scalar else {
        return None;
    };
    Some(*value)
}

fn estimate_uncompressed_bytes(rows: u64, candidate_rows: u64, candidate_bytes: u64) -> u64 {
    if rows == 0 || candidate_rows == 0 || candidate_bytes == 0 {
        return 0;
    }

    let bytes = (rows as u128).saturating_mul(candidate_bytes as u128) / candidate_rows as u128;
    bytes.min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use databend_common_catalog::plan::block_id_from_location;
    use databend_common_expression::ORIGIN_BLOCK_ID_COLUMN_ID;
    use databend_common_expression::ORIGIN_VERSION_COLUMN_ID;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::decimal::DecimalScalar;
    use databend_storages_common_table_meta::meta::BlockMeta;
    use databend_storages_common_table_meta::meta::ColumnStatistics;
    use databend_storages_common_table_meta::meta::Compression;
    use databend_storages_common_table_meta::table::StreamMode;

    use super::StreamBacklog;
    use super::estimate_append_only_rows_to_process;
    use super::estimate_standard_rows_to_process;
    use super::estimate_uncompressed_bytes;
    use super::is_likely_physical_rewrite;
    use super::stream_backlog_from_segment_subset;

    const TEST_USER_COLUMN_ID: u32 = 0;

    fn test_block_path(uuid: &str) -> String {
        format!("bucket/root/_b/{}_v2.parquet", uuid)
    }

    fn test_block_id(uuid: &str) -> i128 {
        block_id_from_location(&test_block_path(uuid)).unwrap()
    }

    fn u64_stats(min: u64, max: u64, null_count: u64) -> ColumnStatistics {
        ColumnStatistics {
            min: Scalar::Number(NumberScalar::UInt64(min)),
            max: Scalar::Number(NumberScalar::UInt64(max)),
            null_count,
            in_memory_size: 0,
            distinct_of_values: None,
        }
    }

    fn block_id_stats(min: i128, max: i128, null_count: u64) -> ColumnStatistics {
        ColumnStatistics {
            min: Scalar::Decimal(DecimalScalar::Decimal128(min, DecimalSize::default_128())),
            max: Scalar::Decimal(DecimalScalar::Decimal128(max, DecimalSize::default_128())),
            null_count,
            in_memory_size: 0,
            distinct_of_values: None,
        }
    }

    fn test_block(
        uuid: &str,
        row_count: u64,
        block_size: u64,
        col_stats: HashMap<u32, ColumnStatistics>,
    ) -> Arc<BlockMeta> {
        Arc::new(BlockMeta {
            row_count,
            block_size,
            file_size: block_size,
            col_stats,
            col_metas: HashMap::new(),
            cluster_stats: None,
            location: (test_block_path(uuid), 0),
            bloom_filter_index_location: None,
            bloom_filter_index_size: 0,
            inverted_index_size: None,
            ngram_filter_index_size: None,
            vector_index_size: None,
            vector_index_location: None,
            spatial_index_size: None,
            spatial_index_location: None,
            spatial_stats: None,
            vector_stats: None,
            virtual_block_meta: None,
            compression: Compression::None,
            create_on: None,
        })
    }

    fn inserted_block(uuid: &str, row_count: u64) -> Arc<BlockMeta> {
        test_block(uuid, row_count, row_count * 100, HashMap::new())
    }

    fn inserted_block_with_user_stats(
        uuid: &str,
        row_count: u64,
        min: u64,
        max: u64,
    ) -> Arc<BlockMeta> {
        let mut col_stats = HashMap::new();
        col_stats.insert(TEST_USER_COLUMN_ID, u64_stats(min, max, 0));
        test_block(uuid, row_count, row_count * 100, col_stats)
    }

    fn rewritten_block(
        uuid: &str,
        row_count: u64,
        origin_uuid: &str,
        origin_version: u64,
    ) -> Arc<BlockMeta> {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            ORIGIN_VERSION_COLUMN_ID,
            u64_stats(origin_version, origin_version, 0),
        );
        let origin_block_id = test_block_id(origin_uuid);
        col_stats.insert(
            ORIGIN_BLOCK_ID_COLUMN_ID,
            block_id_stats(origin_block_id, origin_block_id, 0),
        );
        test_block(uuid, row_count, row_count * 100, col_stats)
    }

    fn rewritten_block_with_user_stats(
        uuid: &str,
        row_count: u64,
        origin_uuid: &str,
        origin_version: u64,
        min: u64,
        max: u64,
    ) -> Arc<BlockMeta> {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            ORIGIN_VERSION_COLUMN_ID,
            u64_stats(origin_version, origin_version, 0),
        );
        let origin_block_id = test_block_id(origin_uuid);
        col_stats.insert(
            ORIGIN_BLOCK_ID_COLUMN_ID,
            block_id_stats(origin_block_id, origin_block_id, 0),
        );
        col_stats.insert(TEST_USER_COLUMN_ID, u64_stats(min, max, 0));
        test_block(uuid, row_count, row_count * 100, col_stats)
    }

    fn compacted_block_with_user_stats(
        uuid: &str,
        row_count: u64,
        origin_uuids: &[&str],
        origin_version: u64,
        min: u64,
        max: u64,
    ) -> Arc<BlockMeta> {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            ORIGIN_VERSION_COLUMN_ID,
            u64_stats(origin_version, origin_version, 0),
        );
        let min_origin_block_id = origin_uuids
            .iter()
            .map(|uuid| test_block_id(uuid))
            .min()
            .unwrap();
        let max_origin_block_id = origin_uuids
            .iter()
            .map(|uuid| test_block_id(uuid))
            .max()
            .unwrap();
        col_stats.insert(
            ORIGIN_BLOCK_ID_COLUMN_ID,
            block_id_stats(min_origin_block_id, max_origin_block_id, 0),
        );
        col_stats.insert(TEST_USER_COLUMN_ID, u64_stats(min, max, 0));
        test_block(uuid, row_count, row_count * 100, col_stats)
    }

    fn mixed_version_block(
        uuid: &str,
        row_count: u64,
        origin_uuid: &str,
        min_origin_version: u64,
        max_origin_version: u64,
    ) -> Arc<BlockMeta> {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            ORIGIN_VERSION_COLUMN_ID,
            u64_stats(min_origin_version, max_origin_version, 0),
        );
        let origin_block_id = test_block_id(origin_uuid);
        col_stats.insert(
            ORIGIN_BLOCK_ID_COLUMN_ID,
            block_id_stats(origin_block_id, origin_block_id, 0),
        );
        test_block(uuid, row_count, row_count * 100, col_stats)
    }

    #[test]
    fn test_stream_backlog_from_segment_subset_fast_path() {
        assert_eq!(
            stream_backlog_from_segment_subset(
                &StreamMode::Standard,
                13,
                10,
                1300,
                1000,
                true,
                false
            ),
            Some(StreamBacklog {
                rows_added: 3,
                rows_removed: 0,
                estimated_rows: 3,
                estimated_bytes: 300,
            })
        );
        assert_eq!(
            stream_backlog_from_segment_subset(
                &StreamMode::Standard,
                6,
                10,
                600,
                1000,
                false,
                true
            ),
            Some(StreamBacklog {
                rows_added: 0,
                rows_removed: 4,
                estimated_rows: 4,
                estimated_bytes: 400,
            })
        );
        assert_eq!(
            stream_backlog_from_segment_subset(
                &StreamMode::AppendOnly,
                6,
                10,
                600,
                1000,
                false,
                true
            ),
            Some(StreamBacklog::default())
        );
        assert_eq!(
            stream_backlog_from_segment_subset(
                &StreamMode::Standard,
                10,
                10,
                1000,
                1000,
                false,
                false
            ),
            None
        );
    }

    #[test]
    fn test_estimate_rows_to_process_for_standard_insert_delete() {
        assert_eq!(
            estimate_standard_rows_to_process(13, 10, 3, 0, 1, 0, false),
            3
        );
        assert_eq!(
            estimate_standard_rows_to_process(7, 10, 0, 3, 0, 1, false),
            3
        );
        assert_eq!(
            estimate_standard_rows_to_process(0, 2, 0, 2, 0, 1, false),
            2
        );
    }

    #[test]
    fn test_estimate_rows_to_process_for_standard_mixed_changes() {
        assert_eq!(
            estimate_standard_rows_to_process(10, 10, 8, 8, 5, 5, false),
            5
        );
        assert_eq!(
            estimate_standard_rows_to_process(10, 10, 6, 6, 4, 4, false),
            4
        );
        assert_eq!(
            estimate_standard_rows_to_process(10, 10, 6, 6, 4, 4, true),
            0
        );
    }

    #[test]
    fn test_estimate_rows_to_process_avoids_block_row_amplification() {
        assert_eq!(
            estimate_standard_rows_to_process(999_999, 1_000_000, 999_999, 1_000_000, 1, 1, false),
            1
        );
    }

    #[test]
    fn test_estimate_rows_to_process_ignores_likely_physical_rewrite() {
        let base_uuid_1 = "0191114d30fd78b89fae8e5c88327805";
        let base_uuid_2 = "0191114d30fd78b89fae8e5c88327806";
        let base_block_1 = inserted_block_with_user_stats(base_uuid_1, 1, 1, 1);
        let base_block_2 = inserted_block_with_user_stats(base_uuid_2, 1, 2, 2);
        let compacted = compacted_block_with_user_stats(
            "0191114d30fd78b89fae8e5c88327807",
            2,
            &[base_uuid_1, base_uuid_2],
            11,
            1,
            2,
        );

        assert!(
            is_likely_physical_rewrite(&[compacted], &[base_block_1.clone(), base_block_2.clone()])
                .unwrap()
        );
        assert_eq!(estimate_standard_rows_to_process(2, 2, 2, 2, 1, 2, true), 0);
    }

    #[test]
    fn test_estimate_rows_to_process_keeps_standard_update_candidate() {
        let base_uuid = "0191114d30fd78b89fae8e5c88327815";
        let base_block = inserted_block_with_user_stats(base_uuid, 1, 1, 1);
        let updated = rewritten_block_with_user_stats(
            "0191114d30fd78b89fae8e5c88327816",
            1,
            base_uuid,
            11,
            2,
            2,
        );

        assert!(!is_likely_physical_rewrite(&[updated], &[base_block]).unwrap());
        assert_eq!(
            estimate_standard_rows_to_process(1, 1, 1, 1, 1, 1, false),
            1
        );
    }

    #[test]
    fn test_likely_physical_rewrite_does_not_hide_one_to_one_update() {
        let base_uuid = "0191114d30fd78b89fae8e5c88327825";
        let base_block = inserted_block_with_user_stats(base_uuid, 2, 1, 2);
        let updated = rewritten_block_with_user_stats(
            "0191114d30fd78b89fae8e5c88327826",
            2,
            base_uuid,
            11,
            1,
            2,
        );

        assert!(!is_likely_physical_rewrite(&[updated], &[base_block]).unwrap());
    }

    #[test]
    fn test_estimate_uncompressed_bytes_scales_by_estimated_rows() {
        assert_eq!(estimate_uncompressed_bytes(5, 16, 1600), 500);
        assert_eq!(estimate_uncompressed_bytes(0, 16, 1600), 0);
        assert_eq!(estimate_uncompressed_bytes(5, 0, 1600), 0);
    }

    #[test]
    fn test_estimate_append_only_rows_ignores_update_rewrite_from_base() {
        let base_block = inserted_block("0191114d30fd78b89fae8e5c88327725", 1);
        let rewritten = rewritten_block(
            "0191114d30fd78b89fae8e5c88327726",
            1,
            "0191114d30fd78b89fae8e5c88327725",
            11,
        );

        let (rows, bytes) =
            estimate_append_only_rows_to_process(10, 1, 1, &[rewritten], &[base_block]).unwrap();
        assert_eq!(rows, 0);
        assert_eq!(bytes, 0);
    }

    #[test]
    fn test_estimate_append_only_rows_ignores_delete_rewrite_from_base() {
        let base_block = inserted_block("0191114d30fd78b89fae8e5c88327735", 3);
        let rewritten_survivors = rewritten_block(
            "0191114d30fd78b89fae8e5c88327736",
            2,
            "0191114d30fd78b89fae8e5c88327735",
            11,
        );

        let (rows, bytes) =
            estimate_append_only_rows_to_process(10, 2, 3, &[rewritten_survivors], &[base_block])
                .unwrap();
        assert_eq!(rows, 0);
        assert_eq!(bytes, 0);
    }

    #[test]
    fn test_estimate_append_only_rows_counts_insert_masked_by_delete() {
        let base_block = inserted_block("0191114d30fd78b89fae8e5c88327745", 1);
        let inserted = inserted_block("0191114d30fd78b89fae8e5c88327746", 1);

        let (rows, bytes) =
            estimate_append_only_rows_to_process(10, 1, 1, &[inserted], &[base_block]).unwrap();
        assert_eq!(rows, 1);
        assert_eq!(bytes, 100);
    }

    #[test]
    fn test_estimate_append_only_rows_caps_mixed_version_unknown_rows() {
        let base_block = inserted_block("0191114d30fd78b89fae8e5c88327755", 10);
        let mixed = mixed_version_block(
            "0191114d30fd78b89fae8e5c88327756",
            10,
            "0191114d30fd78b89fae8e5c88327757",
            9,
            11,
        );

        let (rows, bytes) =
            estimate_append_only_rows_to_process(10, 13, 10, &[mixed], &[base_block]).unwrap();
        assert_eq!(rows, 3);
        assert_eq!(bytes, 300);
    }
}
