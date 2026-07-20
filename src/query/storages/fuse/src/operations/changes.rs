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
use databend_common_expression::ORIGIN_BLOCK_ID_COLUMN_ID;
use databend_common_expression::ORIGIN_VERSION_COLUMN_ID;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::Decimal128Type;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_storages_common_table_meta::meta::BlockMeta;
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

#[derive(Clone)]
pub struct ChangesDesc {
    pub mode: StreamMode,
    pub seq: u64,
    pub location: Option<String>,
    pub desc: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamBacklog {
    // Physical rows in latest-only endpoint blocks.
    pub rows_added: u64,
    // Physical rows in base-only endpoint blocks.
    pub rows_removed: u64,
    // Estimated CDC rows the stream may need to process, bounded by the
    // physical endpoint candidates.
    pub estimated_rows: u64,
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
                let Some(base_location) = base_location else {
                    return Ok(StreamMode::AppendOnly);
                };
                let Some(latest_snapshot) = self.read_table_snapshot().await? else {
                    return Ok(StreamMode::Standard);
                };
                let base_snapshot = self.changes_read_offset_snapshot(base_location).await?;
                if logical_change_delta(Some(&base_snapshot), Some(&latest_snapshot))?
                    .is_some_and(|delta| delta == (0, 0))
                {
                    Ok(StreamMode::AppendOnly)
                } else {
                    Ok(StreamMode::Standard)
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
        let bloom_index_cols = self.bloom_index_cols();
        let ngram_args =
            Self::create_ngram_index_args(&self.table_info.meta.indexes, &self.schema(), false)?;
        let spatial_index_columns =
            Self::create_spatial_index_columns(&self.table_info.meta.indexes);

        info!(
            "[FUSE-CHANGE-TRACKING] prune snapshot block start, at node {}",
            ctx.get_cluster().local_id,
        );

        let mut pruner = FusePruner::create(
            &ctx,
            self.get_operator(),
            table_schema.clone(),
            &push_downs,
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

        let latest_segments = latest_snapshot
            .as_deref()
            .map_or_else(HashSet::new, |snapshot| {
                HashSet::from_iter(&snapshot.segments)
            });
        let base_segments = base_snapshot
            .as_deref()
            .map_or_else(HashSet::new, |snapshot| {
                HashSet::from_iter(&snapshot.segments)
            });
        // Identical endpoint segment sets produce no stream rows even if the
        // counters show intermediate changes that cancelled each other out.
        if base_segments == latest_segments {
            return Ok(StreamBacklog::default());
        }

        let logical_rows =
            logical_change_rows(base_snapshot.as_deref(), latest_snapshot.as_deref())?;
        if logical_rows
            .as_ref()
            .is_some_and(|rows| rows.inserted == 0 && rows.updated == 0 && rows.deleted == 0)
        {
            return Ok(StreamBacklog::default());
        }
        let latest_rows = latest_snapshot
            .as_deref()
            .map_or(0, |snapshot| snapshot.summary.row_count);
        let base_rows = base_snapshot
            .as_deref()
            .map_or(0, |snapshot| snapshot.summary.row_count);

        // Segment set differences bound the candidate change area. Base-only
        // segments may contain deleted rows, latest-only segments may contain
        // inserted rows, and common segments are definitely unchanged.
        let diff_in_base = base_segments
            .difference(&latest_segments)
            .map(|location| (**location).clone())
            .collect::<Vec<_>>();
        let diff_in_latest = latest_segments
            .difference(&base_segments)
            .map(|location| (**location).clone())
            .collect::<Vec<_>>();

        // Read only the changed segment metadata and compare block locations.
        // This filters out blocks that survived segment rewrites unchanged.
        let (del_blocks, add_blocks) = self
            .collect_incremental_blocks_from_diff(ctx, &diff_in_base, &diff_in_latest)
            .await?;

        // These physical block-level rows are the endpoint change candidates.
        // The stream may emit fewer rows after row-id joins and filtering.
        let candidate_added_rows = sum_block_rows(&add_blocks);
        let candidate_removed_rows = sum_block_rows(&del_blocks);

        if matches!(mode, StreamMode::AppendOnly) {
            // UPDATE does not change append-only cardinality: an inserted row
            // that is later updated is still one appended endpoint row.
            let estimated_rows = match &logical_rows {
                Some(rows) if rows.deleted == 0 || del_blocks.is_empty() => {
                    rows.inserted.min(candidate_added_rows)
                }
                Some(rows) => rows.inserted.min(estimate_append_candidate_rows(
                    seq,
                    &add_blocks,
                    &del_blocks,
                )?),
                // A legacy offset has no complete logical delta. Do not treat
                // unknown counters as zero; fall back to endpoint metadata.
                None => estimate_append_candidate_rows(seq, &add_blocks, &del_blocks)?,
            };
            return Ok(StreamBacklog {
                rows_added: candidate_added_rows,
                rows_removed: candidate_removed_rows,
                estimated_rows,
            });
        }

        let estimated_rows = if del_blocks.is_empty() {
            candidate_added_rows
        } else if add_blocks.is_empty() {
            candidate_removed_rows
        } else if let Some(rows) = &logical_rows {
            (rows.inserted + rows.updated).min(candidate_added_rows)
                + (rows.deleted + rows.updated).min(candidate_removed_rows)
        } else {
            let candidate_rows = candidate_added_rows + candidate_removed_rows;
            let changed_blocks = add_blocks.len().max(del_blocks.len()) as u64;
            latest_rows
                .abs_diff(base_rows)
                .max(changed_blocks)
                .min(candidate_rows)
        };

        Ok(StreamBacklog {
            rows_added: candidate_added_rows,
            rows_removed: candidate_removed_rows,
            estimated_rows,
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
        let Some(latest_snapshot) = self.read_table_snapshot().await? else {
            return Ok(None);
        };

        if HashSet::<&Location>::from_iter(&base_snapshot.segments)
            == HashSet::<&Location>::from_iter(&latest_snapshot.segments)
        {
            return Ok(Some(scale_snapshot_statistics(&latest_snapshot, 0)));
        }

        let base_rows = base_snapshot.summary.row_count;
        let latest_rows = latest_snapshot.summary.row_count;
        let logical_rows = logical_change_rows(Some(&base_snapshot), Some(&latest_snapshot))?;
        let Some(logical_rows) = logical_rows else {
            return Ok(Some(legacy_changes_table_statistics(
                &base_snapshot,
                &latest_snapshot,
                change_type,
            )));
        };
        let num_rows = if logical_rows.updated == 0 && logical_rows.deleted == 0 {
            match change_type {
                ChangeType::Append | ChangeType::Insert => logical_rows.inserted,
                ChangeType::Delete => 0,
            }
        } else if logical_rows.updated == 0 && logical_rows.inserted == 0 {
            match change_type {
                ChangeType::Append | ChangeType::Insert => 0,
                ChangeType::Delete => logical_rows.deleted,
            }
        } else {
            let total_rows = base_rows + logical_rows.inserted;
            let net_rows = i128::from(latest_rows) - i128::from(base_rows);
            let base_deleted = if logical_rows.deleted == 0 || total_rows == 0 {
                0
            } else {
                rounded_ratio(logical_rows.deleted, base_rows, total_rows)
                    .max(base_rows.saturating_sub(latest_rows))
                    .min(logical_rows.deleted.min(base_rows))
            };
            let insert_survived = (net_rows + i128::from(base_deleted)).max(0) as u64;
            let base_survived = base_rows - base_deleted;
            let endpoint_updated = if logical_rows.updated == 0 || total_rows == 0 {
                0
            } else {
                rounded_ratio(logical_rows.updated, base_survived, total_rows)
                    .min(logical_rows.updated)
                    .min(base_survived)
            };
            match change_type {
                ChangeType::Append => insert_survived,
                ChangeType::Insert => insert_survived + endpoint_updated,
                ChangeType::Delete => base_deleted + endpoint_updated,
            }
        };

        let source = match change_type {
            ChangeType::Append | ChangeType::Insert => &latest_snapshot,
            ChangeType::Delete => &base_snapshot,
        };
        Ok(Some(scale_snapshot_statistics(source, num_rows)))
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

#[derive(Debug, PartialEq, Eq)]
struct LogicalChangeRows {
    inserted: u64,
    updated: u64,
    deleted: u64,
}

fn logical_change_delta(
    base: Option<&TableSnapshot>,
    latest: Option<&TableSnapshot>,
) -> Result<Option<(u64, u64)>> {
    let Some((latest_updated, latest_deleted)) =
        latest.and_then(TableSnapshot::logical_change_counters)
    else {
        return Ok(None);
    };
    let (base_updated, base_deleted) = match base {
        Some(snapshot) => {
            let Some(counters) = snapshot.logical_change_counters() else {
                return Ok(None);
            };
            counters
        }
        None => (0, 0),
    };

    let updated = latest_updated
        .checked_sub(base_updated)
        .ok_or_else(|| ErrorCode::Internal("logical updated row counter decreased"))?;
    let deleted = latest_deleted
        .checked_sub(base_deleted)
        .ok_or_else(|| ErrorCode::Internal("logical deleted row counter decreased"))?;
    Ok(Some((updated, deleted)))
}

fn logical_change_rows(
    base: Option<&TableSnapshot>,
    latest: Option<&TableSnapshot>,
) -> Result<Option<LogicalChangeRows>> {
    let Some((updated, deleted)) = logical_change_delta(base, latest)? else {
        return Ok(None);
    };
    let base_rows = base.map_or(0, |snapshot| snapshot.summary.row_count);
    let latest_rows = latest.map_or(0, |snapshot| snapshot.summary.row_count);
    let inserted = (latest_rows + deleted)
        .checked_sub(base_rows)
        .ok_or_else(|| ErrorCode::Internal("invalid logical inserted row count"))?;
    Ok(Some(LogicalChangeRows {
        inserted,
        updated,
        deleted,
    }))
}

fn estimate_append_candidate_rows(
    seq: u64,
    add_blocks: &[Arc<BlockMeta>],
    del_blocks: &[Arc<BlockMeta>],
) -> Result<u64> {
    let mut base_block_ids = HashSet::with_capacity(del_blocks.len());
    for block in del_blocks {
        base_block_ids.insert(block_id_from_location(&block.location.0)?);
    }

    Ok(add_blocks.iter().fold(0_u64, |rows, block| {
        let row_count = block.row_count;
        let block_rows = if let Some(version_stats) = block.col_stats.get(&ORIGIN_VERSION_COLUMN_ID)
        {
            let null_rows = version_stats.null_count.min(row_count);
            let origin_version_before_offset = matches!(
                &version_stats.max,
                Scalar::Number(NumberScalar::UInt64(max)) if *max < seq
            );
            let origin_block_from_base = block
                .col_stats
                .get(&ORIGIN_BLOCK_ID_COLUMN_ID)
                .is_some_and(|stats| {
                    if stats.null_count > 0 {
                        return false;
                    }
                    let (
                        Scalar::Decimal(DecimalScalar::Decimal128(min, _)),
                        Scalar::Decimal(DecimalScalar::Decimal128(max, _)),
                    ) = (&stats.min, &stats.max)
                    else {
                        return false;
                    };
                    min == max && base_block_ids.contains(min)
                });
            if null_rows == row_count || origin_version_before_offset || origin_block_from_base {
                null_rows
            } else {
                row_count
            }
        } else {
            row_count
        };
        rows + block_rows
    }))
}

fn rounded_ratio(value: u64, numerator: u64, denominator: u64) -> u64 {
    if value == 0 || numerator == 0 || denominator == 0 {
        return 0;
    }
    debug_assert!(numerator <= denominator);

    let product = u128::from(value) * u128::from(numerator);
    ((product + u128::from(denominator / 2)) / u128::from(denominator)) as u64
}

fn legacy_changes_table_statistics(
    base: &TableSnapshot,
    latest: &TableSnapshot,
    change_type: ChangeType,
) -> TableStatistics {
    let base = &base.summary;
    let latest = &latest.summary;
    let divisor = match change_type {
        ChangeType::Append => 1,
        ChangeType::Insert if latest.row_count <= base.row_count => 2,
        ChangeType::Delete if latest.row_count >= base.row_count => 2,
        _ => 1,
    };
    let diff = |latest: u64, base: u64| latest.abs_diff(base) / divisor;
    let optional_diff = |latest: Option<u64>, base: Option<u64>| match (latest, base) {
        (Some(latest), base) => Some(diff(latest, base.unwrap_or_default())),
        (None, base) => base.map(|value| value / divisor),
    };

    TableStatistics {
        num_rows: Some(diff(latest.row_count, base.row_count)),
        data_size: Some(diff(
            latest.uncompressed_byte_size,
            base.uncompressed_byte_size,
        )),
        data_size_compressed: Some(diff(latest.compressed_byte_size, base.compressed_byte_size)),
        index_size: Some(diff(latest.index_size, base.index_size)),
        bloom_index_size: optional_diff(latest.bloom_index_size, base.bloom_index_size),
        ngram_index_size: optional_diff(latest.ngram_index_size, base.ngram_index_size),
        inverted_index_size: optional_diff(latest.inverted_index_size, base.inverted_index_size),
        vector_index_size: optional_diff(latest.vector_index_size, base.vector_index_size),
        virtual_column_size: optional_diff(latest.virtual_column_size, base.virtual_column_size),
        number_of_blocks: Some(diff(latest.block_count, base.block_count)),
        number_of_segments: None,
    }
}

fn scale_snapshot_statistics(snapshot: &TableSnapshot, rows: u64) -> TableStatistics {
    let summary = &snapshot.summary;
    let source_rows = summary.row_count;
    let scale = |value| {
        if value == 0 || rows == 0 || source_rows == 0 {
            0
        } else {
            debug_assert!(rows <= source_rows);
            ((u128::from(value) * u128::from(rows)) / u128::from(source_rows)) as u64
        }
    };
    TableStatistics {
        num_rows: Some(rows),
        data_size: Some(scale(summary.uncompressed_byte_size)),
        data_size_compressed: Some(scale(summary.compressed_byte_size)),
        index_size: Some(scale(summary.index_size)),
        bloom_index_size: summary.bloom_index_size.map(scale),
        ngram_index_size: summary.ngram_index_size.map(scale),
        inverted_index_size: summary.inverted_index_size.map(scale),
        vector_index_size: summary.vector_index_size.map(scale),
        virtual_column_size: summary.virtual_column_size.map(scale),
        number_of_blocks: Some(scale(summary.block_count)),
        number_of_segments: None,
    }
}

fn sum_block_rows(blocks: &[Arc<BlockMeta>]) -> u64 {
    blocks.iter().map(|block| block.row_count).sum()
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use databend_common_expression::TableSchema;
    use databend_storages_common_table_meta::meta::Statistics;
    use databend_storages_common_table_meta::meta::TableMetaTimestamps;

    use super::*;

    fn snapshot(rows: u64) -> TableSnapshot {
        TableSnapshot::try_new(
            None,
            None,
            TableSchema::default(),
            Statistics {
                row_count: rows,
                ..Default::default()
            },
            vec![],
            None,
            None,
            TableMetaTimestamps::new(None, TimeDelta::hours(1)),
        )
        .unwrap()
    }

    fn legacy_snapshot(rows: u64) -> TableSnapshot {
        let snapshot = snapshot(rows);
        let mut value = serde_json::to_value(snapshot).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("logical_change_counters");
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn test_logical_change_rows() {
        let legacy = legacy_snapshot(10);
        let base = snapshot(10);
        let mut latest = snapshot(9);
        latest.add_logical_change_delta(2, 3);

        assert_eq!(
            logical_change_rows(Some(&legacy), Some(&latest)).unwrap(),
            None
        );
        assert_eq!(
            logical_change_rows(Some(&base), Some(&latest)).unwrap(),
            Some(LogicalChangeRows {
                inserted: 2,
                updated: 2,
                deleted: 3,
            })
        );

        let mut newer_base = snapshot(10);
        newer_base.add_logical_change_delta(3, 0);
        assert!(logical_change_rows(Some(&newer_base), Some(&latest)).is_err());
    }
}
