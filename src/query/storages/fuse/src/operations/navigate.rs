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

use chrono::DateTime;
use chrono::Utc;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_meta_app::storage::S3StorageClass;
use databend_common_sql::ApproxDistinctColumns;
use databend_common_sql::BloomIndexColumns;
use databend_common_sql::check_table_ref_access;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::VACUUM2_OBJECT_KEY_PREFIX;
use databend_storages_common_table_meta::table::OPT_KEY_APPROX_DISTINCT_COLUMNS;
use databend_storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_SOURCE_TABLE_ID;
use futures::TryStreamExt;
use log::info;
use opendal::EntryMode;

use crate::FUSE_TBL_SNAPSHOT_PREFIX;
use crate::FuseTable;
use crate::fuse_table::RetentionPolicy;
use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::io::SnapshotsIO;
use crate::io::TableMetaLocationGenerator;

impl FuseTable {
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn navigate_to_point(
        &self,
        ctx: &Arc<dyn TableContext>,
        point: &NavigationPoint,
    ) -> Result<Arc<FuseTable>> {
        match point {
            NavigationPoint::SnapshotID(snapshot_id) => {
                self.navigate_to_snapshot(ctx, snapshot_id.as_str()).await
            }
            NavigationPoint::TimePoint(time_point) => {
                let Some(location) = self.snapshot_loc() else {
                    return Err(ErrorCode::TableHistoricalDataNotFound(
                        "Empty Table has no historical data",
                    ));
                };
                self.navigate_to_time_point(ctx, location, *time_point)
                    .await
            }
            NavigationPoint::StreamInfo(info) => {
                let location = self.stream_snapshot_location(info)?;
                self.load_table_by_location(ctx, location).await
            }
            NavigationPoint::TableTag(tag_name) => {
                let snapshot_loc = self.get_tag_snapshot_location(ctx, tag_name).await?;
                let (snapshot, format_version) =
                    SnapshotsIO::read_snapshot(snapshot_loc, self.get_operator(), true).await?;
                self.load_table_by_snapshot(
                    snapshot.as_ref(),
                    format_version,
                    ctx.get_settings().get_s3_storage_class()?,
                )
            }
        }
    }

    #[async_backtrace::framed]
    async fn load_table_by_location(
        &self,
        ctx: &Arc<dyn TableContext>,
        location: Option<String>,
    ) -> Result<Arc<FuseTable>> {
        let Some(snapshot_loc) = location else {
            let mut table_info = self.table_info.clone();
            table_info.meta.options.remove(OPT_KEY_SNAPSHOT_LOCATION);
            table_info.meta.statistics = TableStatistics::default();
            let table = FuseTable::create_without_refresh_table_info(
                table_info,
                ctx.get_settings().get_s3_storage_class()?,
            )?;
            return Ok(table.into());
        };
        let (snapshot, format_version) =
            SnapshotsIO::read_snapshot(snapshot_loc.clone(), self.get_operator(), true).await?;
        self.load_table_by_snapshot(
            snapshot.as_ref(),
            format_version,
            ctx.get_settings().get_s3_storage_class()?,
        )
    }

    #[async_backtrace::framed]
    pub async fn navigate_to_time_point(
        &self,
        ctx: &Arc<dyn TableContext>,
        location: String,
        time_point: DateTime<Utc>,
    ) -> Result<Arc<FuseTable>> {
        self.find(ctx, location, |snapshot| {
            if let Some(ts) = snapshot.timestamp {
                ts <= time_point
            } else {
                false
            }
        })
        .await
    }

    pub async fn navigate_back_with_limit(
        &self,
        ctx: &Arc<dyn TableContext>,
        location: String,
        limit: usize,
    ) -> Result<Arc<FuseTable>> {
        let mut counter = 0;
        self.find(ctx, location, |_snapshot| {
            counter += 1;
            counter >= limit
        })
        .await
    }

    #[async_backtrace::framed]
    async fn navigate_to_snapshot(
        &self,
        ctx: &Arc<dyn TableContext>,
        snapshot_id: &str,
    ) -> Result<Arc<FuseTable>> {
        let Some(location) = self.snapshot_loc() else {
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "Empty Table has no historical data",
            ));
        };

        self.find(ctx, location, |snapshot| {
            snapshot
                .snapshot_id
                .simple()
                .to_string()
                .as_str()
                .starts_with(snapshot_id)
        })
        .await
    }

    #[async_backtrace::framed]
    pub async fn find<P>(
        &self,
        ctx: &Arc<dyn TableContext>,
        location: String,
        pred: P,
    ) -> Result<Arc<FuseTable>>
    where
        P: FnMut(&TableSnapshot) -> bool,
    {
        if let Some((snapshot, format_version)) =
            self.find_snapshot_with_version(ctx, location, pred).await?
        {
            self.load_table_by_snapshot(
                snapshot.as_ref(),
                format_version,
                ctx.get_settings().get_s3_storage_class()?,
            )
        } else {
            Err(ErrorCode::TableHistoricalDataNotFound(
                "No historical data found at given point",
            ))
        }
    }

    /// Load the table instance by the snapshot
    pub fn load_table_by_snapshot(
        &self,
        snapshot: &TableSnapshot,
        format_version: u64,
        s3_storage_class: S3StorageClass,
    ) -> Result<Arc<FuseTable>> {
        // The `seq` of ident that we cloned here is JUST a place holder
        // we should NOT use it other than a pure place holder.
        let mut table_info = self.table_info.clone();

        // There are more to be kept in snapshot, like engine_options, ordering keys...
        // or we could just keep a clone of TableMeta in the snapshot.
        //
        // currently, here are what we can recovery from the snapshot:

        // 1. the table schema
        // 2. the table option `snapshot_location`
        let loc = self
            .meta_location_generator
            .gen_snapshot_location(&snapshot.snapshot_id, format_version)?;

        self.apply_navigation_metadata(&mut table_info.meta, snapshot)?;
        table_info
            .meta
            .options
            .insert(OPT_KEY_SNAPSHOT_LOCATION.to_owned(), loc);
        self.apply_snapshot_statistics(&mut table_info.meta, snapshot);

        // let's instantiate it
        let table = FuseTable::create_without_refresh_table_info(table_info, s3_storage_class)?;
        Ok(table.into())
    }

    #[async_backtrace::framed]
    pub async fn navigate_for_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        navigation_point: Option<NavigationPoint>,
    ) -> Result<(Arc<FuseTable>, Vec<String>)> {
        let retention_policy = self.get_data_retention_policy(ctx.as_ref())?;
        let root_snapshot = if let Some(snapshot) = self.read_table_snapshot().await? {
            snapshot
        } else {
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "No historical data found at given point",
            ));
        };

        assert!(root_snapshot.timestamp.is_some());

        match retention_policy {
            RetentionPolicy::ByTimePeriod(time_delta) => {
                info!("navigate by time period, {:?}", time_delta);
                let mut time_point = root_snapshot.timestamp.unwrap() - time_delta;
                let (candidate_snapshot_path, files) = match navigation_point {
                    Some(NavigationPoint::TimePoint(point)) => {
                        time_point = std::cmp::min(point, time_point);
                        self.list_by_time_point(time_point).await
                    }
                    Some(NavigationPoint::SnapshotID(snapshot_id)) => {
                        self.list_by_snapshot_id(snapshot_id.as_str(), time_point)
                            .await
                    }
                    Some(NavigationPoint::StreamInfo(info)) => {
                        self.list_by_stream(info, time_point).await
                    }
                    Some(NavigationPoint::TableTag(tag_name)) => {
                        let snapshot_loc = self.get_tag_snapshot_location(ctx, &tag_name).await?;
                        self.list_by_location(snapshot_loc, time_point).await
                    }
                    None => self.list_by_time_point(time_point).await,
                }?;

                let table = self
                    .navigate_to_time_point(ctx, candidate_snapshot_path, time_point)
                    .await?;

                Ok((table, files))
            }
            RetentionPolicy::ByNumOfSnapshotsToKeep(num) => {
                assert!(num > 0);
                info!("navigate by number of snapshots, {:?}", num);
                let table = self
                    .navigate_back_with_limit(ctx, self.snapshot_loc().unwrap(), num)
                    .await?;

                // Safe to unwrap: table snapshot and snapshot timestamp exist, otherwise we should not be here
                let timestamp = table
                    .read_table_snapshot()
                    .await?
                    .unwrap()
                    .timestamp
                    .unwrap();

                let (_candidate_snapshot_path, files) = self.list_by_time_point(timestamp).await?;

                Ok((table, files))
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn list_by_time_point(
        &self,
        time_point: DateTime<Utc>,
    ) -> Result<(String, Vec<String>)> {
        let Some(location) = self.snapshot_loc() else {
            return Err(ErrorCode::TableHistoricalDataNotFound("No historical data"));
        };

        let prefix = self.snapshot_prefix();

        let files = self
            .list_files(prefix, |_, modified| modified <= time_point)
            .await?;
        if files.is_empty() {
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "No historical data found at given point",
            ));
        }

        Ok((location, files))
    }

    #[async_backtrace::framed]
    pub async fn list_by_snapshot_id(
        &self,
        snapshot_id: &str,
        retention_point: DateTime<Utc>,
    ) -> Result<(String, Vec<String>)> {
        // TODO(Sky): unify location related logic into a single place
        let mut location = None;
        let prefix = self.snapshot_prefix();
        let prefix_loc = format!("{}{}", prefix, snapshot_id);
        let prefix_loc_v5 = format!("{}{}{}", prefix, VACUUM2_OBJECT_KEY_PREFIX, snapshot_id);

        let files = self
            .list_files(prefix, |loc, modified| {
                if loc.starts_with(&prefix_loc) || loc.starts_with(&prefix_loc_v5) {
                    location = Some(loc);
                }
                modified <= retention_point
            })
            .await?;
        let location = location.ok_or_else(|| {
            ErrorCode::TableHistoricalDataNotFound("No historical data found at given point")
        })?;
        Ok((location, files))
    }

    #[async_backtrace::framed]
    async fn list_by_stream(
        &self,
        stream_info: TableInfo,
        retention_point: DateTime<Utc>,
    ) -> Result<(String, Vec<String>)> {
        let snapshot_loc = self
            .stream_snapshot_location(&stream_info)?
            .ok_or_else(|| {
                ErrorCode::TableHistoricalDataNotFound("No historical data found at given point")
            })?;
        self.list_by_location(snapshot_loc, retention_point).await
    }

    #[async_backtrace::framed]
    async fn list_by_location(
        &self,
        snapshot_loc: String,
        retention_point: DateTime<Utc>,
    ) -> Result<(String, Vec<String>)> {
        let mut found = false;
        let prefix = self.snapshot_prefix();

        let files = self
            .list_files(prefix, |loc, modified| {
                if loc == snapshot_loc {
                    found = true;
                }
                modified <= retention_point
            })
            .await?;

        if !found {
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "No historical data found at given point",
            ));
        }
        Ok((snapshot_loc, files))
    }

    #[async_backtrace::framed]
    pub async fn list_files<F>(&self, prefix: String, mut f: F) -> Result<Vec<String>>
    where F: FnMut(String, DateTime<Utc>) -> bool {
        let mut file_list = vec![];
        let op = self.operator.clone();
        let mut ds = op.lister_with(&prefix).await?;
        while let Some(de) = ds.try_next().await? {
            let meta = de.metadata();
            match meta.mode() {
                EntryMode::FILE => {
                    let modified = if let Some(v) = meta.last_modified() {
                        Some(v)
                    } else {
                        let meta = op.stat(de.path()).await?;
                        meta.last_modified()
                    };

                    let location = de.path().to_string();
                    if let Some(modified) = modified {
                        if f(location.clone(), modified) {
                            file_list.push((location, modified));
                        }
                    }
                }
                _ => {
                    continue;
                }
            }
        }

        file_list.sort_by(|(_, m1), (_, m2)| m2.cmp(m1));

        Ok(file_list.into_iter().map(|v| v.0).collect())
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn navigate_to_location(
        &self,
        ctx: Arc<dyn TableContext>,
        point: &NavigationPoint,
    ) -> Result<Option<String>> {
        match point {
            NavigationPoint::SnapshotID(snapshot_id) => {
                // Because the user explicitly asked for a specific snapshot,
                // we treat "not found" as an error instead of silently returning None.
                let Some(location) = self.snapshot_loc() else {
                    return Err(ErrorCode::TableHistoricalDataNotFound(
                        "Empty Table has no historical data",
                    ));
                };
                let loc = self
                    .find_location(&ctx, location, |snapshot| {
                        snapshot
                            .snapshot_id
                            .simple()
                            .to_string()
                            .as_str()
                            .starts_with(snapshot_id)
                    })
                    .await?;
                Ok(Some(loc))
            }
            NavigationPoint::TimePoint(time_point) => {
                // This allows users to query historical states gracefully even if
                // the table was created *after* the given time.
                let Some(location) = self.snapshot_loc() else {
                    return Ok(None);
                };
                let loc = self
                    .find_location(&ctx, location, |snapshot| {
                        if let Some(ts) = snapshot.timestamp {
                            ts <= *time_point
                        } else {
                            false
                        }
                    })
                    .await
                    .ok();
                Ok(loc)
            }
            NavigationPoint::StreamInfo(stream_info) => {
                let options = stream_info.options();
                let stream_table_id = options
                    .get(OPT_KEY_SOURCE_TABLE_ID)
                    .ok_or_else(|| ErrorCode::Internal("table id must be set"))?
                    .parse::<u64>()?;
                if stream_table_id != self.table_info.ident.table_id {
                    return Err(ErrorCode::IllegalStream(format!(
                        "The stream '{}' is not match the table '{}'",
                        stream_info.desc, self.table_info.desc
                    )));
                }
                Ok(options.get(OPT_KEY_SNAPSHOT_LOCATION).cloned())
            }
            NavigationPoint::TableTag(tag_name) => {
                let snapshot_loc = self.get_tag_snapshot_location(&ctx, tag_name).await?;
                Ok(Some(snapshot_loc))
            }
        }
    }

    // Only used when the table branch is none.
    #[async_backtrace::framed]
    pub async fn find_location<P>(
        &self,
        ctx: &Arc<dyn TableContext>,
        location: String,
        pred: P,
    ) -> Result<String>
    where
        P: FnMut(&TableSnapshot) -> bool,
    {
        let Some((snapshot, format_version)) =
            self.find_snapshot_with_version(ctx, location, pred).await?
        else {
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "No historical data found at given point",
            ));
        };

        let snapshot_location = self
            .meta_location_generator
            .gen_snapshot_location(&snapshot.snapshot_id, format_version)?;
        Ok(snapshot_location)
    }

    fn snapshot_prefix(&self) -> String {
        format!(
            "{}/{}/",
            self.meta_location_generator().prefix(),
            FUSE_TBL_SNAPSHOT_PREFIX,
        )
    }

    fn stream_snapshot_location(&self, stream_info: &TableInfo) -> Result<Option<String>> {
        let options = stream_info.options();
        let stream_table_id = options
            .get(OPT_KEY_SOURCE_TABLE_ID)
            .ok_or_else(|| ErrorCode::Internal("table id must be set"))?
            .parse::<u64>()?;
        if stream_table_id != self.table_info.ident.table_id {
            return Err(ErrorCode::IllegalStream(format!(
                "The stream '{}' is not match the table '{}'",
                stream_info.desc, self.table_info.desc
            )));
        }
        Ok(options.get(OPT_KEY_SNAPSHOT_LOCATION).cloned())
    }

    async fn find_snapshot_with_version<P>(
        &self,
        ctx: &Arc<dyn TableContext>,
        location: String,
        mut pred: P,
    ) -> Result<Option<(Arc<TableSnapshot>, u64)>>
    where
        P: FnMut(&TableSnapshot) -> bool,
    {
        let abort_checker = ctx.clone().get_abort_checker();
        let snapshot_version = TableMetaLocationGenerator::snapshot_version(location.as_str());
        let reader = MetaReaders::table_snapshot_reader(self.get_operator());
        // grab the table history as stream
        // snapshots are order by timestamp DESC.
        let mut snapshot_stream = reader.snapshot_history(
            location,
            snapshot_version,
            self.meta_location_generator().clone(),
        );

        // Find the snapshot which matches the given `time_point`.
        while let Some((snapshot, format_version)) = snapshot_stream.try_next().await? {
            abort_checker
                .try_check_aborting()
                .with_context(|| "failed to find snapshot")?;
            if pred(snapshot.as_ref()) {
                return Ok(Some((snapshot, format_version)));
            }
        }

        Ok(None)
    }

    async fn get_tag_snapshot_location(
        &self,
        ctx: &Arc<dyn TableContext>,
        tag_name: &str,
    ) -> Result<String> {
        check_table_ref_access(ctx.as_ref())?;
        // Tags are only stored on the base table. Reject tag navigation on branches
        // to avoid confusing "Unknown TAG" errors.
        if self.is_table_branch() {
            return Err(ErrorCode::Unimplemented(
                "tag navigation is not supported on table branches",
            ));
        }
        let catalog = ctx.get_catalog(self.table_info.catalog()).await?;
        let table_tag = catalog
            .get_table_tag(self.table_info.ident.table_id, tag_name, false)
            .await?
            .ok_or_else(|| {
                ErrorCode::UnknownReference(format!(
                    "Unknown TAG '{}' in table {}",
                    tag_name, self.table_info.desc
                ))
            })?;
        Ok(table_tag.data.snapshot_loc)
    }

    pub(crate) fn apply_snapshot_metadata_to_meta(
        &self,
        table_meta: &mut TableMeta,
        snapshot: &TableSnapshot,
    ) -> Result<bool> {
        let snapshot_cluster_key_meta = snapshot.cluster_key_meta.clone();
        let cluster_type = if let Some(cluster_type) = snapshot.cluster_type {
            Some(cluster_type)
        } else if snapshot_cluster_key_meta == self.table_info.meta.cluster_key_v2 {
            // Historical snapshots written before cluster_type was persisted may still share the
            // same cluster-key version as the current table. Reuse the current cluster_type only
            // in that compatibility case.
            self.cluster_type()
        } else {
            // Intentionally do not expose partial cluster metadata here. Historical snapshots
            // written before cluster_type persistence may only carry cluster_key_meta, which is
            // not enough to safely reconstruct the exact historical clustering mode once the
            // table has moved to a different cluster-key generation or non-linear clustering
            // implementation. Clearing both cluster key and cluster type in that case is a
            // deliberate fallback to avoid fabricating misleading metadata during navigation.
            None
        };

        if let Some(cluster_type) = cluster_type {
            table_meta.cluster_key_v2 = snapshot_cluster_key_meta;
            table_meta.options.insert(
                OPT_KEY_CLUSTER_TYPE.to_owned(),
                cluster_type.to_string().to_lowercase(),
            );
        } else {
            table_meta.options.remove(OPT_KEY_CLUSTER_TYPE);
            table_meta.cluster_key_v2 = None;
        }

        let mut historical_schema = snapshot.schema.clone();
        historical_schema.next_column_id = self
            .table_info
            .meta
            .schema
            .next_column_id()
            .max(historical_schema.next_column_id());

        // Preserve current table-level governance metadata when the target snapshot keeps the
        // same schema after normalizing next_column_id. Snapshot navigation restores
        // snapshot-carried metadata such as schema, clustering and statistics, but does not
        // rewind later governance-only table_meta changes when the visible column layout is
        // unchanged.
        if table_meta.schema.as_ref() == &historical_schema {
            return Ok(false);
        }

        table_meta.fill_field_comments();
        let comment_by_column_id = table_meta
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                (
                    field.column_id(),
                    table_meta
                        .field_comments
                        .get(index)
                        .cloned()
                        .unwrap_or_default(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();

        table_meta.schema = Arc::new(historical_schema);
        table_meta.field_comments = snapshot
            .schema
            .fields()
            .iter()
            .map(|field| {
                comment_by_column_id
                    .get(&field.column_id())
                    .cloned()
                    .unwrap_or_default()
            })
            .collect();
        // Virtual columns are table-level derived metadata and are not versioned in snapshots.
        // Snapshot navigation cannot prove that the current virtual schema is valid for the
        // historical point, so clear it conservatively.
        table_meta.virtual_schema = None;
        table_meta.column_mask_policy = None;
        table_meta.row_access_policy = None;

        if let Some(value) = table_meta.options.get(OPT_KEY_APPROX_DISTINCT_COLUMNS) {
            if let ApproxDistinctColumns::Specify(cols) = value.parse::<ApproxDistinctColumns>()? {
                let compatible = cols.iter().all(|col| {
                    table_meta
                        .schema
                        .field_with_name(col)
                        .map(|field| RangeIndex::supported_table_type(field.data_type()))
                        .unwrap_or(false)
                });

                if !compatible {
                    table_meta.options.remove(OPT_KEY_APPROX_DISTINCT_COLUMNS);
                }
            }
        }

        if let Some(value) = table_meta.options.get(OPT_KEY_BLOOM_INDEX_COLUMNS) {
            if let BloomIndexColumns::Specify(cols) = value.parse::<BloomIndexColumns>()? {
                let compatible = cols.iter().all(|col| {
                    table_meta
                        .schema
                        .field_with_name(col)
                        .map(|field| BloomIndex::supported_type(field.data_type()))
                        .unwrap_or(false)
                });

                if !compatible {
                    table_meta.options.remove(OPT_KEY_BLOOM_INDEX_COLUMNS);
                }
            }
        }

        Ok(true)
    }

    pub fn apply_navigation_metadata(
        &self,
        table_meta: &mut TableMeta,
        snapshot: &TableSnapshot,
    ) -> Result<()> {
        if !self.apply_snapshot_metadata_to_meta(table_meta, snapshot)? {
            return Ok(());
        }

        let column_ids = table_meta.schema.to_column_ids();
        table_meta.indexes.retain(|_, index| {
            index
                .column_ids
                .iter()
                .all(|column_id| column_ids.contains(column_id))
        });

        let mut broken_mask_column_ids = Vec::new();
        // Time travel may drop policy metadata from the derived table when the target column
        // disappeared, but still rejects partially broken references.
        table_meta
            .column_mask_policy_columns_ids
            .retain(|column_id, policy_map| {
                if !column_ids.contains(column_id) {
                    return false;
                }
                if !policy_map
                    .columns_ids
                    .iter()
                    .all(|id| column_ids.contains(id))
                {
                    broken_mask_column_ids.push(*column_id);
                }
                true
            });
        if !broken_mask_column_ids.is_empty() {
            return Err(ErrorCode::IllegalReference(format!(
                "Cannot navigate to target snapshot: masking policy on column ID(s) {:?} \
                 references columns that do not exist in the target schema. \
                 Please unset the masking policy before proceeding.",
                broken_mask_column_ids
            )));
        }

        // Time travel navigation can clear a policy if all its referenced columns disappeared,
        // but rejects partially missing references.
        if let Some(policy_map) = &table_meta.row_access_policy_columns_ids {
            let present_count = policy_map
                .columns_ids
                .iter()
                .filter(|id| column_ids.contains(id))
                .count();
            if present_count == 0 {
                table_meta.row_access_policy_columns_ids = None;
            } else if present_count < policy_map.columns_ids.len() {
                return Err(ErrorCode::IllegalReference(
                    "Cannot navigate to target snapshot: row access policy references \
                     columns that partially do not exist in the target schema. \
                     Please drop the row access policy before proceeding."
                        .to_string(),
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn apply_snapshot_statistics(
        &self,
        table_meta: &mut TableMeta,
        snapshot: &TableSnapshot,
    ) {
        let summary = &snapshot.summary;
        table_meta.statistics = TableStatistics {
            number_of_rows: summary.row_count,
            data_bytes: summary.uncompressed_byte_size,
            compressed_data_bytes: summary.compressed_byte_size,
            index_data_bytes: summary.index_size,
            bloom_index_size: summary.bloom_index_size,
            ngram_index_size: summary.ngram_index_size,
            inverted_index_size: summary.inverted_index_size,
            vector_index_size: summary.vector_index_size,
            virtual_column_size: summary.virtual_column_size,
            number_of_segments: Some(snapshot.segments.len() as u64),
            number_of_blocks: Some(summary.block_count),
        };
    }
}
