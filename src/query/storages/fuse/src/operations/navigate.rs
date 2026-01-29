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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_meta_app::storage::S3StorageClass;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::VACUUM2_OBJECT_KEY_PREFIX;
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
use crate::statistics::gen_table_statistics;

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
            NavigationPoint::StreamInfo(info) => self.navigate_to_stream(ctx, info).await,
            NavigationPoint::TableRef { .. } => unreachable!(),
        }
    }

    #[async_backtrace::framed]
    pub async fn navigate_to_stream(
        &self,
        ctx: &Arc<dyn TableContext>,
        stream_info: &TableInfo,
    ) -> Result<Arc<FuseTable>> {
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
        let location = options.get(OPT_KEY_SNAPSHOT_LOCATION).cloned();
        self.load_table_by_location(ctx, location).await
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
    pub async fn navigate_to_snapshot(
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
        mut pred: P,
    ) -> Result<Arc<FuseTable>>
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
            self.get_branch_id(),
        );

        // Find the instant which matches the given `time_point`.
        let mut instant = None;
        while let Some(snapshot_with_version) = snapshot_stream.try_next().await? {
            abort_checker
                .try_check_aborting()
                .with_context(|| "failed to find snapshot")?;
            if pred(snapshot_with_version.0.as_ref()) {
                instant = Some(snapshot_with_version);
                break;
            }
        }

        if let Some((snapshot, format_version)) = instant {
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
        let loc = self.meta_location_generator.gen_snapshot_location(
            self.get_branch_id(),
            &snapshot.snapshot_id,
            format_version,
        )?;

        // Cluster key will be restored from the snapshot.
        // If the snapshot has no cluster key, means the table is currently unclustered.
        // We do NOT fall back to table-level cluster key metadata here, even if
        // the base table defines one. This is expected and by design.
        let new_branch = match self.branch_info.as_ref() {
            Some(branch) => {
                let mut new_branch = branch.clone();
                new_branch.info.loc = loc;
                new_branch.schema = Arc::new(snapshot.schema.clone());
                new_branch.cluster_key_meta = snapshot.cluster_key_meta.clone();
                Some(new_branch)
            }
            None => {
                table_info.meta.schema = Arc::new(snapshot.schema.clone());
                table_info.meta.cluster_key_v2 = snapshot.cluster_key_meta.clone();
                table_info
                    .meta
                    .options
                    .insert(OPT_KEY_SNAPSHOT_LOCATION.to_owned(), loc);
                None
            }
        };

        // 3. The statistics
        table_info.meta.statistics = gen_table_statistics(snapshot);

        // let's instantiate it
        let mut table = FuseTable::create_without_refresh_table_info(table_info, s3_storage_class)?;
        table.branch_info = new_branch;
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
                    Some(NavigationPoint::TableRef { .. }) => unreachable!(),
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

        let prefix = format!(
            "{}/{}/",
            self.meta_location_generator().prefix(),
            FUSE_TBL_SNAPSHOT_PREFIX,
        );

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
        let prefix = format!(
            "{}/{}/",
            self.meta_location_generator().prefix(),
            FUSE_TBL_SNAPSHOT_PREFIX,
        );
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
    pub async fn list_by_stream(
        &self,
        stream_info: TableInfo,
        retention_point: DateTime<Utc>,
    ) -> Result<(String, Vec<String>)> {
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

        let snapshot_loc = options
            .get(OPT_KEY_SNAPSHOT_LOCATION)
            .ok_or_else(|| {
                ErrorCode::TableHistoricalDataNotFound("No historical data found at given point")
            })?
            .parse::<String>()?;

        let mut found = false;
        let prefix = format!(
            "{}/{}/",
            self.meta_location_generator().prefix(),
            FUSE_TBL_SNAPSHOT_PREFIX,
        );

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
            NavigationPoint::TableRef { typ, name } => {
                let table_ref = self.table_info.get_table_ref(name)?;
                let ref_type = &table_ref.typ;
                if ref_type != typ {
                    return Err(ErrorCode::MismatchedReferenceType(format!(
                        "'{}' is a {}, please use 'AT({} => {})' instead.",
                        name, ref_type, ref_type, name,
                    )));
                }
                Ok(Some(table_ref.loc.clone()))
            }
        }
    }

    // Only used when the table branch is none.
    #[async_backtrace::framed]
    pub async fn find_location<P>(
        &self,
        ctx: &Arc<dyn TableContext>,
        location: String,
        mut pred: P,
    ) -> Result<String>
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
            self.get_branch_id(),
        );

        // Find the snapshot which matches the given `time_point`.
        while let Some((snapshot, format_version)) = snapshot_stream.try_next().await? {
            abort_checker
                .try_check_aborting()
                .with_context(|| "failed to find snapshot")?;
            if pred(snapshot.as_ref()) {
                let snapshot_location = self.meta_location_generator.gen_snapshot_location(
                    None,
                    &snapshot.snapshot_id,
                    format_version,
                )?;
                return Ok(snapshot_location);
            }
        }

        Err(ErrorCode::TableHistoricalDataNotFound(
            "No historical data found at given point",
        ))
    }
}
