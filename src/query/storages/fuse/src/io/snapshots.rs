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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use chrono::DateTime;
use chrono::Utc;
use common_base::runtime::execute_futures_in_parallel;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableSchemaRef;
use futures::stream::StreamExt;
use futures_util::TryStreamExt;
use opendal::EntryMode;
use opendal::Metakey;
use opendal::Operator;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::FormatVersion;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SnapshotId;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotLite;
use tracing::info;
use tracing::warn;
use tracing::Instrument;

use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::io::TableMetaLocationGenerator;

#[derive(Debug, PartialEq, Eq)]
pub struct SnapshotReferencedFiles {
    pub segments: BTreeSet<String>,
    pub blocks: BTreeSet<String>,
    pub blocks_index: BTreeSet<String>,
}

impl SnapshotReferencedFiles {
    pub fn all_files(&self) -> Vec<String> {
        let mut files = vec![];
        for file in &self.segments {
            files.push(file.clone());
        }
        for file in &self.blocks {
            files.push(file.clone());
        }
        for file in &self.blocks_index {
            files.push(file.clone());
        }
        files
    }
}

type BlockLocationTuple = (BTreeSet<String>, BTreeSet<String>);

#[derive(Clone, Debug)]
pub struct SnapshotLiteExtended {
    pub format_version: u64,
    pub snapshot_id: SnapshotId,
    pub prev_snapshot_id: Option<SnapshotId>,
    pub timestamp: Option<DateTime<Utc>>,
    pub segments: HashSet<Location>,
    pub table_statistics_location: Option<String>,
}

// Read snapshot related operations.
pub struct SnapshotsIO {
    ctx: Arc<dyn TableContext>,
    operator: Operator,
}

impl SnapshotsIO {
    pub fn create(ctx: Arc<dyn TableContext>, operator: Operator) -> Self {
        Self { ctx, operator }
    }

    #[async_backtrace::framed]
    async fn read_snapshot(
        snapshot_location: String,
        data_accessor: Operator,
    ) -> Result<(Arc<TableSnapshot>, FormatVersion)> {
        let reader = MetaReaders::table_snapshot_reader(data_accessor);
        let ver = TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());
        let load_params = LoadParams {
            location: snapshot_location,
            len_hint: None,
            ver,
            put_cache: true,
        };
        let snapshot = reader.read(&load_params).await?;
        Ok((snapshot, ver))
    }

    #[async_backtrace::framed]
    async fn read_snapshot_lite(
        snapshot_location: String,
        data_accessor: Operator,
        min_snapshot_timestamp: Option<DateTime<Utc>>,
    ) -> Result<TableSnapshotLite> {
        let reader = MetaReaders::table_snapshot_reader(data_accessor);
        let ver = TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());
        let load_params = LoadParams {
            location: snapshot_location,
            len_hint: None,
            ver,
            put_cache: false,
        };
        let snapshot = reader.read(&load_params).await?;

        if snapshot.timestamp > min_snapshot_timestamp {
            // filter out snapshots which have larger (artificial)timestamp , they are
            // not members of precedents of the current snapshot, whose timestamp is
            // min_snapshot_timestamp.
            //
            // NOTE: it is NOT the case that all those have lesser timestamp, are
            // members of precedents of the current snapshot, though.
            // Error is directly returned, since it can be ignored through flatten
            // in read_snapshot_lites_ext.
            return Err(ErrorCode::StorageOther(
                "The timestamp of snapshot need less than the min_snapshot_timestamp",
            ));
        }

        Ok(TableSnapshotLite::from((snapshot.as_ref(), ver)))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    #[async_backtrace::framed]
    async fn read_snapshot_lites(
        &self,
        snapshot_files: &[String],
        min_snapshot_timestamp: Option<DateTime<Utc>>,
    ) -> Result<Vec<Result<TableSnapshotLite>>> {
        // combine all the tasks.
        let mut iter = snapshot_files.iter();
        let tasks = std::iter::from_fn(move || {
            iter.next().map(|location| {
                Self::read_snapshot_lite(
                    location.clone(),
                    self.operator.clone(),
                    min_snapshot_timestamp,
                )
                .instrument(tracing::debug_span!("read_snapshot"))
            })
        });

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let permit_nums = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        execute_futures_in_parallel(
            tasks,
            threads_nums,
            permit_nums,
            "fuse-req-snapshots-worker".to_owned(),
        )
        .await
    }

    // Read all the snapshots by the root file.
    #[async_backtrace::framed]
    pub async fn read_snapshot_lites_ext<T>(
        &self,
        root_snapshot_file: String,
        min_snapshot_timestamp: Option<DateTime<Utc>>,
        status_callback: T,
    ) -> Result<(Vec<TableSnapshotLite>, Vec<TableSnapshotLite>)>
    where
        T: Fn(String),
    {
        let ctx = self.ctx.clone();
        let data_accessor = self.operator.clone();

        // List all the snapshot file paths
        // note that snapshot file paths of ongoing txs might be included
        let mut snapshot_files = vec![];
        if let Some(prefix) = Self::get_s3_prefix_from_file(&root_snapshot_file) {
            snapshot_files = Self::list_files(self.operator.clone(), &prefix, None).await?;
        }

        // 1. Get all the snapshot by chunks.
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;
        let mut snapshot_lites = Vec::with_capacity(snapshot_files.len());

        let start = Instant::now();
        let mut count = 0;
        for chunk in snapshot_files.chunks(max_io_requests) {
            let results = self
                .read_snapshot_lites(chunk, min_snapshot_timestamp)
                .await?;

            for snapshot_lite in results.into_iter().flatten() {
                snapshot_lites.push(snapshot_lite);
            }

            // Refresh status.
            {
                count += chunk.len();
                let status = format!(
                    "read snapshot files:{}/{}, cost:{} sec",
                    count,
                    snapshot_files.len(),
                    start.elapsed().as_secs()
                );
                info!(status);
                (status_callback)(status);
            }
        }

        let (root_snapshot, format_version) =
            Self::read_snapshot(root_snapshot_file.clone(), data_accessor.clone()).await?;

        Ok(Self::chain_snapshots(
            snapshot_lites,
            &root_snapshot,
            format_version,
        ))
    }

    // read all the precedent snapshots of given `root_snapshot`
    #[async_backtrace::framed]
    pub async fn read_chained_snapshot_lites(
        &self,
        location_generator: TableMetaLocationGenerator,
        root_snapshot: String,
        limit: Option<usize>,
    ) -> Result<Vec<TableSnapshotLite>> {
        let table_snapshot_reader =
            MetaReaders::table_snapshot_reader(self.ctx.get_data_operator()?.operator());
        let format_version = TableMetaLocationGenerator::snapshot_version(root_snapshot.as_str());
        let lite_snapshot_stream = table_snapshot_reader
            .snapshot_history(root_snapshot, format_version, location_generator)
            .map_ok(|(snapshot, format_version)| {
                TableSnapshotLite::from((snapshot.as_ref(), format_version))
            });
        if let Some(l) = limit {
            lite_snapshot_stream.take(l).try_collect::<Vec<_>>().await
        } else {
            lite_snapshot_stream.try_collect::<Vec<_>>().await
        }
    }

    // return from files that last modified time within timestamp
    #[tracing::instrument(level = "debug", skip_all)]
    #[async_backtrace::framed]
    pub async fn get_within_time_files(
        &self,
        files: &[String],
        timestamp: i64,
    ) -> Result<HashSet<String>> {
        async fn get_within_time_file(
            file: String,
            operator: Operator,
            timestamp: i64,
        ) -> Result<String> {
            let metadata = operator.stat(&file).await?;
            if let Some(last_modified) = metadata.last_modified() {
                if last_modified.timestamp() >= timestamp {
                    return Ok(file);
                }
            }

            Ok("".to_string())
        }

        async fn get_within_time_files(
            files: &[String],
            operator: Operator,
            timestamp: i64,
            threads_nums: usize,
            permit_nums: usize,
        ) -> Result<Vec<Result<String>>> {
            // combine all the tasks.
            let mut iter = files.iter();
            let tasks = std::iter::from_fn(move || {
                iter.next().map(|location| {
                    get_within_time_file(location.to_string(), operator.clone(), timestamp)
                        .instrument(tracing::debug_span!("read_segment_blocks"))
                })
            });

            execute_futures_in_parallel(
                tasks,
                threads_nums,
                permit_nums,
                "fuse-req-snapshots-worker".to_owned(),
            )
            .await
        }

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let permit_nums = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        let max_io_requests = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        let mut file_set = HashSet::new();
        for chunk in files.chunks(max_io_requests) {
            let results = get_within_time_files(
                chunk,
                self.operator.clone(),
                timestamp,
                threads_nums,
                permit_nums,
            )
            .await?;
            for file in results.into_iter().flatten() {
                file_set.insert(file);
            }
        }

        Ok(file_set)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    #[async_backtrace::framed]
    async fn read_segment_blocks(
        &self,
        segment_files: &[&String],
        schema: TableSchemaRef,
        version: u64,
    ) -> Result<Vec<Result<BlockLocationTuple>>> {
        async fn read_segment_block(
            location: String,
            ver: u64,
            operator: Operator,
            schema: TableSchemaRef,
        ) -> Result<BlockLocationTuple> {
            let reader = MetaReaders::segment_info_reader(operator, schema);
            // Keep in mind that segment_info_read must need a schema
            let load_params = LoadParams {
                location,
                len_hint: None,
                ver,
                put_cache: false,
            };

            let segment = reader.read(&load_params).await?;
            let mut blocks = BTreeSet::new();
            let mut blocks_index = BTreeSet::new();
            segment.blocks.iter().for_each(|block_meta| {
                blocks.insert(block_meta.location.0.clone());
                if let Some(bloom_loc) = &block_meta.bloom_filter_index_location {
                    blocks_index.insert(bloom_loc.0.clone());
                }
            });
            Ok((blocks, blocks_index))
        }

        // combine all the tasks.
        let mut iter = segment_files.iter();
        let tasks = std::iter::from_fn(move || {
            iter.next().map(|location| {
                read_segment_block(
                    location.to_string(),
                    version,
                    self.operator.clone(),
                    schema.clone(),
                )
                .instrument(tracing::debug_span!("read_segment_blocks"))
            })
        });

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let permit_nums = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        execute_futures_in_parallel(
            tasks,
            threads_nums,
            permit_nums,
            "fuse-req-snapshots-worker".to_owned(),
        )
        .await
    }

    // Read all the referenced {segments|blocks|blocks_index} by the snapshot file.
    // limit: limits the number of snapshot files listed
    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    pub async fn get_snapshot_referenced_files<T>(
        &self,
        root_snapshot_location: String,
        root_snapshot_lite: Arc<SnapshotLiteExtended>,
        schema: TableSchemaRef,
        status_callback: T,
    ) -> Result<Option<SnapshotReferencedFiles>>
    where
        T: Fn(String),
    {
        let ctx = self.ctx.clone();

        // List all the snapshot file paths
        // note that snapshot file paths of ongoing txs might be included
        let mut snapshot_files = vec![];
        if let Some(prefix) = Self::get_s3_prefix_from_file(&root_snapshot_location) {
            snapshot_files = Self::list_files(self.operator.clone(), &prefix, None).await?;
        }

        if snapshot_files.is_empty() {
            return Ok(None);
        }

        // 1. Get all the snapshot by chunks, save all the segments location.
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

        let start = Instant::now();
        let mut count = 0;

        let mut snapshot_lites = BTreeMap::new();
        for chunk in snapshot_files.chunks(max_io_requests) {
            let results = self
                .read_snapshot_lite_extends(chunk, root_snapshot_lite.clone())
                .await?;

            for snapshot_lite_extend in results.into_iter().flatten() {
                snapshot_lites.insert(snapshot_lite_extend.snapshot_id, snapshot_lite_extend);
            }

            // Refresh status.
            {
                count += chunk.len();
                let status = format!(
                    "gc orphan: read snapshot files:{}/{}, cost:{} sec",
                    count,
                    snapshot_files.len(),
                    start.elapsed().as_secs()
                );
                info!(status);
                (status_callback)(status);
            }
        }

        // 2. Get all the referenced segments
        let mut segments = BTreeSet::new();
        let mut current_snapshot_lite_opt = Some(root_snapshot_lite.as_ref());
        while let Some(current_snapshot_lite) = current_snapshot_lite_opt {
            current_snapshot_lite
                .segments
                .iter()
                .for_each(|(location, _)| {
                    segments.insert(location.to_owned());
                });

            current_snapshot_lite_opt = match current_snapshot_lite.prev_snapshot_id {
                Some(prev_snapshot_id) => snapshot_lites.get(&prev_snapshot_id),
                None => None,
            };
        }
        drop(snapshot_lites);
        // Refresh status.
        {
            let status = format!(
                "gc orphan: read segments files:{}, cost:{} sec",
                segments.len(),
                start.elapsed().as_secs()
            );
            info!(status);
            (status_callback)(status);
        }

        // 3. Get all the referenced blocks
        let mut count = 0;
        let mut blocks = BTreeSet::new();
        let mut blocks_index = BTreeSet::new();
        let segment_locations = Vec::from_iter(segments.iter());
        for segment_chunk in segment_locations.chunks(max_io_requests) {
            let results = self
                .read_segment_blocks(
                    segment_chunk,
                    schema.clone(),
                    root_snapshot_lite.format_version,
                )
                .await?;
            for (ret_blocks, ret_index) in results.into_iter().flatten() {
                blocks.extend(ret_blocks);
                blocks_index.extend(ret_index);
            }

            // Refresh status.
            {
                count += 1;
                let status = format!(
                    "gc orphan: read segments block files:{}/{}, cost:{} sec",
                    count,
                    segment_locations.len(),
                    start.elapsed().as_secs()
                );
                info!(status);
                (status_callback)(status);
            }
        }

        Ok(Some(SnapshotReferencedFiles {
            segments,
            blocks,
            blocks_index,
        }))
    }

    #[async_backtrace::framed]
    pub async fn get_files_by_prefix(&self, input_file: &str) -> Result<Vec<String>> {
        if let Some(prefix) = SnapshotsIO::get_s3_prefix_from_file(input_file) {
            Self::list_files(self.operator.clone(), &prefix, None).await
        } else {
            Ok(vec![])
        }
    }

    // Read all the snapshots by the root file.
    // limit: limits the number of snapshot files listed
    // with_segment_locations: if true will get the segments of the snapshot
    #[async_backtrace::framed]
    async fn read_snapshot_lite_extend(
        snapshot_location: String,
        data_accessor: Operator,
        root_snapshot: Arc<SnapshotLiteExtended>,
    ) -> Result<SnapshotLiteExtended> {
        let reader = MetaReaders::table_snapshot_reader(data_accessor);
        let ver = TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());
        let load_params = LoadParams {
            location: snapshot_location,
            len_hint: None,
            ver,
            put_cache: false,
        };
        let snapshot = reader.read(&load_params).await?;

        if snapshot.timestamp >= root_snapshot.timestamp {
            // filter out snapshots which have larger (artificial)timestamp , they are
            // not members of precedents of the current snapshot, whose timestamp is
            // min_snapshot_timestamp.
            //
            // NOTE: it is NOT the case that all those have lesser timestamp, are
            // members of precedents of the current snapshot, though.
            // Error is directly returned, since we can be ignored through flatten.
            return Err(ErrorCode::StorageOther(
                "The timestamp of snapshot need less than the min_snapshot_timestamp",
            ));
        }
        let mut segments = HashSet::new();
        // collects extended segments.
        for segment_location in &snapshot.segments {
            if root_snapshot.segments.contains(segment_location) {
                continue;
            }
            segments.insert(segment_location.clone());
        }
        let table_statistics_location =
            if snapshot.table_statistics_location != root_snapshot.table_statistics_location {
                snapshot.table_statistics_location.clone()
            } else {
                None
            };

        Ok(SnapshotLiteExtended {
            format_version: ver,
            snapshot_id: snapshot.snapshot_id,
            prev_snapshot_id: snapshot
                .prev_snapshot_id
                .map(|(prev_snapshot_id, _)| prev_snapshot_id),
            timestamp: snapshot.timestamp,
            segments,
            table_statistics_location,
        })
    }

    #[tracing::instrument(level = "debug", skip_all)]
    #[async_backtrace::framed]
    pub async fn read_snapshot_lite_extends(
        &self,
        snapshot_files: &[String],
        root_snapshot: Arc<SnapshotLiteExtended>,
    ) -> Result<Vec<Result<SnapshotLiteExtended>>> {
        // combine all the tasks.
        let mut iter = snapshot_files.iter();
        let tasks = std::iter::from_fn(move || {
            iter.next().map(|location| {
                Self::read_snapshot_lite_extend(
                    location.clone(),
                    self.operator.clone(),
                    root_snapshot.clone(),
                )
                .instrument(tracing::debug_span!("read_snapshot"))
            })
        });

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let permit_nums = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        execute_futures_in_parallel(
            tasks,
            threads_nums,
            permit_nums,
            "fuse-req-snapshots-worker".to_owned(),
        )
        .await
    }

    fn chain_snapshots(
        snapshot_lites: Vec<TableSnapshotLite>,
        root_snapshot: &TableSnapshot,
        format_version: FormatVersion,
    ) -> (Vec<TableSnapshotLite>, Vec<TableSnapshotLite>) {
        let mut snapshot_map = HashMap::new();
        let mut chained_snapshot_lites = vec![];
        for snapshot_lite in snapshot_lites.into_iter() {
            snapshot_map.insert(snapshot_lite.snapshot_id, snapshot_lite);
        }
        let root_snapshot_lite = TableSnapshotLite::from((root_snapshot, format_version));
        let mut prev_snapshot_id_tuple = root_snapshot_lite.prev_snapshot_id;
        chained_snapshot_lites.push(root_snapshot_lite);
        while let Some((prev_snapshot_id, _)) = prev_snapshot_id_tuple {
            let prev_snapshot_lite = snapshot_map.remove(&prev_snapshot_id);
            match prev_snapshot_lite {
                None => {
                    break;
                }
                Some(prev_snapshot) => {
                    prev_snapshot_id_tuple = prev_snapshot.prev_snapshot_id;
                    chained_snapshot_lites.push(prev_snapshot);
                }
            }
        }
        // remove root from orphan list
        snapshot_map.remove(&root_snapshot.snapshot_id);
        (chained_snapshot_lites, snapshot_map.into_values().collect())
    }

    #[async_backtrace::framed]
    pub async fn list_files(
        op: Operator,
        prefix: &str,
        exclude_file: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut file_list = vec![];
        let mut ds = op.list(prefix).await?;
        while let Some(de) = ds.try_next().await? {
            let meta = op
                .metadata(&de, Metakey::Mode | Metakey::LastModified)
                .await?;
            match meta.mode() {
                EntryMode::FILE => match exclude_file {
                    Some(path) if de.path() == path => continue,
                    _ => {
                        let location = de.path().to_string();
                        let modified = meta.last_modified();
                        file_list.push((location, modified));
                    }
                },
                _ => {
                    warn!("found not snapshot file in {:}, found: {:?}", prefix, de);
                    continue;
                }
            }
        }

        // let mut vector: Vec<(i32, Option<i32>)> = vec![(1, Some(1)), (2, None), (3, Some(3)), (4, None)];
        // vector.sort_by(|(_, k1), (_, k2)| k2.cmp(k1));
        // Result:
        // [(3, Some(3)), (1, Some(1)), (2, None),(4, None)]
        file_list.sort_by(|(_, m1), (_, m2)| m2.cmp(m1));
        Ok(file_list.into_iter().map(|v| v.0).collect())
    }

    // _ss/xx/yy.json -> _ss/xx/
    pub fn get_s3_prefix_from_file(file_path: &str) -> Option<String> {
        if let Some(path) = Path::new(&file_path).parent() {
            let mut prefix = path.to_str().unwrap_or("").to_string();

            if !prefix.contains('/') {
                return None;
            }

            // Append '/' to the end if need.
            if !prefix.ends_with('/') {
                prefix += "/";
            }
            return Some(prefix);
        }
        None
    }
}
