// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_storages_fuse::io::SnapshotsIO;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::FuseTable;
use futures::TryStreamExt;
use log::info;
use log::warn;
use opendal::EntryMode;
use opendal::Metakey;
use storages_common_table_meta::meta::time_from_snapshot_id;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableVersion;

const DRY_RUN_LIMIT: usize = 1000;
const BATCH_PURGE_FILE_NUM: usize = 100;

struct VacuumOperator {
    pub fuse_table: FuseTable,
    pub ctx: Arc<dyn TableContext>,
    pub retention_time: DateTime<Utc>,
    pub start: Instant,
    pub root_snapshot_location: String,
}

#[derive(Debug)]
struct SnapshotReferencedFileSet {
    pub segments: HashSet<String>,
    pub blocks: HashSet<String>,
    pub blocks_index: HashSet<String>,
}

struct VacuumContext {
    pub snapshot_files_with_time: Vec<SnapshotLocationWithTime>,
    pub root_gc_snapshot_index: usize,
    pub root_gc_snapshot_reference_files: SnapshotReferencedFileSet,
    pub keep_snapshot_table_versions: TableVersionSet,
}

type SnapshotLocationWithTime = (Option<DateTime<Utc>>, String);
type TableVersionSet = HashSet<TableVersion>;

impl VacuumOperator {
    // list all snapshot files and sort by time
    #[async_backtrace::framed]
    async fn list_snapshot_files_with_time(&self) -> Result<Vec<SnapshotLocationWithTime>> {
        // List all the snapshot file paths
        // note that snapshot file paths of ongoing txs might be included
        let root_snapshot_location = &self.root_snapshot_location;
        let operator = self.fuse_table.get_operator();
        let mut snapshot_files = vec![];
        if let Some(prefix) = SnapshotsIO::get_s3_prefix_from_file(root_snapshot_location) {
            snapshot_files = SnapshotsIO::list_files(operator, &prefix, None).await?;
        }

        let mut snapshot_files_with_time = Vec::with_capacity(snapshot_files.len());
        if snapshot_files.is_empty() {
            return Ok(snapshot_files_with_time);
        }

        for snapshot_file in snapshot_files {
            let snapshot_id =
                TableMetaLocationGenerator::snapshot_id_from_location(&snapshot_file)?;
            let timestamp = time_from_snapshot_id(&snapshot_id);
            snapshot_files_with_time.push((timestamp, snapshot_file));
        }

        // sort snapshot files with time in descending order
        snapshot_files_with_time.sort_by(|(t1, _), (t2, _)| t2.cmp(t1));

        Ok(snapshot_files_with_time)
    }

    // get root gc snapshot file and index in snapshot_files_with_time vector
    #[async_backtrace::framed]
    async fn get_root_gc_snapshot_file(
        &self,
        snapshot_files_with_time: &[SnapshotLocationWithTime],
    ) -> Result<Option<(usize, String)>> {
        let retention_time = &self.retention_time;

        let mut last_snapshot_location_opt = None;
        let mut status = format!(
            "do_vacuum with table {}: cannot find any snapshot that timestamp < retention_time",
            self.fuse_table.get_table_info().name,
        );
        // iterator snapshot files with time in descending order
        for (i, (timestamp, snapshot_file)) in snapshot_files_with_time.iter().enumerate() {
            let last_snapshot_location = if let Some(timestamp) = timestamp {
                if timestamp >= retention_time {
                    // save the last snapshot location that timestamp >= retention_time and continue the next loop
                    last_snapshot_location_opt = Some(snapshot_file);
                    continue;
                } else if let Some(last_snapshot_location) = last_snapshot_location_opt {
                    // now we get the first snapshot that timestamp < retention_time, return last_snapshot_location
                    last_snapshot_location
                } else {
                    status = format!(
                        "do_vacuum with table {}: current snapshot timestamp < retention_time",
                        self.fuse_table.get_table_info().name,
                    );
                    // if it is the current snapshot(last_snapshot_location_opt == None), return None
                    break;
                }
            } else {
                // if cannot find a snapshot with timestamp, return None
                status = format!(
                    "do_vacuum with table {}: cannot find a snapshot filename with timestamp",
                    self.fuse_table.get_table_info().name,
                );
                break;
            };

            // here we get the first snapshot that:
            //  1. timestamp < retention_time
            //  2. it is not the current snapshot

            // check if last_snapshot_location'prev match snapshot_file
            // if not it means that last snapshot or snapshot_files_with_time[i]
            // has not been committed success
            // in both cases, return None
            let last_snapshot = match self
                .fuse_table
                .read_table_snapshot_by_location(last_snapshot_location.to_string())
                .await?
            {
                Some(next_snapshot) => next_snapshot,
                None => {
                    status = format!(
                        "do_vacuum with table {}: read last snapshot {:?} return None",
                        self.fuse_table.get_table_info().name,
                        last_snapshot_location
                    );
                    break;
                }
            };

            let prev_snapshot_location = if let Some((prev_snapshot_id, version, table_version)) =
                last_snapshot.prev_snapshot_id
            {
                let generator = self.fuse_table.meta_location_generator();
                generator.gen_snapshot_location(&prev_snapshot_id, version, table_version)?
            } else {
                status = format!(
                    "do_vacuum with table {}: last snapshot {:?} has no prev snapshot id",
                    self.fuse_table.get_table_info().name,
                    last_snapshot_location
                );
                break;
            };

            let snapshot_file = snapshot_file.to_string();
            if prev_snapshot_location != snapshot_file {
                status = format!(
                    "do_vacuum with table {}: last snapshot {:?} 's prev snapshot location is not {:?}",
                    self.fuse_table.get_table_info().name,
                    last_snapshot_location,
                    snapshot_file
                );
                break;
            }

            // retturn root gc snapshot that:
            //  1. with timestamp in file name
            //  2. first snapshot that timestamp < retention(it'next snapshot timestamp >= retention.)
            //  3. it has been commit success
            return Ok(Some((i, snapshot_file)));
        }

        self.log_status(status);
        Ok(None)
    }

    #[async_backtrace::framed]
    fn get_keep_snapshot_table_versions(
        &self,
        snapshot_files_with_time: &[SnapshotLocationWithTime],
        root_gc_snapshot_index: usize,
    ) -> TableVersionSet {
        let mut table_version_set = HashSet::new();
        for snapshot_file_with_time in snapshot_files_with_time[0..root_gc_snapshot_index].iter() {
            let snapshot_file = &snapshot_file_with_time.1;
            // all snapshot file commit after root_gc_snapshot will with table version, so safe to unwrap()
            let table_version =
                TableMetaLocationGenerator::location_table_version(snapshot_file).unwrap();
            table_version_set.insert(table_version);
        }
        table_version_set
    }

    // get all root gc snapshot file referenced files: segments\block\block index
    #[async_backtrace::framed]
    async fn get_root_gc_snapshot_reference_files(
        &self,
        root_gc_snapshot: &TableSnapshot,
    ) -> Result<SnapshotReferencedFileSet> {
        let segments: HashSet<String> = root_gc_snapshot
            .segments
            .iter()
            .map(|(location, _)| location.clone())
            .collect();

        let locations_referenced = self
            .fuse_table
            .get_block_locations(self.ctx.clone(), &root_gc_snapshot.segments, false, false)
            .await?;
        let (blocks, blocks_index) = (
            locations_referenced.block_location,
            locations_referenced.bloom_location,
        );

        Ok(SnapshotReferencedFileSet {
            segments,
            blocks,
            blocks_index,
        })
    }

    // return true if purge_files.len() >= DRY_RUN_LIMIT
    #[async_backtrace::framed]
    async fn batch_gc_files(
        &self,
        batch_purge_files: &mut Vec<String>,
        purge_files: &mut Option<&mut Vec<String>>,
    ) -> Result<bool> {
        if let Some(purge_files) = purge_files {
            purge_files.extend(batch_purge_files.to_vec());
            batch_purge_files.clear();
            Ok(purge_files.len() >= DRY_RUN_LIMIT)
        } else {
            let files: HashSet<String> = batch_purge_files.iter().cloned().collect();
            self.fuse_table
                .try_purge_location_files(self.ctx.clone(), files)
                .await?;
            batch_purge_files.clear();
            Ok(false)
        }
    }

    // return true if purge_files.len() >= DRY_RUN_LIMIT
    #[async_backtrace::framed]
    async fn gc_files_in_dir(
        &self,
        context: &VacuumContext,
        referenced_files: &HashSet<String>,
        purge_files: &mut Option<&mut Vec<String>>,
    ) -> Result<bool> {
        if let Some(referenced_file) = referenced_files.iter().next().cloned() {
            if let Some(prefix) = SnapshotsIO::get_s3_prefix_from_file(&referenced_file) {
                let operator = self.fuse_table.get_operator();
                let mut ds = operator.lister_with(&prefix).metakey(Metakey::Mode).await?;
                let keep_snapshot_table_versions = &context.keep_snapshot_table_versions;
                let mut batch_purge_files = Vec::with_capacity(BATCH_PURGE_FILE_NUM);
                let mut count = 0;
                while let Some(de) = ds.try_next().await? {
                    let meta = de.metadata();
                    match meta.mode() {
                        EntryMode::FILE => {
                            let location = de.path().to_string();

                            let is_table_version_in_reference =
                                match TableMetaLocationGenerator::location_table_version(&location)
                                {
                                    Some(table_version) => {
                                        keep_snapshot_table_versions.contains(&table_version)
                                    }
                                    None => false,
                                };

                            if !is_table_version_in_reference
                                && !referenced_files.contains(&location)
                            {
                                batch_purge_files.push(location);
                                if batch_purge_files.len() >= BATCH_PURGE_FILE_NUM {
                                    count += batch_purge_files.len();
                                    let end = self
                                        .batch_gc_files(&mut batch_purge_files, purge_files)
                                        .await?;
                                    let status = format!(
                                        "do_vacuum with table {}: do_gc_files_in_dir in prefix {}, count:{},cos:{} sec, end: {}",
                                        self.fuse_table.get_table_info().name,
                                        prefix,
                                        count,
                                        self.start.elapsed().as_secs(),
                                        end
                                    );
                                    self.log_status(status);
                                    if end {
                                        return Ok(end);
                                    }
                                }
                            }
                        }
                        _ => {
                            warn!("found not snapshot file in {:}, found: {:?}", prefix, de);
                            continue;
                        }
                    }
                }

                if !batch_purge_files.is_empty() {
                    count += batch_purge_files.len();
                    let end = self
                        .batch_gc_files(&mut batch_purge_files, purge_files)
                        .await?;
                    let status = format!(
                        "do_vacuum with table {}: do_gc_files_in_dir in prefix {}, count:{},cos:{} sec, end: {}",
                        self.fuse_table.get_table_info().name,
                        prefix,
                        count,
                        self.start.elapsed().as_secs(),
                        end
                    );
                    self.log_status(status);
                    if end {
                        return Ok(end);
                    }
                }
            }
        }

        Ok(false)
    }

    #[async_backtrace::framed]
    async fn gc_files(
        &self,
        context: VacuumContext,
        purge_files: &mut Option<&mut Vec<String>>,
    ) -> Result<()> {
        let snapshot_files_with_time = &context.snapshot_files_with_time;
        let root_gc_snapshot_index = context.root_gc_snapshot_index;
        let mut purge_snapshot_files = vec![];
        if root_gc_snapshot_index < snapshot_files_with_time.len() - 1 {
            for snapshot_file_with_time in
                snapshot_files_with_time[root_gc_snapshot_index + 1..].iter()
            {
                let snapshot_location = snapshot_file_with_time.1.clone();
                purge_snapshot_files.push(snapshot_location);
            }
        }
        if let Some(purge_files) = purge_files {
            purge_files.extend(purge_snapshot_files.clone());
            if purge_files.len() > DRY_RUN_LIMIT {
                return Ok(());
            }
        }

        // purge unreference files
        let root_gc_snapshot_reference_files = &context.root_gc_snapshot_reference_files;
        let reference_file_set = vec![
            &root_gc_snapshot_reference_files.blocks_index,
            &root_gc_snapshot_reference_files.blocks,
            &root_gc_snapshot_reference_files.segments,
        ];
        for referenced_files in reference_file_set {
            if self
                .gc_files_in_dir(&context, referenced_files, purge_files)
                .await?
            {
                return Ok(());
            }
        }

        // purge old of retention time snapshot files
        let files: HashSet<String> = purge_snapshot_files.iter().cloned().collect();
        self.fuse_table
            .try_purge_location_files(self.ctx.clone(), files)
            .await?;
        let status = format!(
            "do_vacuum with table {}: purge snapshot files:{},cos:{} sec",
            self.fuse_table.get_table_info().name,
            purge_snapshot_files.len(),
            self.start.elapsed().as_secs(),
        );
        self.log_status(status);

        Ok(())
    }

    #[async_backtrace::framed]
    async fn gc(&self, purge_files: &mut Option<&mut Vec<String>>) -> Result<()> {
        let snapshot_files_with_time = self.list_snapshot_files_with_time().await?;
        if snapshot_files_with_time.is_empty() {
            self.log_status(format!(
                "do_vacuum with table {}: list_snapshot_files_with_time return empty",
                self.fuse_table.get_table_info().name
            ));
            return Ok(());
        }
        self.log_status(format!(
            "do_vacuum with table {}: list_snapshot_files_with_time:{}, cost:{} sec",
            self.fuse_table.get_table_info().name,
            snapshot_files_with_time.len(),
            self.start.elapsed().as_secs()
        ));

        let (root_gc_snapshot_index, root_gc_snapshot_location) = match self
            .get_root_gc_snapshot_file(&snapshot_files_with_time)
            .await?
        {
            Some((index, root_gc_snapshot_location)) => {
                self.log_status(format!(
                    "do_vacuum with table {}: get_root_gc_snapshot_file {:?}",
                    self.fuse_table.get_table_info().name,
                    self.root_snapshot_location,
                ));
                (index, root_gc_snapshot_location)
            }
            None => {
                self.log_status(format!(
                    "do_vacuum with table {}: get_root_gc_snapshot_file return None",
                    self.fuse_table.get_table_info().name,
                ));
                return Ok(());
            }
        };

        let root_gc_snapshot = match self
            .fuse_table
            .read_table_snapshot_by_location(root_gc_snapshot_location.clone())
            .await?
        {
            Some(snapshot) => snapshot,
            None => {
                self.log_status(format!(
                    "do_vacuum with table {}: read root gc snapshot with read_table_snapshot_by_location {} return None",
                    self.fuse_table.get_table_info().name,
                    root_gc_snapshot_location
                ));
                return Ok(());
            }
        };

        let root_gc_snapshot_reference_files = self
            .get_root_gc_snapshot_reference_files(root_gc_snapshot.as_ref())
            .await?;
        self.log_status(format!(
            "do_vacuum with table {}: get_root_gc_snapshot_reference_files segment:{}, block:{}, index:{}, cost:{} sec",
            root_gc_snapshot_reference_files.segments.len(),
            root_gc_snapshot_reference_files.blocks.len(),
            root_gc_snapshot_reference_files.blocks_index.len(),
            self.fuse_table.get_table_info().name,
            self.start.elapsed().as_secs()
        ));

        let keep_snapshot_table_versions = self
            .get_keep_snapshot_table_versions(&snapshot_files_with_time, root_gc_snapshot_index);

        let context = VacuumContext {
            snapshot_files_with_time,
            root_gc_snapshot_index,
            root_gc_snapshot_reference_files,
            keep_snapshot_table_versions,
        };

        self.gc_files(context, purge_files).await?;
        self.log_status(format!(
            "do_vacuum with table {}: do_gc_files, cost:{} sec",
            self.fuse_table.get_table_info().name,
            self.start.elapsed().as_secs()
        ));

        Ok(())
    }

    #[async_backtrace::framed]
    fn log_status(&self, status: String) {
        self.ctx.set_status_info(&status);
        info!("{}", status);
    }
}

#[async_backtrace::framed]
pub async fn do_vacuum(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    retention_time: DateTime<Utc>,
    dry_run: bool,
) -> Result<Option<Vec<String>>> {
    let start = Instant::now();
    let retention = Duration::hours(ctx.get_settings().get_retention_period()? as i64);
    // use min(now - get_retention_period(), retention_time) as gc files retention time
    // to protect files that generated by txn which has not been committed being gc.
    let retention_time = std::cmp::min(chrono::Utc::now() - retention, retention_time);

    // Read the root snapshot location.
    let root_snapshot_location = match fuse_table.snapshot_loc().await? {
        Some(root_snapshot_location) => root_snapshot_location,
        None => {
            let status = format!(
                "do_vacuum with table {}: read root snapshot location return None",
                fuse_table.get_table_info().name,
            );
            ctx.set_status_info(&status);
            info!("{}", status);
            return Ok(None);
        }
    };

    let vacuum_operator = VacuumOperator {
        fuse_table: fuse_table.clone(),
        ctx,
        retention_time,
        root_snapshot_location,
        start,
    };
    if dry_run {
        let mut purge_files = Vec::with_capacity(DRY_RUN_LIMIT);
        let mut purge_files_opt = Some(&mut purge_files);
        vacuum_operator.gc(&mut purge_files_opt).await?;
        Ok(Some(purge_files))
    } else {
        let mut purge_files_opt = None;
        vacuum_operator.gc(&mut purge_files_opt).await?;

        Ok(None)
    }
}
