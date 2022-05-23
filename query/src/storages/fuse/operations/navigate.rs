//  Copyright 2021 Datafuse Labs.
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
//

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common_exception::Result;
use common_meta_types::TableStatistics;
use futures::TryStreamExt;

use crate::sessions::QueryContext;
use crate::sql::OPT_KEY_SNAPSHOT_LOCATION;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::FuseTable;

impl FuseTable {
    pub async fn navigate(
        &self,
        ctx: &Arc<QueryContext>,
        time_point: DateTime<Utc>,
    ) -> Result<Option<Arc<FuseTable>>> {
        let snapshot_location = if let Some(loc) = self.snapshot_loc() {
            loc
        } else {
            // not an error?
            return Ok(None);
        };

        let snapshot_version = self.snapshot_format_version();
        let reader = MetaReaders::table_snapshot_reader(ctx);

        // grab the table history
        let mut snapshots = reader.snapshot_history(
            snapshot_location,
            snapshot_version,
            self.meta_location_generator().clone(),
        );

        // Find the instant which matched ths given `time_point`.
        // The history returned is ordered by timestamp desc.
        let mut instant = None;
        while let Some(snapshot) = snapshots.try_next().await? {
            if let Some(ts) = snapshot.timestamp {
                // break on the first one
                if ts <= time_point {
                    instant = Some(snapshot)
                }
            }
        }

        if let Some(snapshot) = instant {
            // Load the table instance by the snapshot

            // The `seq` of ident that we cloned here is JUST a place holder
            // we should NOT use it other than a pure place holder.
            // Fortunately, historical table should be read-only.
            // - Although, caller of fuse table will not perform mutation on a historical table
            //   but in case there is careless refactorings, an extra attribute `read_only` is
            //   added the FuseTable, and during mutation operations, FuseTable will check it.
            // - Figuring out better way...
            let mut table_info = self.table_info.clone();

            // There are more to be kept in snapshot, like engine_options, ordering keys...
            // or we could just keep a clone of TableMeta in the snapshot.
            //
            // currently, here are what we can recovery from the snapshot:

            // 1. the table schema
            table_info.meta.schema = Arc::new(snapshot.schema.clone());

            // 2. the table option `snapshot_location`
            let ver = snapshot.format_version();
            let loc = self
                .meta_location_generator
                .snapshot_location_from_uuid(&snapshot.snapshot_id, ver)?;
            table_info
                .meta
                .options
                .insert(OPT_KEY_SNAPSHOT_LOCATION.to_owned(), loc);

            // 3. The statistics
            let summary = &snapshot.summary;
            table_info.meta.statistics = TableStatistics {
                number_of_rows: summary.row_count,
                data_bytes: summary.uncompressed_byte_size,
                compressed_data_bytes: summary.compressed_byte_size,
                index_data_bytes: 0, // we do not have it yet
            };

            // let's instantiate it
            let read_only = true;
            let fuse_tbl = FuseTable::do_create(table_info, read_only)?;
            Ok(Some(fuse_tbl.into()))
        } else {
            Ok(None)
        }
    }
}
