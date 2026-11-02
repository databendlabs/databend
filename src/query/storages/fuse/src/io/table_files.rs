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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_io::Files;
use opendal::Operator;

/// Deletes files of a fuse table, and drops them from the disk cache: a deleted snapshot would
/// otherwise still be read when the snapshot history is walked, and deleted segments, blocks
/// and indexes would hold disk cache room until evicted.
///
/// The files leave the disk cache before the delete and are kept out until it is done, failed
/// or cancelled: a fill that read a file before is rejected, one that starts after finds no
/// file, and a crash meanwhile leaves none of them on disk.
pub struct TableFiles {
    files: Files,
    disk: DiskCachedStorage,
}

impl TableFiles {
    pub fn create(ctx: Arc<dyn TableContext>, operator: Operator) -> Self {
        TableFiles {
            disk: DiskCachedStorage::new(operator.clone()),
            files: Files::create(ctx, operator),
        }
    }

    pub async fn remove_file_in_batch(&self, locations: &[String]) -> Result<()> {
        // Ended when the delete is done, or dropped: a cancelled query drops it at the await.
        // A file whose cached copy cannot be deleted is not deleted: it would be read again
        // from the disk cache after a restart.
        let _changes = locations
            .iter()
            // As `Files` deletes it, and as readers name it.
            .map(|location| self.disk.begin_change(location.trim_start_matches('/')))
            .collect::<Result<Vec<_>>>()?;
        self.files.remove_file_in_batch(locations).await
    }
}
