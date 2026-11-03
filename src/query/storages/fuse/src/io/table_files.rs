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

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::spawn_blocking;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_io::Files;
use opendal::Operator;

/// Deletes files of a fuse table, and drops them from the disk cache: a deleted snapshot would
/// otherwise still be read when the snapshot history is walked, and deleted segments, blocks
/// and indexes would hold disk cache room until evicted.
///
/// The files leave the disk cache before the delete and are kept out until it has finished,
/// failed or not: a fill that read a file before is rejected, one that starts after finds no
/// file, and a crash meanwhile leaves none of them on disk. The invalidation and the delete run
/// to their end in the background when the caller is cancelled, as its storage requests do,
/// and keep the files out until then.
pub struct TableFiles {
    ctx: Arc<dyn TableContext>,
    operator: Operator,
    disk: DiskCachedStorage,
}

impl TableFiles {
    pub fn create(ctx: Arc<dyn TableContext>, operator: Operator) -> Self {
        TableFiles {
            disk: DiskCachedStorage::new(operator.clone()),
            ctx,
            operator,
        }
    }

    pub async fn remove_file_in_batch(&self, locations: &[String]) -> Result<()> {
        let disk = self.disk.clone();
        let files = Files::create(self.ctx.clone(), self.operator.clone());
        let locations = locations.to_vec();
        GlobalIORuntime::instance()
            .spawn(async move {
                // Deleting the cached copies is file system work, many files for a large
                // batch: on a blocking thread, not an IO worker. A file whose cached copy
                // cannot be deleted is not deleted: it would be read again from the disk cache
                // after a restart.
                let names = locations.clone();
                let changes = spawn_blocking(move || {
                    names
                        .iter()
                        // As `Files` deletes it, and as readers name it.
                        .map(|location| disk.begin_change(location.trim_start_matches('/')))
                        .collect::<Result<Vec<_>>>()
                })
                .await
                .map_err(|e| {
                    ErrorCode::Internal(format!("disk cache invalidation task failed: {e}"))
                })??;
                let removed = files.remove_file_in_batch(&locations).await;
                drop(changes);
                removed
            })
            .await
            .map_err(|e| ErrorCode::Internal(format!("table file delete task failed: {e}")))?
    }
}
