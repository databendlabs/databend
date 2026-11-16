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

use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_io::Files;
use opendal::Operator;

/// Deletes files of a fuse table, and drops them from the disk cache: a deleted snapshot would
/// otherwise still be read when the snapshot history is walked, and deleted segments, blocks
/// and indexes would hold disk cache room until evicted. Dropping them is best effort, as for
/// the memory caches (see `DiskCache::evict_object`).
///
/// The delete and the eviction after it run to their end on the IO runtime when the caller is
/// cancelled: a delete may have gone through, in part or whole, by then.
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
                let removed =
                    CatchUnwindFuture::create(files.remove_file_in_batch(&locations)).await;
                // After the delete, failed or not, even on a memory limit panic: some files may
                // be gone.
                disk.evict_objects(&locations).await?;
                match removed {
                    Ok(removed) => removed,
                    Err(cause) => Err(cause.with_context("table file delete panicked")),
                }
            })
            .await
            .map_err(|e| ErrorCode::Internal(format!("table file delete task failed: {e}")))?
    }
}
