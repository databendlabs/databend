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
use std::time::Instant;

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use log::info;
use opendal::Operator;

/// Deduplicate a list of file locations in place.
/// Returns (number_of_duplicates_removed, up_to_5_sample_duplicate_paths).
pub fn dedup_file_locations(locations: &mut Vec<String>) -> (usize, Vec<String>) {
    let len_before = locations.len();
    locations.sort_unstable();
    let dup_samples: Vec<String> = locations
        .windows(2)
        .filter(|w| w[0] == w[1])
        .map(|w| w[0].clone())
        .take(5)
        .collect();
    locations.dedup();
    (len_before - locations.len(), dup_samples)
}

// File related operations.
pub struct Files {
    ctx: Arc<dyn TableContext>,
    operator: Operator,
}

impl Files {
    pub fn create(ctx: Arc<dyn TableContext>, operator: Operator) -> Self {
        Self { ctx, operator }
    }

    /// Removes a batch of files asynchronously by splitting a list of file locations into smaller groups of size 1000,
    /// and then deleting each group of files using the delete_files function.
    #[fastrace::trace]
    // #[async_backtrace::framed]
    pub async fn remove_file_in_batch(
        &self,
        file_locations: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<()> {
        let mut locations: Vec<String> = file_locations
            .into_iter()
            .map(|v| v.as_ref().trim_start_matches('/').to_string())
            .filter(|v| !v.is_empty())
            .collect();

        if locations.is_empty() {
            return Ok(());
        }

        // Deduplicate: opendal's Deleter uses a HashSet internally but tracks size by
        // insertion count. Duplicates cause cur_size to diverge from the buffer, making
        // Deleter::close() loop forever.
        let (duplicates, dup_samples) = dedup_file_locations(&mut locations);
        if duplicates > 0 {
            info!(
                "remove_file_in_batch: deduplicated {} entries ({} -> {}), duplicate samples: {:?}",
                duplicates,
                duplicates + locations.len(),
                locations.len(),
                dup_samples
            );
        }

        // Number of object keys deleted per outer chunk. Kept independent of `max_threads`:
        // a single batch-delete request (e.g. S3 DeleteObjects) costs one request regardless
        // of key count, so a larger chunk means fewer requests, which is strictly better for
        // both throughput and request-rate limits. Runtime-tunable via `storage_delete_batch_size`.
        //
        // The chunk size is capped by the backend's batch-delete capability
        // (`delete_max_size`, e.g. S3 1000, Azure 256, GCS 100). Chunking above that limit
        // would let opendal's `Deleter` flush backend-sized sub-batches *serially* inside a
        // single `delete_files` future, bypassing `execute_futures_in_parallel` and losing
        // the `permit` concurrency those backends still need. Capping at the backend limit
        // keeps one request per chunk and preserves cross-chunk parallelism.
        //
        // Backends that advertise no batch-delete capability (`delete_max_size` unset, e.g.
        // the `fs` service) delete one key per request; opendal's `Deleter` treats the missing
        // capability as `max_size = 1`. Mirror that with `unwrap_or(1)` so such backends are
        // chunked into single-key tasks that run at `permit` concurrency, rather than collapsing
        // into one future that deletes serially.
        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let backend_delete_limit = self
            .operator
            .info()
            .full_capability()
            .delete_max_size
            .unwrap_or(1)
            .max(1);
        let batch_size = (self.ctx.get_settings().get_storage_delete_batch_size()? as usize)
            .clamp(1, backend_delete_limit);

        info!(
            "remove file in batch, batch_size: {}, number of chunks {}",
            batch_size,
            (locations.len() / batch_size).max(1)
        );

        if locations.len() <= batch_size {
            Self::delete_files(self.operator.clone(), locations).await?;
        } else {
            let mut chunks = locations.chunks(batch_size);

            let tasks = std::iter::from_fn(move || {
                chunks
                    .next()
                    .map(|location| Self::delete_files(self.operator.clone(), location.to_vec()))
            });

            // At most 3 concurrent batch deletions allowed, mitigate rate limit errors
            let permit = (threads_nums / 2).clamp(1, 3);

            // IO tasks, 2 threads should be enough
            let pool_thread_number = 2;
            execute_futures_in_parallel(
                tasks,
                pool_thread_number,
                permit,
                "batch-remove-files-worker".to_owned(),
            )
            .await?
            .into_iter()
            .collect::<Result<_>>()?
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn delete_files(op: Operator, locations: Vec<String>) -> Result<()> {
        let start = Instant::now();
        info!("deleting files {:?}", &locations);
        let num_of_files = locations.len();

        op.delete_iter(locations).await?;

        info!(
            "deleted files, number of files {}, time used {:?}",
            num_of_files,
            start.elapsed(),
        );
        Ok(())
    }
}
