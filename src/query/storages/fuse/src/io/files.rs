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
    #[minitrace::trace]
    // #[async_backtrace::framed]
    pub async fn remove_file_in_batch(
        &self,
        file_locations: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<()> {
        let locations = Vec::from_iter(file_locations.into_iter().map(|v| v.as_ref().to_string()));

        if locations.is_empty() {
            return Ok(());
        }

        // adjusts batch_size according to the `max_threads` settings,
        // limits its min/max value to 1 and 1000.
        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let batch_size = (locations.len() / threads_nums).min(1000).max(1);

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

            execute_futures_in_parallel(
                tasks,
                threads_nums,
                threads_nums * 2,
                "batch-remove-files-worker".to_owned(),
            )
            .await?;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn delete_files(op: Operator, locations: Vec<String>) -> Result<()> {
        let start = Instant::now();
        // temporary fix for https://github.com/datafuselabs/databend/issues/13804
        let locations = locations
            .into_iter()
            .map(|loc| loc.trim_start_matches('/').to_owned())
            .filter(|loc| !loc.is_empty())
            .collect::<Vec<_>>();
        info!("deleting files {:?}", &locations);
        let num_of_files = locations.len();

        op.remove(locations).await?;

        info!(
            "deleted files, number of files {}, time used {:?}",
            num_of_files,
            start.elapsed(),
        );
        Ok(())
    }
}
