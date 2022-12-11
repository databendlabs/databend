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

use std::sync::Arc;

use common_base::base::tokio::sync::Semaphore;
use common_base::runtime::Runtime;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use futures_util::future;
use opendal::Operator;
use tracing::warn;
use tracing::Instrument;

// File related operations.
pub struct Files {
    ctx: Arc<dyn TableContext>,
    operator: Operator,
}

impl Files {
    pub fn create(ctx: Arc<dyn TableContext>, operator: Operator) -> Self {
        Self { ctx, operator }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn remove_file_in_batch(
        &self,
        file_locations: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<()> {
        let ctx = self.ctx.clone();
        let max_runtime_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

        // 1.1 combine all the tasks.
        let mut iter = file_locations.into_iter();
        let tasks = std::iter::from_fn(move || {
            if let Some(location) = iter.next() {
                let location = location.as_ref().to_owned();
                Some(
                    Self::remove_file(self.operator.clone(), location)
                        .instrument(tracing::debug_span!("remove_file")),
                )
            } else {
                None
            }
        });

        // 1.2 build the runtime.
        let semaphore = Semaphore::new(max_io_requests);
        let file_runtime = Arc::new(Runtime::with_worker_threads(
            max_runtime_threads,
            Some("fuse-req-remove-files-worker".to_owned()),
        )?);

        // 1.3 spawn all the tasks to the runtime.
        let join_handlers = file_runtime.try_spawn_batch(semaphore, tasks).await?;

        // 1.4 get all the result.
        future::try_join_all(join_handlers).await.map_err(|e| {
            ErrorCode::StorageOther(format!("remove files in batch failure, {}", e))
        })?;
        Ok(())
    }

    async fn remove_file(operator: Operator, block_location: impl AsRef<str>) -> Result<()> {
        match Self::do_remove_file_by_location(operator, block_location.as_ref()).await {
            Err(e) if e.code() == ErrorCode::STORAGE_NOT_FOUND => {
                warn!(
                    "concurrent remove: file of location {} already removed",
                    block_location.as_ref()
                );
                Ok(())
            }
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        }
    }

    // make type checker happy
    #[inline]
    async fn do_remove_file_by_location(operator: Operator, location: &str) -> Result<()> {
        Ok(operator.object(location.as_ref()).delete().await?)
    }
}
