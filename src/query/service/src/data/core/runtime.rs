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

use std::future::Future;
use std::sync::Arc;

use databend_common_base::runtime::GlobalPruningRuntime;
use databend_common_base::runtime::GlobalQueryRuntime;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::BlockMeta;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;

use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

/// Runs the operation `op` on the query runtime, like the pruning of a SQL query,
/// rather than on the runtime of the HTTP server.
pub async fn run<T, F>(op: &str, operation: F) -> Result<T>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    GlobalQueryRuntime::instance()
        .runtime()
        .spawn(operation)
        .await
        .map_err(|e| ErrorCode::UnwindError(format!("data {op} task failed: {e}")))?
}

/// `step` of every block, up to `max_threads` blocks at a time, with the position of the
/// block in `blocks`, in no particular order.
///
/// Each step is a task of the runtime that SQL queries prune on, as many threads as CPUs:
/// steps are CPU bound (index searches), so the blocks of one request search in parallel,
/// and the requests together are not capped by the threads of [`run`]'s runtime. Without
/// that runtime (no global queries executor), the steps run on [`run`]'s runtime.
pub async fn for_each_block<T, F, Fut>(
    ctx: &QueryContext,
    blocks: &[Arc<BlockMeta>],
    step: F,
) -> Result<Vec<(usize, T)>>
where
    T: Send + 'static,
    F: Fn(Arc<BlockMeta>) -> Fut,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    let max_threads = ctx.get_settings().get_max_threads()? as usize;
    let pruning = GlobalPruningRuntime::try_instance();
    let query = GlobalQueryRuntime::instance();
    let runtime: &Runtime = match &pruning {
        Some(runtime) => runtime,
        None => query.runtime(),
    };
    stream::iter(blocks.iter().cloned().enumerate())
        .map(|(position, block)| {
            let task = runtime.spawn(step(block));
            async move {
                let output = task.await.map_err(|e| {
                    ErrorCode::UnwindError(format!("data block task failed: {e}"))
                })??;
                Ok::<_, ErrorCode>((position, output))
            }
        })
        .buffer_unordered(max_threads.max(1))
        .try_collect()
        .await
}
