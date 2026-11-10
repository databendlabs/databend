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

// Logs from this module will show up as "[TABLE-HOOK] ...".
databend_common_tracing::register_module_tag!("[TABLE-HOOK]");

use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::Result;
use databend_common_pipeline::core::SharedLockGuard;
use log::info;
use log::warn;

use crate::interpreters::hook::compact_hook::compact_after_write_enabled;
use crate::interpreters::hook::resolve_current_table_name_by_id;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextPartitionStats;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct TableHookTaskKey {
    pub(super) catalog: String,
    pub(super) table_id: u64,
}

pub(super) trait SchedulerTask: Send + 'static {
    fn key(&self) -> TableHookTaskKey;

    fn merge(&mut self, other: Self);
}

pub struct TableHookTarget {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub table_id: u64,
}

impl TableHookTarget {
    /// Resolve separately for each stage: maintenance can outlive a rename or drop.
    /// Resolution refreshes the table cache and clears cached table metadata timestamps.
    pub(super) async fn resolve(
        &self,
        ctx: &Arc<QueryContext>,
        hook_name: &str,
    ) -> Result<Option<Self>> {
        let Some((database, table)) = resolve_current_table_name_by_id(
            ctx,
            hook_name,
            &self.catalog,
            &self.database,
            &self.table,
            Some(self.table_id),
        )
        .await?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            catalog: self.catalog.clone(),
            database,
            table,
            table_id: self.table_id,
        }))
    }
}

pub struct TableHookTask {
    pub ctx: Arc<QueryContext>,
    pub target: TableHookTarget,
    pub hook_settings: TableHookTaskSettings,
    pub lock_opt: LockTableOption,
    /// Only synchronous autocommit tasks carry a statement lock. Queued and post-commit
    /// tasks carry none and acquire maintenance locks where required.
    pub lock_guard: Option<SharedLockGuard>,
    pub operation_name: String,
    pub main_operation_start: Instant,
}

impl TableHookTask {
    /// Run the same maintenance chain for autocommit and post-commit hooks, in both modes.
    /// Release any handed-over statement lock after refresh and before analyze.
    ///
    /// Each stage lives with its own hook module: `execute_compact` in `compact_hook.rs`,
    /// `execute_refresh` in `refresh_hook.rs`, and `execute_analyze` in `analyze_hook.rs`.
    pub(crate) async fn execute(mut self, is_async: bool) {
        if is_async {
            info!(
                "Start async table hook job for operation {}, table_id={}, table={}",
                self.operation_name, self.target.table_id, self.target.table
            );
        } else {
            info!(
                "Start sync table hook job for operation {}, table_id={}, table={}",
                self.operation_name, self.target.table_id, self.target.table
            );
        }
        if self.hook_settings.compact_after_write {
            self.execute_compact().await.unwrap_or_else(|e| {
                warn!(
                    "Table hook compact failed, table_id={}: {e}",
                    self.target.table_id
                )
            });
        }
        self.execute_refresh().await;
        if let Some(guard) = self.lock_guard.take() {
            drop(guard.try_take());
        }
        self.execute_analyze().await;
        if is_async {
            info!(
                "Async table hook job completed for operation {}, table_id={}, table={}",
                self.operation_name, self.target.table_id, self.target.table
            );
        } else {
            info!(
                "Sync table hook job completed for operation {}, table_id={}, table={}",
                self.operation_name, self.target.table_id, self.target.table
            );
        }
    }
}

impl SchedulerTask for TableHookTask {
    fn key(&self) -> TableHookTaskKey {
        TableHookTaskKey {
            catalog: self.target.catalog.clone(),
            table_id: self.target.table_id,
        }
    }

    /// Combine pending inputs while keeping the latest task's execution context.
    fn merge(&mut self, mut other: Self) {
        other.hook_settings.merge(self.hook_settings);

        other
            .ctx
            .written_segment_locations()
            .extend(self.ctx.written_segment_locations().list());

        let compaction_num_block_hint = self
            .ctx
            .get_compaction_num_block_hint(&self.target.table)
            .saturating_add(other.ctx.get_compaction_num_block_hint(&other.target.table));
        if compaction_num_block_hint > 0 {
            other
                .ctx
                .set_compaction_num_block_hint(&other.target.table, compaction_num_block_hint);
        }

        if self.ctx.get_enable_auto_analyze() {
            other.ctx.set_enable_auto_analyze(true);
        }

        other.main_operation_start = self.main_operation_start;
        other.operation_name = self.operation_name.clone();

        *self = other;
    }
}

#[derive(Clone, Copy)]
pub struct TableHookTaskSettings {
    pub compact_after_write: bool,
}

impl TableHookTaskSettings {
    pub fn create(ctx: &Arc<QueryContext>) -> Self {
        Self {
            compact_after_write: compact_after_write_enabled(ctx),
        }
    }

    fn merge(&mut self, other: Self) {
        self.compact_after_write |= other.compact_after_write;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sessions::TableContextSettings;
    use crate::test_kits::TestFixture;

    fn table_hook_task(ctx: Arc<QueryContext>, table: &str) -> TableHookTask {
        let hook_settings = TableHookTaskSettings::create(&ctx);
        TableHookTask {
            ctx,
            target: TableHookTarget {
                catalog: "default".to_string(),
                database: "default".to_string(),
                table: table.to_string(),
                table_id: 1,
            },
            hook_settings,
            lock_opt: LockTableOption::NoLock,
            lock_guard: None,
            operation_name: "insert".to_string(),
            main_operation_start: Instant::now(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_table_hook_task_merge_accumulates_compaction_hint() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let old_ctx = fixture.new_query_ctx().await?;
        let latest_ctx = fixture.new_query_ctx().await?;

        old_ctx.set_compaction_num_block_hint("t", 7);
        latest_ctx.set_compaction_num_block_hint("t", 5);
        let mut task = table_hook_task(old_ctx, "t");
        task.merge(table_hook_task(latest_ctx, "t"));

        assert_eq!(task.ctx.get_compaction_num_block_hint("t"), 12);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_table_hook_task_merge_preserves_hook_settings() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let old_ctx = fixture.new_query_ctx().await?;
        let latest_ctx = fixture.new_query_ctx().await?;

        old_ctx
            .get_settings()
            .set_setting("enable_compact_after_write".to_string(), "1".to_string())?;
        latest_ctx
            .get_settings()
            .set_setting("enable_compact_after_write".to_string(), "0".to_string())?;

        let mut task = table_hook_task(old_ctx, "t");
        task.merge(table_hook_task(latest_ctx, "t"));

        assert!(task.hook_settings.compact_after_write);
        Ok(())
    }
}
