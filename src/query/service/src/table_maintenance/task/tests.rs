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

use std::future::pending;
use std::time::Duration;

use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::FuseCaches;

use super::*;
use crate::sessions::TableContextTableAccess;
use crate::table_maintenance::MaintenanceContext;
use crate::test_kits::TestFixture;

fn target(table: &str, table_id: u64) -> TableHookTarget {
    TableHookTarget {
        catalog: "default".into(),
        database: "default".into(),
        table: table.into(),
        table_id,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_coalesced_refresh_keeps_oldest_anchor_without_retaining_writer() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let request = async |seq| {
        let mut tasks = MaintenanceTask::from_write(
            &ctx,
            target("t", 1),
            None,
            Some(RefreshAnchor {
                snapshot_location: format!("snapshot-{seq}"),
                base_table_seq: seq,
            }),
        )
        .await?;
        Ok::<_, ErrorCode>(tasks.remove(0))
    };
    let mut refresh = request(107).await?;
    let oldest = request(100).await?;
    let writer = Arc::downgrade(&ctx);
    drop(ctx);
    assert!(writer.upgrade().is_none());

    refresh
        .runtime
        .apply_outcome(Err(ErrorCode::Internal("index build failed")));
    let retry_at = refresh.ready_at();
    refresh.merge(oldest);
    assert_eq!(refresh.ready_at(), retry_at);
    let TaskData::Refresh(anchor) = refresh.data else {
        unreachable!()
    };
    assert_eq!(anchor.base_table_seq, 100);
    assert_eq!(anchor.snapshot_location, "snapshot-100");
    Ok(())
}

#[tokio::test]
async fn test_warehouse_idle_probe_does_not_share_timeout_after_waiting() {
    let probes = WarehouseIdleProbes::default();
    let cancellation = CancellationToken::new();
    let expires = Instant::now() + MAX_TASK_BLOCKED;
    let slot = Arc::new(WarehouseProbe::default());
    let guard = slot.lock().await;
    probes.0.lock().insert("a".into(), slot.clone());

    // The previous probe leaves no sample. The waiter takes over with less time.
    let (timed_out, ()) = tokio::join!(
        biased;
        probes.check_with("a", expires, &cancellation, pending()),
        async { drop(guard) },
    );
    assert_eq!(timed_out, None);
    assert!(
        probes
            .check_with("a", expires, &cancellation, async { true })
            .await
            .is_some()
    );
}

#[test]
fn test_only_real_progress_resets_the_attempt_budget() {
    let mut runtime = TaskRuntime::new(Instant::now());
    runtime.failures = MAX_TASK_FAILURES - 1;
    assert_eq!(
        runtime.apply_outcome(Ok(MaintenanceOutcome::ScanProgress)),
        TaskDecision::Continue
    );
    assert_eq!(
        runtime.apply_outcome(Err(ErrorCode::Internal("unrepairable range"))),
        TaskDecision::Exhausted("retry_exhausted")
    );
    runtime.blocked_since = Some(Instant::now() - MAX_TASK_BLOCKED);
    assert_eq!(
        runtime.apply_outcome(Ok(MaintenanceOutcome::ScanProgress)),
        TaskDecision::Exhausted("no_progress")
    );
    assert_eq!(
        runtime.apply_outcome(Ok(MaintenanceOutcome::Progress(Duration::ZERO))),
        TaskDecision::Continue
    );
    assert_eq!(runtime.failures, 0);
    assert!(runtime.blocked_since.is_none());
    assert_eq!(
        runtime.apply_outcome(resource_outcome(Err(ErrorCode::NoResourcesAvailable(
            "peer busy"
        )))),
        TaskDecision::Continue
    );
    assert_eq!(runtime.failures, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_background_refresh_needs_commit_gate_and_baseline() -> Result<()> {
    let fixture = TestFixture::setup_with_stopped_maintenance().await?;
    fixture
        .execute_command(
            "CREATE TABLE refresh_guard(s STRING, ASYNC INVERTED INDEX inv(s)) \
             AUTO_COMPACTION_IMPERFECT_BLOCKS_THRESHOLD=0",
        )
        .await?;
    fixture
        .execute_command("INSERT INTO refresh_guard VALUES ('alpha')")
        .await?;
    let fetch_table = async || {
        fixture
            .new_query_ctx()
            .await?
            .get_table("default", "default", "refresh_guard")
            .await
    };
    let base = fetch_table().await?;
    let fuse = FuseTable::try_from_table(base.as_ref())?;
    let baseline = fuse.snapshot_loc().unwrap();
    fixture
        .execute_command("INSERT INTO refresh_guard VALUES ('beta')")
        .await?;
    let head = fetch_table().await?;
    let anchor = RefreshAnchor {
        snapshot_location: FuseTable::try_from_table(head.as_ref())?
            .snapshot_loc()
            .unwrap(),
        base_table_seq: base.get_table_info().ident.seq,
    };
    let target = target("refresh_guard", head.get_id());
    let writer = fixture.new_query_ctx().await?;
    let identity = MaintenanceIdentity::capture(&writer).await?;
    let ctx = MaintenanceContext::from_identity(&identity).await?.ctx;
    let lock = writer
        .acquire_table_lock_by_id("default", head.get_id(), &LockTableOption::LockWithRetry)
        .await?
        .expect("foreground table lock");
    let result = refresh_hook::run(ctx.clone(), &target, &anchor).await;
    assert_eq!(
        result.err().map(|err| err.code()),
        Some(ErrorCode::TABLE_ALREADY_LOCKED)
    );
    drop(lock);

    fuse.get_operator().delete(&baseline).await?;
    FuseCaches::instance().snapshots.evict(&baseline);
    assert!(matches!(
        refresh_hook::run(ctx, &target, &anchor).await?,
        MaintenanceOutcome::Completed
    ));
    let latest = fetch_table().await?;
    assert_eq!(
        FuseTable::try_from_table(latest.as_ref())?.snapshot_loc(),
        Some(anchor.snapshot_location),
        "missing history must not turn an incremental Refresh into a full-table publication"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hook_target_follows_rename_but_not_replacement() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let create = "CREATE TABLE refresh_target(s STRING)";
    fixture.execute_command(create).await?;
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx
        .get_table("default", "default", "refresh_target")
        .await?;
    let target = target("refresh_target", table.get_id());
    fixture
        .execute_command("ALTER TABLE refresh_target RENAME TO renamed")
        .await?;
    fixture.execute_command(create).await?;
    let renamed = target.resolve(&ctx, "refresh").await?.unwrap();
    assert_eq!(renamed.table, "renamed");

    fixture.execute_command("DROP TABLE renamed").await?;
    assert!(target.resolve(&ctx, "refresh").await?.is_none());
    Ok(())
}
