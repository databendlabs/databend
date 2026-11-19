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

use std::time::Duration;

use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use futures::TryStreamExt;
use tokio_util::sync::CancellationToken;

use super::MaintenanceOutcome as TaskOutcome;
use super::*;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::table_maintenance::MaintenanceContext;
use crate::table_maintenance::TableMaintenanceService;
use crate::test_kits::TestFixture;

fn target(database: &str, table: &str, table_id: u64) -> TableHookTarget {
    TableHookTarget {
        catalog: "default".into(),
        database: database.into(),
        table: table.into(),
        table_id,
    }
}

async fn run_task(task: MaintenanceTask, coverage: &RefreshCoverage) -> RunResult<MaintenanceTask> {
    let service = TableMaintenanceService::instance();
    task.execute(CancellationToken::new(), service.resources(), coverage, 1)
        .await
}

// The foreground already owns one revision. Wait until the publisher requests its own.
async fn wait_for_publication_waiter(ctx: &QueryContext, table_id: u64) -> Result<()> {
    use databend_common_meta_app::schema::ListLockRevReq;
    use databend_common_meta_app::schema::LockKey;

    let catalog = ctx.get_catalog("default").await?;
    let key = LockKey::Table {
        tenant: ctx.get_tenant(),
        table_id,
    };
    tokio::time::timeout(Duration::from_secs(20), async {
        while catalog
            .list_lock_revisions(ListLockRevReq::new(key.clone()))
            .await?
            .len()
            < 2
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await
    .expect("publisher did not request the foreground lock")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_table_hook_request_merge_releases_write_context() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let old_ctx = fixture.new_query_ctx().await?;
    let latest_ctx = fixture.new_query_ctx().await?;
    old_ctx
        .get_settings()
        .set_global_setting("recluster_accumulation_secs".into(), "600".into())
        .await?;

    let requests = async |ctx: &Arc<QueryContext>| {
        let target = target("default", "t", 1);
        MaintenanceTask::from_write(ctx, target.clone(), false, Some(target)).await
    };
    let old_weak = Arc::downgrade(&old_ctx);
    let before_enqueue = Instant::now();
    let mut tasks = requests(&old_ctx).await?;
    drop(old_ctx);
    assert!(
        old_weak.upgrade().is_none(),
        "queued work retained the write context"
    );
    let mut recluster = tasks.pop().unwrap();
    assert_eq!(recluster.kind(), TaskKind::Recluster);
    let dirty_deadline = recluster.ready_at();
    assert!(dirty_deadline >= before_enqueue + Duration::from_secs(600));
    assert!(dirty_deadline <= Instant::now() + Duration::from_secs(600));
    let policy = databend_common_settings::Settings::create(recluster.identity.tenant.clone());
    policy
        .set_global_setting("recluster_accumulation_secs".into(), "0".into())
        .await?;
    assert_eq!(
        latest_ctx
            .get_settings()
            .get_recluster_accumulation_secs()?,
        600
    );
    let before_stale_enqueue = Instant::now();
    let stale = requests(&latest_ctx).await?.pop().unwrap();
    assert!(
        stale.ready_at() >= before_stale_enqueue + Duration::from_secs(600),
        "enqueue must use the session's cached value rather than reload GLOBAL settings"
    );
    recluster.merge(stale);
    assert_eq!(recluster.ready_at(), dirty_deadline);

    // Explicitly reload the session to observe the new global value.
    latest_ctx.get_settings().load_changes().await?;
    let mut immediate = requests(&latest_ctx).await?.pop().unwrap();
    assert!(
        immediate.ready_at() <= Instant::now(),
        "a session that loaded a zero window must create an immediately eligible generation"
    );
    let immediate_deadline = immediate.ready_at();
    policy
        .set_global_setting("recluster_accumulation_secs".into(), "600".into())
        .await?;
    assert_eq!(
        latest_ctx
            .get_settings()
            .get_recluster_accumulation_secs()?,
        0
    );
    latest_ctx.get_settings().load_changes().await?;
    let delayed = requests(&latest_ctx).await?.pop().unwrap();
    assert!(delayed.ready_at() > immediate_deadline);
    immediate.merge(delayed);
    assert_eq!(
        immediate.ready_at(),
        immediate_deadline,
        "raising the global window must not postpone an existing generation"
    );
    recluster.merge(immediate);
    assert_eq!(
        recluster.ready_at(),
        dirty_deadline,
        "new writes or global policy changes must not move the first dirty deadline"
    );
    let error = policy
        .set_global_setting("recluster_accumulation_secs".into(), "43201".into())
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::WRONG_VALUE_FOR_VARIABLE);
    policy
        .set_global_setting("recluster_accumulation_secs".into(), "43200".into())
        .await?;
    assert_eq!(policy.get_recluster_accumulation_secs()?, 43200);
    policy
        .set_global_setting("recluster_accumulation_secs".into(), "0".into())
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_recluster_notifications_preserve_scan_progress() -> Result<()> {
    use databend_common_storages_fuse::FuseTable;

    use crate::interpreters::hook::recluster_hook::ReclusterTask;

    let fixture = TestFixture::setup_with_stopped_maintenance().await?;
    fixture
        .execute_command(
            "CREATE TABLE hook_rescan(id INT) CLUSTER BY (id) ROW_PER_BLOCK=1 BLOCK_PER_SEGMENT=1",
        )
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    let settings = ctx.get_settings();
    ctx.get_session_settings().set_max_threads(1)?;
    settings.set_setting("enable_compact_after_write".into(), "0".into())?;
    settings.set_setting("auto_compaction_segments_limit".into(), "2".into())?;
    TestFixture::execute_query_with_ctx(
        ctx.clone(),
        "INSERT INTO hook_rescan SELECT number FROM numbers(1025)",
    )
    .await?
    .try_collect::<Vec<_>>()
    .await?;
    let table = fixture
        .new_query_ctx()
        .await?
        .get_table("default", "default", "hook_rescan")
        .await?;
    let snapshot = FuseTable::try_from_table(table.as_ref())?
        .read_table_snapshot()
        .await?
        .unwrap();
    assert_eq!(snapshot.segments.len(), 1025);
    let target = target("default", "hook_rescan", table.get_id());
    let mut task = ReclusterTask::new();
    // With one thread the background scan range is 1024, even though the inline
    // limit is 2. Writes between the two units request one extra pass, not a rewind.
    for unit in 0..4 {
        if unit == 1 {
            task.merge();
            task.merge();
        }
        let outcome = task.run(ctx.clone(), &target).await?;
        if unit == 3 {
            assert!(matches!(outcome, TaskOutcome::Completed));
        } else {
            assert!(
                matches!(&outcome, TaskOutcome::ScanProgress),
                "unit={unit}, completed={}, committed={}",
                matches!(&outcome, TaskOutcome::Completed),
                matches!(&outcome, TaskOutcome::Progress(_))
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_bounded_failures_survive_notifications_and_allow_new_attempts() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let request = async || {
        let mut tasks =
            MaintenanceTask::from_write(&ctx, target("default", "t", 1), false, None).await?;
        Ok::<_, ErrorCode>(tasks.remove(0))
    };
    let mut task = request().await?;
    for attempt in 1..=MAX_TASK_FAILURES {
        let decision = task
            .runtime
            .apply_outcome(Err(ErrorCode::Internal("persistent index error")));
        if attempt < MAX_TASK_FAILURES {
            assert_eq!(decision, TaskDecision::Continue);
            let deadline = task.ready_at();
            task.merge(request().await?);
            assert_eq!(task.runtime.failures, attempt);
            assert!(task.ready_at() >= deadline);
        } else {
            assert_eq!(decision, TaskDecision::Stopped("retry_exhausted"));
        }
    }
    let task = request().await?;
    assert_eq!(
        task.runtime.failures, 0,
        "a later notification may open a fresh attempt"
    );
    Ok(())
}

#[test]
fn test_scanning_cannot_reset_a_failing_attempt_budget() {
    let mut runtime = TaskRuntime::new(Instant::now());
    runtime.failures = MAX_TASK_FAILURES - 1;
    assert_eq!(
        runtime.apply_outcome(Ok(TaskOutcome::ScanProgress)),
        TaskDecision::Continue
    );
    assert_eq!(
        runtime.apply_outcome(Err(ErrorCode::Internal("unrepairable range"))),
        TaskDecision::Stopped("retry_exhausted")
    );
    runtime.blocked_since = Some(Instant::now() - MAX_TASK_BLOCKED);
    assert_eq!(
        runtime.apply_outcome(Ok(TaskOutcome::ScanProgress)),
        TaskDecision::Stopped("no_progress")
    );
}

#[test]
fn test_no_progress_is_bounded_but_real_progress_resets_cooldown() {
    let mut runtime = TaskRuntime::new(Instant::now());
    runtime.blocked_since = Some(Instant::now() - MAX_TASK_BLOCKED);
    assert_eq!(
        runtime.apply_outcome(Ok(TaskOutcome::Deferred(Duration::from_secs(1)))),
        TaskDecision::Stopped("no_progress")
    );
    runtime.failures = MAX_TASK_FAILURES - 1;
    assert_eq!(
        runtime.apply_outcome(Ok(TaskOutcome::Progress(Duration::ZERO))),
        TaskDecision::Continue
    );
    assert_eq!(runtime.failures, 0);
    assert!(runtime.blocked_since.is_none());
    for (error, failures) in [
        (ErrorCode::NoResourcesAvailable("peer admission busy"), 0),
        (ErrorCode::MemoryExceedsLimit("execution OOM"), 1),
    ] {
        assert_eq!(
            runtime.apply_outcome(resource_outcome(Err(error))),
            TaskDecision::Continue
        );
        assert_eq!(runtime.failures, failures);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_refresh_replans_after_history_gc_and_definition_change() -> Result<()> {
    use databend_common_catalog::cache_policy::CachePolicy;
    use databend_common_storages_fuse::FuseTable;
    use databend_common_storages_fuse::io::FuseCaches;
    use databend_common_storages_fuse::io::SegmentInfoReader;
    use databend_storages_common_cache::Reader;

    let fixture = TestFixture::setup_with_stopped_maintenance().await?;
    let coverage = RefreshCoverage::create(1);
    fixture.execute_command("CREATE DATABASE hook_gc").await?;
    fixture
        .execute_command(
            "CREATE TABLE hook_gc.t(content STRING) ROW_PER_BLOCK=1 BLOCK_PER_SEGMENT=1000",
        )
        .await?;
    fixture
        .execute_command(
            "INSERT INTO hook_gc.t SELECT concat('alpha-', to_string(number)) FROM numbers(40)",
        )
        .await?;
    fixture
        .execute_command("CREATE ASYNC INVERTED INDEX inv ON hook_gc.t(content)")
        .await?;
    let assert_index_coverage = async |expected_blocks| -> Result<()> {
        let table = fixture
            .new_query_ctx()
            .await?
            .get_table("default", "hook_gc", "t")
            .await?;
        let fuse = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = fuse.read_table_snapshot().await?.unwrap();
        let index = &table.get_table_info().meta.indexes["inv"];
        let reader = SegmentInfoReader::new(fuse.get_operator(), table.schema());
        let mut block_count = 0;
        for location in &snapshot.segments {
            let segment = reader.read(location, &CachePolicy::BYPASS).await?;
            for block in segment.block_metas()? {
                let meta = block
                    .inverted_index_metas
                    .as_deref()
                    .and_then(|metas| metas.iter().find(|meta| meta.index_name == index.name))
                    .expect("every block must contain the current inverted index");
                assert_eq!(meta.index_version, index.version);
                block_count += 1;
            }
        }
        assert_eq!(block_count, expected_blocks);
        Ok(())
    };
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx.get_table("default", "hook_gc", "t").await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let source = fuse.snapshot_loc().unwrap();
    let refresh = async || {
        let mut tasks =
            MaintenanceTask::from_write(&ctx, target("hook_gc", "t", table.get_id()), false, None)
                .await?;
        Ok::<_, ErrorCode>(run_task(tasks.remove(0), &coverage).await)
    };
    let lock = ctx
        .clone()
        .acquire_table_lock_by_id("default", table.get_id(), &LockTableOption::LockWithRetry)
        .await?;
    assert!(lock.is_some());
    let execution = refresh();
    tokio::pin!(execution);
    tokio::select! {
        _ = &mut execution => panic!("Refresh finished while the foreground held the lock"),
        result = wait_for_publication_waiter(&ctx, table.get_id()) => result?,
    }
    let current = fixture
        .new_query_ctx()
        .await?
        .get_table("default", "hook_gc", "t")
        .await?;
    assert_eq!(
        FuseTable::try_from_table(current.as_ref())?.snapshot_loc(),
        Some(source.clone()),
        "Refresh must not publish while the foreground owns the table lock"
    );
    drop(lock);
    assert!(
        matches!(execution.await?, RunResult::Completed),
        "all 40 blocks must refresh in a single execution"
    );
    assert_index_coverage(40).await?;
    let current = fixture
        .new_query_ctx()
        .await?
        .get_table("default", "hook_gc", "t")
        .await?;
    let baseline = FuseTable::try_from_table(current.as_ref())?
        .snapshot_loc()
        .unwrap();
    assert_ne!(baseline, source);
    // Move the head before collecting the covered baseline.
    fixture
        .execute_command("INSERT INTO hook_gc.t VALUES ('alpha-extra')")
        .await?;
    fuse.get_operator().delete(&baseline).await?;
    FuseCaches::instance().snapshots.evict(&baseline);
    assert!(
        matches!(refresh().await?, RunResult::Completed),
        "missing baseline history must fall back to a full check"
    );
    assert_index_coverage(41).await?;
    fixture
        .execute_command("DROP INVERTED INDEX inv ON hook_gc.t")
        .await?;
    fixture
        .execute_command(
            "CREATE ASYNC INVERTED INDEX inv ON hook_gc.t(content) tokenizer='english'",
        )
        .await?;
    // A new definition invalidates the covered baseline even without new blocks.
    assert!(matches!(refresh().await?, RunResult::Completed));
    assert_index_coverage(41).await?;
    let blocks = fixture
        .execute_query("SELECT * FROM hook_gc.t WHERE match(content, 'alpha')")
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        blocks.iter().map(|block| block.num_rows()).sum::<usize>(),
        41
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_queued_refresh_follows_rename_but_not_same_name_recreation() -> Result<()> {
    let fixture = TestFixture::setup_with_stopped_maintenance().await?;
    let coverage = RefreshCoverage::create(1);
    fixture
        .execute_command("CREATE DATABASE hook_lifecycle")
        .await?;
    fixture
        .execute_command("CREATE TABLE hook_lifecycle.t(content STRING)")
        .await?;
    fixture
        .execute_command("INSERT INTO hook_lifecycle.t VALUES ('alpha')")
        .await?;
    fixture
        .execute_command("CREATE ASYNC INVERTED INDEX inv ON hook_lifecycle.t(content)")
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx.get_table("default", "hook_lifecycle", "t").await?;
    let request = MaintenanceTask::from_write(
        &ctx,
        target("hook_lifecycle", "t", table.get_id()),
        false,
        None,
    )
    .await?
    .remove(0);
    fixture
        .execute_command("ALTER TABLE hook_lifecycle.t RENAME TO renamed")
        .await?;
    // Even the write context that cached the old name must follow the table ID.
    let renamed = request.target.resolve(&ctx, "refresh").await?.unwrap();
    assert_eq!(renamed.database, "hook_lifecycle");
    assert_eq!(renamed.table, "renamed");
    assert!(matches!(
        run_task(request, &coverage).await,
        RunResult::Completed
    ));
    let blocks = fixture
        .execute_query("SELECT * FROM hook_lifecycle.renamed WHERE match(content, 'alpha')")
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        blocks.iter().map(|block| block.num_rows()).sum::<usize>(),
        1
    );

    let old = MaintenanceTask::from_write(&ctx, renamed, false, None)
        .await?
        .remove(0);
    fixture
        .execute_command("DROP TABLE hook_lifecycle.renamed")
        .await?;
    fixture
        .execute_command("CREATE TABLE hook_lifecycle.renamed(content STRING)")
        .await?;
    fixture
        .execute_command("INSERT INTO hook_lifecycle.renamed VALUES ('new-only')")
        .await?;
    fixture
        .execute_command("CREATE ASYNC INVERTED INDEX inv ON hook_lifecycle.renamed(content)")
        .await?;
    assert!(matches!(
        run_task(old, &coverage).await,
        RunResult::Completed
    ));
    let error = fixture
        .execute_command("SELECT * FROM hook_lifecycle.renamed WHERE match(content, 'new')")
        .await
        .expect_err("an old table's notification must not refresh its replacement");
    assert!(
        error
            .to_string()
            .contains("missing or uses an outdated format"),
        "{error}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_background_refresh_rejects_index_definition_drift() -> Result<()> {
    use databend_common_pipeline::core::Pipeline;
    use databend_common_storages_fuse::FuseTable;

    use crate::pipelines::executor::ExecutorSettings;
    use crate::pipelines::executor::PipelineCompleteExecutor;

    let fixture = TestFixture::setup_with_stopped_maintenance().await?;
    fixture
        .execute_command("CREATE TABLE drift(a STRING, b STRING, ASYNC INVERTED INDEX inv(a))")
        .await?;
    fixture
        .execute_command("INSERT INTO drift VALUES ('old', 'new')")
        .await?;
    let writer = fixture.new_query_ctx().await?;
    let identity = MaintenanceIdentity::capture(&writer).await?;
    let ctx = MaintenanceContext::from_identity(&identity).await?.ctx;
    assert!(ctx.is_automatic_maintenance());
    let table = ctx.get_table("default", "default", "drift").await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let before = fuse.snapshot_loc().unwrap();
    let mut pipeline = Pipeline::create();
    assert_eq!(
        fuse.do_refresh_indexes(ctx.clone(), &["inv".into()], None, &mut pipeline)
            .await?,
        1
    );
    fixture
        .execute_command("CREATE OR REPLACE ASYNC INVERTED INDEX inv ON drift(b)")
        .await?;
    let error = PipelineCompleteExecutor::try_create(pipeline, ExecutorSettings::try_create(ctx)?)?
        .execute()
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::TABLE_VERSION_MISMATCHED);
    let latest = writer.get_table("default", "default", "drift").await?;
    assert_eq!(
        FuseTable::try_from_table(latest.as_ref())?.snapshot_loc(),
        Some(before)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_zone_map_publication_respects_explicit_refresh_lock() -> Result<()> {
    use databend_common_sql::Planner;
    use databend_common_storages_fuse::FuseTable;
    use databend_common_storages_fuse::segment_zone_map::RefreshOutcome;

    use crate::interpreters::InterpreterFactory;

    let fixture = TestFixture::setup_with_stopped_maintenance().await?;
    fixture
        .execute_command("CREATE TABLE zone_map_lock(s STRING) BLOCK_PER_SEGMENT=1")
        .await?;
    fixture
        .execute_command("INSERT INTO zone_map_lock VALUES ('row')")
        .await?;
    fixture
        .execute_command("CREATE ASYNC INVERTED INDEX inv ON zone_map_lock(s)")
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx.get_table("default", "default", "zone_map_lock").await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let identity = MaintenanceIdentity::capture(&ctx).await?;
    let background = MaintenanceContext::from_identity(&identity).await?.ctx;
    background
        .get_settings()
        .set_setting("acquire_lock_timeout".into(), "0".into())?;

    // Keep the explicit Refresh pipeline (and its lock) alive without running it.
    let (plan, _) = Planner::new(ctx.clone())
        .plan_sql("REFRESH INVERTED INDEX inv ON zone_map_lock")
        .await?;
    let refresh = InterpreterFactory::get(ctx.clone(), &plan)
        .await?
        .execute2()
        .await?;
    let error = fuse
        .refresh_segment_zone_map(background.clone(), 0)
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::TABLE_ALREADY_LOCKED);
    let catalog = ctx.get_catalog("default").await?;
    let current = catalog
        .get_table(&ctx.get_tenant(), "default", "zone_map_lock")
        .await?;
    assert_eq!(current.get_table_info().ident, table.get_table_info().ident);
    drop(refresh);

    background
        .get_settings()
        .set_setting("acquire_lock_timeout".into(), "30".into())?;
    assert!(matches!(
        fuse.refresh_segment_zone_map(background, 0).await?,
        RefreshOutcome::Built { .. }
    ));
    let current = catalog
        .get_table(&ctx.get_tenant(), "default", "zone_map_lock")
        .await?;
    let snapshot = FuseTable::try_from_table(current.as_ref())?
        .read_table_snapshot()
        .await?
        .unwrap();
    assert!(snapshot.segment_zone_map.is_some());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_background_analyze_waits_for_table_lock() -> Result<()> {
    use databend_common_storages_fuse::FuseTable;

    let fixture = TestFixture::setup_with_stopped_maintenance().await?;
    fixture
        .execute_command("CREATE TABLE hook_analyze_lock(a INT) enable_auto_analyze=1")
        .await?;
    fixture
        .execute_command("INSERT INTO hook_analyze_lock VALUES (1), (2)")
        .await?;
    // Append statistics are already current; a delete makes ANALYZE necessary.
    fixture
        .execute_command("DELETE FROM hook_analyze_lock WHERE a = 1")
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx
        .get_table("default", "default", "hook_analyze_lock")
        .await?;
    let source = FuseTable::try_from_table(table.as_ref())?.snapshot_loc();
    let request = MaintenanceTask::from_write(
        &ctx,
        target("default", "hook_analyze_lock", table.get_id()),
        true,
        None,
    )
    .await?
    .pop()
    .unwrap();
    assert_eq!(request.kind(), TaskKind::Analyze);
    let lock = ctx
        .clone()
        .acquire_table_lock_by_id("default", table.get_id(), &LockTableOption::LockWithRetry)
        .await?;
    assert!(lock.is_some());
    let coverage = RefreshCoverage::create(1);
    let execution = run_task(request, &coverage);
    tokio::pin!(execution);
    tokio::select! {
        _ = &mut execution => panic!("Analyze finished while the foreground held the lock"),
        result = wait_for_publication_waiter(&ctx, table.get_id()) => result?,
    }
    let current = fixture
        .new_query_ctx()
        .await?
        .get_table("default", "default", "hook_analyze_lock")
        .await?;
    assert_eq!(
        FuseTable::try_from_table(current.as_ref())?.snapshot_loc(),
        source
    );
    drop(lock);
    assert!(matches!(execution.await, RunResult::Completed));
    let current = fixture
        .new_query_ctx()
        .await?
        .get_table("default", "default", "hook_analyze_lock")
        .await?;
    assert_ne!(
        FuseTable::try_from_table(current.as_ref())?.snapshot_loc(),
        source
    );
    Ok(())
}
