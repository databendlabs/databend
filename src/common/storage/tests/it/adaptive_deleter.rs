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
use std::time::Duration;

use databend_common_storage::AdaptiveConcurrencyConfig;
use databend_common_storage::AdaptiveDeleter;
use databend_common_storage::DatabendRetryInterceptor;
use databend_common_storage::FaultInjection;
use databend_common_storage::FaultInjectionLayer;
use databend_common_storage::FaultKind;
use databend_common_storage::FaultOp;
use databend_common_storage::FaultRule;
use databend_common_storage::RetryStats;
use opendal::ErrorKind;
use opendal::Operator;
use opendal::layers::RetryLayer;
use opendal::services::Memory;
use parking_lot::Mutex;

// The fault registry is global; run these tests one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn config() -> AdaptiveConcurrencyConfig {
    AdaptiveConcurrencyConfig {
        initial: 2,
        min: 1,
        max: 4,
        increase_after: 2,
        decrease_cooldown: Duration::ZERO,
    }
}

/// The layers of a Databend operator that matter here: faults are injected below the retry
/// layer, which reports the retries it absorbs to `stats`, as `init_operator_observed` does.
async fn storage(prefixes: &[(&str, usize)]) -> (Operator, Arc<RetryStats>) {
    let stats = Arc::new(RetryStats::default());
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(FaultInjectionLayer)
        .layer(
            RetryLayer::new()
                .with_min_delay(Duration::ZERO)
                .with_max_delay(Duration::ZERO)
                .with_max_times(3)
                .with_notify(DatabendRetryInterceptor::new(Some(stats.clone()))),
        )
        .finish();
    for (prefix, n) in prefixes {
        for i in 0..*n {
            op.write(&format!("{prefix}{i}"), "x").await.unwrap();
        }
    }
    (op, stats)
}

async fn count(op: &Operator, prefix: &str) -> usize {
    op.list_with(prefix).recursive(true).await.unwrap().len()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_files_only_deletes_selected_paths_and_shares_progress() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let (op, stats) = storage(&[("d/selected/", 5), ("d/other/", 3)]).await;
    let progress = Arc::new(Mutex::new(Vec::new()));
    let sink = progress.clone();
    let deleter = AdaptiveDeleter::new(op.clone(), stats, config())
        .unwrap()
        .with_progress(Arc::new(move |n| sink.lock().push(n)));

    // Deliberately not in listing order. No unselected path is removed.
    let report = deleter
        .remove_files(["d/selected/3", "d/selected/1"].map(String::from))
        .await;
    assert!(report.error.is_none());
    assert_eq!(
        (report.removed, report.attempted, report.batches),
        (2, 2, 2)
    );
    assert_eq!(report.retried, 0);
    assert!(!op.exists("d/selected/3").await.unwrap());
    assert!(!op.exists("d/selected/1").await.unwrap());
    assert_eq!(count(&op, "d/selected/").await, 3);
    assert_eq!(*progress.lock(), vec![1, 2]);

    // Both entry points, including cloned handles, report one shared running total.
    let other = deleter.clone().remove_all("d/other/").await.unwrap();
    assert_eq!(other.removed, 3);
    assert_eq!(*progress.lock(), vec![1, 2, 3, 4, 5]);
    let report = deleter.remove_files(Vec::new()).await;
    assert!(report.error.is_none());
    assert_eq!(
        (report.removed, report.attempted, report.batches),
        (0, 0, 0)
    );
    assert_eq!(*progress.lock(), vec![1, 2, 3, 4, 5]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_files_stops_at_failure_and_reports_a_confirmed_input_prefix() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let (op, stats) = storage(&[("e/files/", 4)]).await;
    let deleter = AdaptiveDeleter::new(op.clone(), stats, config()).unwrap();
    let paths = ["e/files/2", "e/files/0", "e/files/3"].map(String::from);
    let fault = FaultInjection::install(
        FaultRule::new(FaultOp::Delete, "e/files/0", FaultKind::Permanent).times(1),
    );

    let report = deleter.remove_files(paths.clone()).await;
    assert_eq!(report.error.unwrap().kind(), ErrorKind::Unexpected);
    assert_eq!(
        (report.removed, report.attempted, report.batches),
        (1, 2, 2)
    );
    assert!(!op.exists(&paths[0]).await.unwrap());
    assert!(op.exists(&paths[1]).await.unwrap());
    assert!(
        op.exists(&paths[2]).await.unwrap(),
        "later input was not attempted"
    );
    assert!(
        op.exists("e/files/1").await.unwrap(),
        "unselected path untouched"
    );
    fault.remove();

    let report = deleter.remove_files(paths.into_iter().skip(1)).await;
    assert!(report.error.is_none());
    assert_eq!((report.removed, report.attempted), (2, 2));
    assert_eq!(count(&op, "e/files/").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_files_observes_hidden_retries_once() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let (op, stats) = storage(&[("f/files/", 3)]).await;
    let deleter = AdaptiveDeleter::new(op.clone(), stats.clone(), config()).unwrap();
    let paths: Vec<_> = (0..3).map(|i| format!("f/files/{i}")).collect();
    let faults: Vec<_> = paths
        .iter()
        .map(|path| {
            FaultInjection::install(
                FaultRule::new(FaultOp::Delete, path, FaultKind::Temporary).times(1),
            )
        })
        .collect();

    let report = deleter.remove_files(paths).await;
    assert!(report.error.is_none());
    assert_eq!(
        (report.removed, report.attempted, report.batches),
        (3, 3, 3)
    );
    assert_eq!(report.retried, 3);
    assert_eq!(stats.retried(), 3);
    assert_eq!(stats.rate_limited(), 0);
    assert_eq!(deleter.concurrency(), 2, "sparse retries hold the bound");
    assert_eq!(count(&op, "f/files/").await, 0);
    for fault in faults {
        assert_eq!(fault.hits(), 1);
        fault.remove();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_files_and_remove_all_share_one_request_bound() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let gate = Arc::new(Gate::default());
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(GateLayer(gate.clone()))
        .finish();
    for i in 0..6 {
        op.write(&format!("g/all/{i}"), "x").await.unwrap();
        op.write(&format!("g/selected/{i}"), "x").await.unwrap();
    }
    let deleter = AdaptiveDeleter::new(op.clone(), Arc::default(), AdaptiveConcurrencyConfig {
        max: 2,
        ..config()
    })
    .unwrap();
    let paths = (0..6).map(|i| format!("g/selected/{i}"));
    let observe = async {
        gate.wait_for_blocked(2).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(gate.blocked(), 2);
        gate.open();
    };
    let (all, selected, ()) = futures::join!(
        deleter.remove_all("g/all/"),
        deleter.remove_files(paths),
        observe,
    );
    assert_eq!(all.unwrap().removed, 6);
    assert!(selected.error.is_none());
    assert_eq!(selected.removed, 6);
    assert_eq!(
        gate.peak(),
        2,
        "both entry points use the same request budget"
    );
    assert_eq!(count(&op, "g/").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cancelled_remove_files_releases_its_request_slot() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let gate = Arc::new(Gate::default());
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(GateLayer(gate.clone()))
        .finish();
    for i in 0..4 {
        op.write(&format!("h/files/{i}"), "x").await.unwrap();
    }
    let deleter = AdaptiveDeleter::new(op.clone(), Arc::default(), AdaptiveConcurrencyConfig {
        max: 2,
        ..config()
    })
    .unwrap();
    {
        let mut selected = Box::pin(deleter.remove_files(["h/files/0".to_owned()]));
        tokio::select! {
            _ = &mut selected => panic!("gate is closed"),
            _ = gate.wait_for_blocked(1) => {}
        }
    }
    assert_eq!(gate.blocked(), 0);
    let observe = async {
        gate.wait_for_blocked(2).await;
        gate.open();
    };
    let (report, ()) = futures::join!(deleter.remove_all("h/files/"), observe);
    assert_eq!(report.unwrap().removed, 4);
    assert_eq!(gate.peak(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_all_removes_only_the_prefix() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let (op, stats) = storage(&[("1/10/", 7), ("1/11/", 3)]).await;
    let deleter = AdaptiveDeleter::new(op.clone(), stats, config()).unwrap();

    let report = deleter.remove_all("1/10/").await.unwrap();

    assert_eq!(report.removed, 7);
    assert_eq!(report.retried, 0);
    assert_eq!(count(&op, "1/10/").await, 0);
    assert_eq!(count(&op, "1/11/").await, 3, "sibling prefix untouched");
    assert_eq!(deleter.remove_all("1/10/").await.unwrap().removed, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_all_fails_closed_and_keeps_the_rest_deletable() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let (op, stats) = storage(&[("2/20/", 6)]).await;
    let deleter = AdaptiveDeleter::new(op.clone(), stats, config()).unwrap();

    let fault = FaultInjection::install(
        FaultRule::new(FaultOp::Delete, "2/20/3", FaultKind::Permanent).times(1),
    );
    let err = deleter.remove_all("2/20/").await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert_eq!(fault.hits(), 1);
    assert!(
        count(&op, "2/20/").await >= 1,
        "the failed object must still be there"
    );
    fault.remove();

    // A later run finishes the job.
    deleter.remove_all("2/20/").await.unwrap();
    assert_eq!(count(&op, "2/20/").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_hidden_retries_reach_the_report_and_the_policy() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let (op, stats) = storage(&[("3/30/", 4)]).await;
    let deleter = AdaptiveDeleter::new(op.clone(), stats, config()).unwrap();
    let initial = deleter.concurrency();

    // Every object fails once with a temporary error, as OSS/COS/OBS/Azure throttling looks
    // to OpenDAL; the retry layer absorbs it and the delete succeeds.
    let faults: Vec<_> = (0..4)
        .map(|i| {
            FaultInjection::install(
                FaultRule::new(FaultOp::Delete, format!("3/30/{i}"), FaultKind::Temporary).times(1),
            )
        })
        .collect();
    let report = deleter.remove_all("3/30/").await.unwrap();
    for fault in faults {
        assert_eq!(fault.hits(), 1);
        fault.remove();
    }

    assert_eq!(report.removed, 4);
    assert_eq!(report.retried, 4, "one hidden retry per object");
    assert_eq!(count(&op, "3/30/").await, 0);
    // Memory deletes one path per batch, so each batch saw one retry: below the throttling
    // threshold max(initial=2, ⌈2/2⌉), it holds the bound instead of halving or raising it.
    assert_eq!(deleter.concurrency(), initial);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clean_prefixes_ramp_the_shared_bound_up() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let (op, stats) = storage(&[("4/40/", 3), ("4/41/", 3)]).await;
    let deleter = AdaptiveDeleter::new(op.clone(), stats, config()).unwrap();
    assert_eq!(deleter.concurrency(), 2);

    // Memory deletes one path per request, so each prefix is three clean batches; the bound
    // moves across prefixes because the policy is shared by the deleter, not per call.
    deleter.remove_all("4/40/").await.unwrap();
    deleter.remove_all("4/41/").await.unwrap();
    assert_eq!(
        deleter.concurrency(),
        4,
        "six clean batches at +1 per two: capped at max"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_exact_prefix_object_goes_through_the_observed_batch_path() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let (op, stats) = storage(&[("5/50/", 2)]).await;
    let deleter = AdaptiveDeleter::new(op.clone(), stats, config()).unwrap();
    // An object at exactly the prefix path, next to the objects under it.
    op.write("5/50", "x").await.unwrap();
    op.write("5/6", "x").await.unwrap();

    // The object at the prefix is deleted through a counted batch whose absorbed retry reaches
    // the report, like every other object; `Operator::remove_all` deletes it uncounted.
    let fault = FaultInjection::install(
        FaultRule::new(FaultOp::Delete, "5/50", FaultKind::Temporary).times(1),
    );
    let report = deleter.remove_all("5/50").await.unwrap();
    fault.remove();

    assert_eq!(report.removed, 3, "the prefix object and the two under it");
    assert_eq!(report.batches, 3);
    assert_eq!(report.retried, 1);
    assert_eq!(count(&op, "5/50").await, 0);
    assert!(op.exists("5/6").await.unwrap(), "sibling untouched");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_calls_share_one_bound() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let gate = Arc::new(Gate::default());
    let stats = Arc::new(RetryStats::default());
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(GateLayer(gate.clone()))
        .finish();
    for i in 0..6 {
        op.write(&format!("6/60/{i}"), "x").await.unwrap();
        op.write(&format!("6/61/{i}"), "x").await.unwrap();
    }
    let deleter = AdaptiveDeleter::new(op.clone(), stats, config()).unwrap();
    assert_eq!(deleter.concurrency(), 2);

    // Memory deletes one path per batch. Two calls could each admit two batches on their own;
    // the shared bound lets only two in flight in total.
    let gate_ = gate.clone();
    let observe = async move {
        gate_.wait_for_blocked(2).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            gate_.blocked(),
            2,
            "no more than the bound across both calls"
        );
        gate_.open();
    };
    let (a, b, ()) = futures::join!(
        deleter.remove_all("6/60/"),
        deleter.remove_all("6/61/"),
        observe
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(
        gate.peak(),
        2,
        "never exceeded the bound while it stayed at 2"
    );
    assert_eq!(count(&op, "6/").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fs_directories_are_removed_after_their_contents() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let root = tempfile::tempdir().unwrap();
    let stats = Arc::new(RetryStats::default());
    let op = Operator::new(opendal::services::Fs::default().root(root.path().to_str().unwrap()))
        .unwrap()
        .finish();
    for p in ["t/1/a", "t/1/b", "t/2/x/y", "t/c"] {
        op.write(p, "x").await.unwrap();
    }
    op.create_dir("t/empty/").await.unwrap();
    op.write("u/keep", "x").await.unwrap();
    let deleter = AdaptiveDeleter::new(op.clone(), stats, config()).unwrap();

    let report = deleter.remove_all("t/").await.unwrap();

    assert_eq!(report.removed, 4, "directory entries are not objects");
    assert_eq!(
        report.batches,
        4 + 5,
        "four files, then 2/x, 2, 1, empty and t itself"
    );
    assert!(
        !root.path().join("t").exists(),
        "the prefix directory is gone"
    );
    assert!(root.path().join("u/keep").exists());
    assert_eq!(deleter.remove_all("t/").await.unwrap().removed, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fs_remove_all_preserves_the_configured_root() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    for prefix in ["", "/", "///", " \t/// \t"] {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("storage");
        let op = Operator::new(opendal::services::Fs::default().root(root.to_str().unwrap()))
            .unwrap()
            .finish();
        for path in ["a", "nested/b", "nested/deep/c", "other/d"] {
            op.write(path, "x").await.unwrap();
        }
        op.create_dir("empty/sub/").await.unwrap();
        let deleter = AdaptiveDeleter::new(op.clone(), Arc::default(), config()).unwrap();

        let report = deleter.remove_all(prefix).await.unwrap();
        assert_eq!(report.removed, 4);
        assert!(
            root.is_dir(),
            "{prefix:?} must preserve the configured root"
        );
        assert!(std::fs::read_dir(&root).unwrap().next().is_none());

        let report = deleter.remove_all(prefix).await.unwrap();
        assert_eq!((report.removed, report.batches), (0, 0));
        assert!(root.is_dir(), "empty-root cleanup must be a no-op");
        op.write("after/cleanup", "still usable").await.unwrap();
        assert_eq!(
            op.read("after/cleanup").await.unwrap().to_vec(),
            b"still usable"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cancelled_call_gives_its_slots_back() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let gate = Arc::new(Gate::default());
    let stats = Arc::new(RetryStats::default());
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(GateLayer(gate.clone()))
        .finish();
    for i in 0..4 {
        op.write(&format!("7/70/{i}"), "x").await.unwrap();
        op.write(&format!("7/71/{i}"), "x").await.unwrap();
    }
    let deleter = AdaptiveDeleter::new(op.clone(), stats, config()).unwrap();

    // Both slots are held by batches of a call that is then dropped mid-flight.
    {
        let d = deleter.clone();
        let mut call = Box::pin(async move { d.remove_all("7/70/").await });
        let gate_ = gate.clone();
        tokio::select! {
            _ = &mut call => panic!("gate is closed"),
            _ = gate_.wait_for_blocked(2) => {}
        }
        assert_eq!(gate.blocked(), 2);
    }
    assert_eq!(
        gate.blocked(),
        0,
        "dropped batches abandoned their requests"
    );

    // The permits came back with them: another call runs at full concurrency, not zero.
    gate.close();
    let gate_ = gate.clone();
    let observe = async move {
        gate_.wait_for_blocked(2).await;
        gate_.open();
    };
    let (report, ()) = futures::join!(deleter.remove_all("7/71/"), observe);
    report.unwrap();
    assert_eq!(count(&op, "7/71/").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_no_batch_is_admitted_after_a_failure() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    // bound 2, six single-path batches: at least one is still waiting for a slot when the
    // first failure is settled, and must not be started.
    let (op, stats) = storage(&[("8/80/", 6)]).await;
    let deleter = AdaptiveDeleter::new(op.clone(), stats, config()).unwrap();

    let fault = FaultInjection::install(
        FaultRule::new(FaultOp::Delete, "8/80/0", FaultKind::Permanent).times(1),
    );
    let err = deleter.remove_all("8/80/").await.unwrap_err();
    fault.remove();
    assert_eq!(err.kind(), ErrorKind::Unexpected);

    let left = count(&op, "8/80/").await;
    assert!(
        left >= 6 - 2,
        "at most the batches in flight with the failed one completed, {left} objects left"
    );

    deleter.remove_all("8/80/").await.unwrap();
    assert_eq!(count(&op, "8/80/").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ready_failure_is_settled_before_a_freed_slot_is_reused() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let gate = Arc::new(Gate::default());
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(GateLayer(gate.clone()))
        .finish();
    for i in 0..6 {
        op.write(&format!("ready/{i}"), "x").await.unwrap();
    }
    let deleter = AdaptiveDeleter::new(op.clone(), Arc::default(), AdaptiveConcurrencyConfig {
        max: 2,
        increase_after: 100,
        ..config()
    })
    .unwrap();
    gate.throttle("ready/1");
    let release = async {
        gate.wait_for_blocked(2).await;
        // Both outcomes become ready together. The success at ready/0 returns a slot, but
        // the queued failure at ready/1 must be settled before that slot starts ready/2.
        gate.open();
    };
    let (result, ()) = futures::join!(deleter.remove_all("ready/"), release);
    assert_eq!(result.unwrap_err().kind(), ErrorKind::RateLimited);
    assert!(!op.exists("ready/0").await.unwrap());
    for i in 1..6 {
        assert!(
            op.exists(&format!("ready/{i}")).await.unwrap(),
            "ready/{i} must not be deleted after the queued failure"
        );
    }
    assert_eq!(gate.peak(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_selected_file_waiters_share_the_lowered_bound() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let gate = Arc::new(Gate::default());
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(GateLayer(gate.clone()))
        .finish();
    for i in 0..17 {
        op.write(&format!("admission/{i}"), "x").await.unwrap();
    }
    for i in 0..8 {
        op.write(&format!("admission-prefix/{i}"), "x")
            .await
            .unwrap();
    }
    let deleter = AdaptiveDeleter::new(op.clone(), Arc::default(), AdaptiveConcurrencyConfig {
        initial: 8,
        max: 8,
        increase_after: 100,
        decrease_cooldown: Duration::from_secs(60),
        ..config()
    })
    .unwrap();
    gate.throttle("admission/0");
    gate.open();
    let report = deleter.remove_files(["admission/0".to_owned()]).await;
    assert_eq!(report.error.unwrap().kind(), ErrorKind::RateLimited);
    assert_eq!(deleter.concurrency(), 4);
    gate.close();

    // Many independent clones compete after an unsaturated 8 -> 4 decrease. They cannot
    // reuse idle capacity from the old bound, even when both entry points are active.
    let calls = futures::future::join_all((1..17).map(|i| {
        let deleter = deleter.clone();
        databend_common_base::runtime::spawn(async move {
            deleter.remove_files([format!("admission/{i}")]).await
        })
    }));
    let release = async {
        gate.wait_for_blocked(4).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(gate.blocked(), 4);
        gate.open();
    };
    let (reports, prefix, ()) =
        futures::join!(calls, deleter.remove_all("admission-prefix/"), release);
    assert_eq!(prefix.unwrap().removed, 8);
    for report in reports {
        let report = report.unwrap();
        assert!(report.error.is_none());
        assert_eq!(report.removed, 1);
    }
    assert_eq!(gate.peak(), 4);
    assert!(op.exists("admission/0").await.unwrap());
    assert_eq!(count(&op, "admission/").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_halving_under_queued_waiters_shrinks_the_batches_in_flight() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let gate = Arc::new(Gate::default());
    let stats = Arc::new(RetryStats::default());
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(GateLayer(gate.clone()))
        .finish();
    for i in 0..8 {
        op.write(&format!("9/90/{i}"), "x").await.unwrap();
        op.write(&format!("9/91/{i}"), "x").await.unwrap();
    }
    // Bound 4 so one halving leaves room to see the difference; the cooldown spans the test
    // so the later throttled batches do not halve it again.
    let deleter = AdaptiveDeleter::new(op.clone(), stats, AdaptiveConcurrencyConfig {
        initial: 4,
        min: 1,
        max: 4,
        increase_after: 100,
        decrease_cooldown: Duration::from_secs(60),
    })
    .unwrap();

    // Call A alone saturates the four slots and has waiters queued behind them. Every delete
    // of A's first batches is throttled, so whichever one is let through first halves the
    // bound and A stops admitting; call B then has the queue to itself.
    for i in 0..8 {
        gate.throttle(&format!("9/90/{i}"));
    }
    let a = deleter.remove_all("9/90/");
    let b = async {
        gate.wait_for_blocked(4).await;
        deleter.remove_all("9/91/").await
    };
    let steps = async {
        gate.wait_for_blocked(4).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(gate.blocked(), 4, "B is queued behind A's four batches");

        // The outcome halves the bound to 2. The returned slot cannot admit B while three
        // of A's requests are still in flight above the new bound.
        gate.release_one();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(deleter.concurrency(), 2);
        assert_eq!(gate.blocked(), 3, "no admission above the new bound");

        // Draining one more of A's reaches the new bound; from then on B refills to that
        // bound and the in-flight count never climbs back to the old one.
        gate.release_one();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(gate.blocked(), 2);
        gate.release_one();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            gate.blocked(),
            2,
            "B took the freed slot: steady at the new bound"
        );
        assert!(gate.peak() <= 4);

        gate.open();
    };
    let (a, b, ()) = futures::join!(a, b, steps);
    assert_eq!(a.unwrap_err().kind(), ErrorKind::RateLimited);
    b.unwrap();
    assert_eq!(count(&op, "9/91/").await, 0);
    assert!(count(&op, "9/90/").await > 0, "A failed closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cancellation_after_a_decrease_preserves_the_lower_bound() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let gate = Arc::new(Gate::default());
    let stats = Arc::new(RetryStats::default());
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(GateLayer(gate.clone()))
        .finish();
    for i in 0..8 {
        op.write(&format!("a/a0/{i}"), "x").await.unwrap();
        op.write(&format!("a/a1/{i}"), "x").await.unwrap();
    }
    let deleter = AdaptiveDeleter::new(op.clone(), stats, AdaptiveConcurrencyConfig {
        initial: 4,
        min: 1,
        max: 4,
        increase_after: 100,
        decrease_cooldown: Duration::from_secs(60),
    })
    .unwrap();

    // A call saturates the four slots; one throttled outcome halves the bound to 2 while
    // the other three batches still hold their slots. Then the call is cancelled.
    gate.throttle("a/a0/0");
    gate.throttle("a/a0/1");
    gate.throttle("a/a0/2");
    gate.throttle("a/a0/3");
    {
        let mut call = Box::pin(deleter.remove_all("a/a0/"));
        let steps = async {
            gate.wait_for_blocked(4).await;
            gate.release_one();
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(deleter.concurrency(), 2);
            assert_eq!(gate.blocked(), 3);
        };
        tokio::select! {
            _ = &mut call => panic!("gate is closed"),
            _ = steps => {}
        }
        // `call` and its three in-flight batches are dropped here.
    }
    assert_eq!(
        gate.blocked(),
        0,
        "dropped batches abandoned their requests"
    );

    // Cancellation must not restore the old capacity: a fresh call gets two in flight,
    // not the three that were still active when the previous call was dropped.
    let observe = async {
        gate.wait_for_blocked(2).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(gate.blocked(), 2, "only the halved bound is available");
        gate.open();
    };
    let (report, ()) = futures::join!(deleter.remove_all("a/a1/"), observe);
    report.unwrap();
    assert_eq!(count(&op, "a/a1/").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_decrease_with_idle_capacity_applies_to_the_next_call() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let gate = Arc::new(Gate::default());
    let stats = Arc::new(RetryStats::default());
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(GateLayer(gate.clone()))
        .finish();
    op.write("b/b0/0", "x").await.unwrap();
    for i in 0..8 {
        op.write(&format!("b/b1/{i}"), "x").await.unwrap();
    }
    let deleter = AdaptiveDeleter::new(op.clone(), stats, AdaptiveConcurrencyConfig {
        initial: 4,
        min: 1,
        max: 4,
        increase_after: 100,
        decrease_cooldown: Duration::from_secs(60),
    })
    .unwrap();

    // A single throttled batch, with the other three slots idle, halves the bound to 2.
    gate.throttle("b/b0/0");
    gate.open();
    assert_eq!(
        deleter.remove_all("b/b0/").await.unwrap_err().kind(),
        ErrorKind::RateLimited
    );
    assert_eq!(deleter.concurrency(), 2);

    // Idle capacity must follow the new bound: the next call gets two in flight, not the
    // three previously idle slots plus the one just released.
    gate.close();
    let observe = async {
        gate.wait_for_blocked(2).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(gate.blocked(), 2, "only the halved bound is available");
        gate.open();
    };
    let (report, ()) = futures::join!(deleter.remove_all("b/b1/"), observe);
    report.unwrap();
    assert_eq!(gate.peak(), 2);
    assert_eq!(count(&op, "b/b1/").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_growth_after_a_decrease_follows_the_current_bound() {
    let _serial = SERIAL.lock().await;
    FaultInjection::clear();
    let gate = Arc::new(Gate::default());
    let stats = Arc::new(RetryStats::default());
    let op = Operator::new(Memory::default())
        .unwrap()
        .layer(GateLayer(gate.clone()))
        .finish();
    for i in 0..8 {
        op.write(&format!("c/c0/{i}"), "x").await.unwrap();
        op.write(&format!("c/c1/{i}"), "x").await.unwrap();
    }
    // Every clean batch raises the bound by one, including while old work is still draining.
    let deleter = AdaptiveDeleter::new(op.clone(), stats, AdaptiveConcurrencyConfig {
        initial: 4,
        min: 1,
        max: 4,
        increase_after: 1,
        decrease_cooldown: Duration::from_secs(60),
    })
    .unwrap();

    // Four in flight. One throttled outcome halves the bound to 2 with three old requests
    // still active. Later clean completions raise the bound one at a time; admission must
    // follow that bound rather than adding capacity on top of returned slots.
    gate.throttle("c/c0/0");
    gate.throttle("c/c0/1");
    gate.throttle("c/c0/2");
    gate.throttle("c/c0/3");
    let a = deleter.remove_all("c/c0/");
    let b = async {
        gate.wait_for_blocked(4).await;
        deleter.remove_all("c/c1/").await
    };
    let steps = async {
        gate.wait_for_blocked(4).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        gate.release_one();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(deleter.concurrency(), 2);
        assert_eq!(gate.blocked(), 3);

        // A's remaining three are all throttled too, but the cooldown holds the bound; their
        // completions release slots B takes. From here on B's clean completions grow the
        // bound 2 → 3 → 4, and the in-flight count must follow the bound, never exceed it.
        for _ in 0..12 {
            gate.release_one();
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert!(
                gate.blocked() <= deleter.concurrency(),
                "in flight {} above bound {}",
                gate.blocked(),
                deleter.concurrency()
            );
        }
        gate.open();
    };
    let (a, b, ()) = futures::join!(a, b, steps);
    assert_eq!(a.unwrap_err().kind(), ErrorKind::RateLimited);
    b.unwrap();
    assert_eq!(count(&op, "c/c1/").await, 0);
}

/// Blocks every delete until opened, counting how many are blocked at once.
struct Gate {
    blocked: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    open: tokio::sync::Notify,
    opened: std::sync::atomic::AtomicBool,
    /// Each permit lets exactly one blocked delete through while the gate is closed.
    step: tokio::sync::Semaphore,
    /// Paths whose delete fails with `RateLimited` once let through.
    throttle: Mutex<std::collections::HashSet<String>>,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            blocked: Default::default(),
            peak: Default::default(),
            open: Default::default(),
            opened: Default::default(),
            step: tokio::sync::Semaphore::new(0),
            throttle: Default::default(),
        }
    }
}

impl Gate {
    fn blocked(&self) -> usize {
        self.blocked.load(std::sync::atomic::Ordering::Acquire)
    }
    fn peak(&self) -> usize {
        self.peak.load(std::sync::atomic::Ordering::Acquire)
    }
    fn open(&self) {
        self.opened
            .store(true, std::sync::atomic::Ordering::Release);
        self.open.notify_waiters();
    }
    fn close(&self) {
        self.opened
            .store(false, std::sync::atomic::Ordering::Release);
    }
    /// Let one blocked delete through.
    fn release_one(&self) {
        self.step.add_permits(1);
    }
    fn throttle(&self, path: &str) {
        self.throttle.lock().insert(path.to_owned());
    }
    fn is_throttled(&self, path: &str) -> bool {
        self.throttle.lock().remove(path)
    }
    async fn wait_for_blocked(&self, n: usize) {
        while self.blocked() < n {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    async fn pass(&self) {
        // Counted while blocked, including when the request is dropped mid-wait.
        struct Blocked<'a>(&'a Gate);
        impl Drop for Blocked<'_> {
            fn drop(&mut self) {
                self.0
                    .blocked
                    .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            }
        }
        let now = self
            .blocked
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        self.peak
            .fetch_max(now, std::sync::atomic::Ordering::AcqRel);
        let _blocked = Blocked(self);
        while !self.opened.load(std::sync::atomic::Ordering::Acquire) {
            let notified = self.open.notified();
            if self.opened.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            tokio::select! {
                _ = notified => {}
                permit = self.step.acquire() => {
                    permit.unwrap().forget();
                    break;
                }
            }
        }
    }
}

struct GateLayer(Arc<Gate>);

impl<A: opendal::raw::Access> opendal::raw::Layer<A> for GateLayer {
    type LayeredAccess = GateAccessor<A>;
    fn layer(&self, inner: A) -> Self::LayeredAccess {
        GateAccessor {
            inner,
            gate: self.0.clone(),
        }
    }
}

struct GateAccessor<A> {
    inner: A,
    gate: Arc<Gate>,
}

impl<A> std::fmt::Debug for GateAccessor<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GateAccessor").finish()
    }
}

impl<A: opendal::raw::Access> opendal::raw::LayeredAccess for GateAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = GateDeleter<A::Deleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }
    async fn read(
        &self,
        path: &str,
        args: opendal::raw::OpRead,
    ) -> opendal::Result<(opendal::raw::RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }
    async fn write(
        &self,
        path: &str,
        args: opendal::raw::OpWrite,
    ) -> opendal::Result<(opendal::raw::RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }
    async fn list(
        &self,
        path: &str,
        args: opendal::raw::OpList,
    ) -> opendal::Result<(opendal::raw::RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }
    async fn delete(&self) -> opendal::Result<(opendal::raw::RpDelete, Self::Deleter)> {
        let (rp, inner) = self.inner.delete().await?;
        Ok((rp, GateDeleter {
            inner,
            gate: self.gate.clone(),
            paths: Vec::new(),
        }))
    }
}

struct GateDeleter<D> {
    inner: D,
    gate: Arc<Gate>,
    paths: Vec<String>,
}

impl<D: opendal::raw::oio::Delete> opendal::raw::oio::Delete for GateDeleter<D> {
    fn delete(&mut self, path: &str, args: opendal::raw::OpDelete) -> opendal::Result<()> {
        self.paths.push(path.to_owned());
        self.inner.delete(path, args)
    }
    async fn flush(&mut self) -> opendal::Result<usize> {
        self.gate.pass().await;
        if self.paths.iter().any(|p| self.gate.is_throttled(p)) {
            return Err(opendal::Error::new(
                ErrorKind::RateLimited,
                "gate throttled",
            ));
        }
        self.inner.flush().await
    }
}
