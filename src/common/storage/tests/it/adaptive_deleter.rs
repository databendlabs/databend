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

        // The outcome halves the bound to 2. The finished batch's permit must be retired by
        // that halving rather than handed to B, so three remain in flight, not four.
        gate.release_one();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(deleter.concurrency(), 2);
        assert_eq!(gate.blocked(), 3, "retired, not handed to a waiter");

        // Draining one more of A's retires the deficit; from then on B refills to the new
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
async fn test_cancelled_slots_are_retired_against_the_deficit() {
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

    // The dropped slots must have been retired down to the new bound, not handed back
    // in full: a fresh call gets two in flight, not three.
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
async fn test_halving_while_unsaturated_retires_idle_permits_at_once() {
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

    // The idle permits must have been retired with the bound: the next call gets two in
    // flight, not the three that were sitting free plus the one just released.
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

/// Blocks every delete until opened, counting how many are blocked at once.
struct Gate {
    blocked: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    open: tokio::sync::Notify,
    opened: std::sync::atomic::AtomicBool,
    /// Each permit lets exactly one blocked delete through while the gate is closed.
    step: tokio::sync::Semaphore,
    /// Paths whose delete fails with `RateLimited` once let through.
    throttle: std::sync::Mutex<std::collections::HashSet<String>>,
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
        self.throttle.lock().unwrap().insert(path.to_owned());
    }
    fn is_throttled(&self, path: &str) -> bool {
        self.throttle.lock().unwrap().remove(path)
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
