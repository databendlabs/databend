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
