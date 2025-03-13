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

use std::collections::BTreeSet;
use std::future::Future;
use std::io;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use databend_common_meta_watcher::dispatch::Dispatcher;
use databend_common_meta_watcher::dispatch::DispatcherHandle;
use databend_common_meta_watcher::event_filter::EventFilter;
use databend_common_meta_watcher::type_config::KVChange;
use databend_common_meta_watcher::type_config::TypeConfig;
use databend_common_meta_watcher::watch_stream::WatchStreamSender;
use databend_common_meta_watcher::KeyRange;
use databend_common_meta_watcher::WatchResult;
use tokio::sync::mpsc;
use tokio::time::timeout;

// Test constants
const REMOVAL_DELAY: Duration = Duration::from_millis(300);
const RECEIVE_TIMEOUT: Duration = Duration::from_millis(1000);
const CHECK_NEGATIVE_TIMEOUT: Duration = Duration::from_millis(100);

// Only Debug is actually needed for the test framework
#[derive(Debug, Copy, Clone)]
struct Types {}

impl TypeConfig for Types {
    type Key = String;
    type Value = String;
    type Response = (String, Option<String>, Option<String>);
    type Error = io::Error;

    fn new_response(change: KVChange<Self>) -> Self::Response {
        change
    }

    fn data_error(error: io::Error) -> Self::Error {
        error
    }

    fn update_watcher_metrics(_delta: i64) {}

    #[allow(clippy::disallowed_methods)]
    fn spawn<T>(fut: T)
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        tokio::spawn(fut);
    }
}

async fn all_senders(handle: &DispatcherHandle<Types>) -> BTreeSet<Arc<WatchStreamSender<Types>>> {
    handle
        .request_blocking(move |dispatcher| {
            dispatcher
                .watch_senders()
                .into_iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        })
        .await
        .unwrap_or_default()
}

// Helper to verify event reception
async fn expect_event(
    rx: &mut mpsc::Receiver<WatchResult<Types>>,
    expected_event: KVChange<Types>,
    message: &str,
) {
    let result = timeout(RECEIVE_TIMEOUT, rx.recv()).await;
    assert!(result.is_ok(), "{}", message);
    assert_eq!(result.unwrap().unwrap().unwrap(), expected_event);
}

// Helper to verify no event is received
async fn expect_no_event(rx: &mut mpsc::Receiver<WatchResult<Types>>, message: &str) {
    let result = timeout(CHECK_NEGATIVE_TIMEOUT, rx.recv()).await;
    assert!(result.is_err(), "{}", message);
}

#[tokio::test]
async fn test_dispatcher_simple_watch() {
    let handle = Dispatcher::<Types>::spawn();

    let (tx, mut rx) = mpsc::channel(10);
    let _watcher = handle
        .add_watcher(rng("a", "d"), EventFilter::all(), tx)
        .await
        .unwrap();

    handle.send_change(kv_update("b", "old_value", "new_value"));

    expect_event(
        &mut rx,
        kv_update("b", "old_value", "new_value"),
        "should receive event",
    )
    .await;
}

#[tokio::test]
async fn test_dispatcher_multiple_events() {
    let handle = Dispatcher::<Types>::spawn();

    let (tx, mut rx) = mpsc::channel(10);
    let _watcher = handle
        .add_watcher(rng("a", "z"), EventFilter::all(), tx)
        .await
        .unwrap();

    let events = [
        kv_update("b", "old_b", "new_b"),
        kv_update("c", "old_c", "new_c"),
        kv_delete("d", "old_d"),
    ];

    for (i, event) in events.iter().enumerate() {
        handle.send_change(event.clone());
        expect_event(
            &mut rx,
            event.clone(),
            &format!("should receive event {}", i + 1),
        )
        .await;
    }
}

#[tokio::test]
async fn test_dispatcher_overlapping_ranges() {
    let handle = Dispatcher::<Types>::spawn();

    let (tx1, mut rx1) = mpsc::channel(10);
    let watcher1 = handle
        .add_watcher(rng("a", "c"), EventFilter::all(), tx1)
        .await
        .unwrap();

    let (tx2, mut rx2) = mpsc::channel(10);
    let watcher2 = handle
        .add_watcher(rng("c", "e"), EventFilter::all(), tx2)
        .await
        .unwrap();

    // Test key in first watcher range
    let event_b = kv_update("b", "old_b", "new_b");
    handle.send_change(event_b.clone());

    expect_event(&mut rx1, event_b, "w1 should receive 'b'").await;
    expect_no_event(&mut rx2, "w2 should not receive 'b'").await;

    // Test key in second watcher range
    handle.send_change(kv_update("c", "old_c", "new_c"));

    expect_no_event(&mut rx1, "w1 should not receive 'c'").await;
    expect_event(
        &mut rx2,
        kv_update("c", "old_c", "new_c"),
        "w2 should receive 'c'",
    )
    .await;

    let watcher1_clone = watcher1.clone();
    handle.remove_watcher(watcher1_clone).await.unwrap();

    let watcher2_clone = watcher2.clone();
    handle.remove_watcher(watcher2_clone).await.unwrap();
}

#[tokio::test]
async fn test_dispatcher_basic_functionality() {
    let handle = Dispatcher::<Types>::spawn();

    // Set up watchers
    let (tx1, mut rx1) = mpsc::channel(10);
    let watcher1 = handle
        .add_watcher(rng("a", "c"), EventFilter::all(), tx1)
        .await
        .unwrap();

    let (tx2, mut rx2) = mpsc::channel(10);
    let watcher2 = handle
        .add_watcher(rng("c", "e"), EventFilter::update(), tx2)
        .await
        .unwrap();

    // First watcher event
    handle.send_change(kv_update("b", "old_value", "new_value"));

    expect_event(
        &mut rx1,
        kv_update("b", "old_value", "new_value"),
        "w1 should receive event",
    )
    .await;
    expect_no_event(&mut rx2, "w2 should not receive (out of range)").await;

    // Second watcher event
    handle.send_change(kv_update("c", "old_value", "new_value"));

    expect_event(
        &mut rx2,
        kv_update("c", "old_value", "new_value"),
        "w2 should receive event",
    )
    .await;
    expect_no_event(&mut rx1, "w1 should not receive (out of range)").await;

    // Delete event
    handle.send_change(kv_delete("d", "old_value"));

    expect_no_event(&mut rx1, "w1 should not receive (out of range)").await;
    expect_no_event(&mut rx2, "w2 should not receive (filter excludes DELETE)").await;

    // Remove first watcher
    let watcher1_clone = watcher1.clone();
    handle.remove_watcher(watcher1_clone).await.unwrap();

    tokio::time::sleep(REMOVAL_DELAY).await;
    drop(rx1);

    handle.send_change(kv_update("b", "old_value", "newer_value"));
    tokio::time::sleep(REMOVAL_DELAY).await;

    // Verify watcher status
    let senders = all_senders(&handle).await;
    assert_eq!(senders.len(), 1);
    assert!(!senders.contains(&watcher1), "w1 should be removed");
    assert!(senders.contains(&watcher2), "w2 should still exist");

    let watcher2_clone = watcher2.clone();
    handle.remove_watcher(watcher2_clone).await.unwrap();
}

#[tokio::test]
async fn test_dispatcher_watch_senders() {
    let handle = Dispatcher::<Types>::spawn();

    // Create three watchers with different ranges
    let watchers = [
        handle
            .add_watcher(rng("a", "c"), EventFilter::update(), mpsc::channel(10).0)
            .await
            .unwrap(),
        handle
            .add_watcher(rng("c", "e"), EventFilter::update(), mpsc::channel(10).0)
            .await
            .unwrap(),
        handle
            .add_watcher(rng("e", "g"), EventFilter::update(), mpsc::channel(10).0)
            .await
            .unwrap(),
    ];

    // Verify senders
    let senders = all_senders(&handle).await;
    assert_eq!(senders.len(), 3);
    for watcher in &watchers {
        assert!(senders.contains(watcher));
    }

    // Clean up
    for watcher in watchers.clone() {
        let watcher_clone = watcher.clone();
        handle.remove_watcher(watcher_clone).await.unwrap();
    }

    let senders = all_senders(&handle).await;
    assert_eq!(senders.len(), 0);
}

#[tokio::test]
async fn test_dispatcher_closed_channel() {
    let handle = Dispatcher::<Types>::spawn();

    // Create and immediately close a channel
    let (tx, rx) = mpsc::channel(10);
    let _watcher = handle
        .add_watcher(rng("a", "c"), EventFilter::update(), tx)
        .await
        .unwrap();
    drop(rx);

    // Verify automatic cleanup
    handle.send_change(kv_update("b", "old_value", "new_value"));
    tokio::time::sleep(REMOVAL_DELAY).await;
    let senders = all_senders(&handle).await;
    assert_eq!(senders.len(), 0);
}

fn s(x: &str) -> String {
    x.to_string()
}

fn rng(start: &str, end: &str) -> KeyRange<Types> {
    (Bound::Included(s(start)), Bound::Excluded(s(end)))
}

fn kv_update(key: &str, before: &str, after: &str) -> KVChange<Types> {
    (s(key), Some(s(before)), Some(s(after)))
}

fn kv_delete(key: &str, before: &str) -> KVChange<Types> {
    (s(key), Some(s(before)), None)
}
