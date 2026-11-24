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
use std::time::Instant;

use databend_common_meta_store::MetaStore;
use databend_meta::runtime_api::TokioRuntime;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::types::UpsertKV;
use databend_meta_client::types::protobuf::WatchRequest;
use futures::StreamExt;
use tokio::time::timeout;

/// While every attempt to acquire a semaphore fails as soon as it starts, `new_acquired_by_time`
/// waits 100 ms, 200 ms, 400 ms and so on before the next attempt.
#[tokio::test(flavor = "multi_thread")]
async fn test_new_acquired_by_time_waits_before_retrying_a_failed_attempt() -> anyhow::Result<()> {
    let store = MetaStore::new_local_testing::<TokioRuntime>().await;

    // An entry that does not decode ends the semaphore's watch as soon as the watch reads it, so
    // every attempt fails, as when no meta node serves the watch.
    store
        .upsert_kv(UpsertKV::update("sem/queue/0", b"not a permit entry"))
        .await?;

    let watch = WatchRequest::new("sem/queue/".to_string(), Some("sem/queue0".to_string()));
    let mut watch = store.watch(watch).await?;

    let start = Instant::now();
    let acquire = store.new_acquired_by_time("sem", 1, "id", Duration::from_secs(10), None);

    // Each attempt inserts its own permit entry, and deletes it when it fails.
    let four_attempts = async {
        let mut attempts = 0;
        while attempts < 4 {
            let resp = watch.next().await.expect("watch stream ended")?;
            if let Some((_key, None, Some(_inserted))) = resp.unpack() {
                attempts += 1;
            }
        }
        anyhow::Ok(())
    };

    tokio::select! {
        res = acquire => panic!("every attempt should fail: {res:?}"),
        res = timeout(Duration::from_secs(10), four_attempts) => res??,
    }

    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(700),
        "elapsed: {elapsed:?}"
    );
    Ok(())
}
