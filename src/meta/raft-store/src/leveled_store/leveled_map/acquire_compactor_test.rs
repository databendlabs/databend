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

use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::sm_v003::SMV003;

#[tokio::test]
async fn test_blocking_wait_timeout() -> anyhow::Result<()> {
    let lm = SMV003::default();

    let _c = lm.acquire_compactor().await;

    let (tx, rx) = oneshot::channel();

    let _ = timeout(std::time::Duration::from_secs(1), async {
        let _got = lm.acquire_compactor().await;
        let _ = tx.send(true);
    })
    .await;

    assert!(
        rx.await.is_err(),
        "can not get two compactor in blocking mode"
    );

    Ok(())
}

#[tokio::test]
async fn test_blocking_wait_ok() -> anyhow::Result<()> {
    let lm = SMV003::default();

    let _c = lm.acquire_compactor().await;

    let (tx, rx) = oneshot::channel();
    databend_common_base::runtime::spawn(async move {
        let _got = lm.acquire_compactor().await;
        let _ = tx.send(true);
    });

    drop(_c);

    assert!(
        rx.await.is_ok(),
        "got an compactor when the previous is dropped"
    );

    Ok(())
}
