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
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_query::sessions::QueueData;
use databend_query::sessions::QueueManager;

#[derive(Debug)]
struct TestData<const PASSED: bool = false>(String);

impl<const PASSED: bool> QueueData for TestData<PASSED> {
    type Key = String;

    fn get_key(&self) -> Self::Key {
        self.0.clone()
    }

    fn remove_error_message(key: Option<Self::Key>) -> ErrorCode {
        ErrorCode::Internal(format!("{:?}", key))
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(1000)
    }

    fn need_acquire_to_queue(&self) -> bool {
        !PASSED
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_passed_acquire() -> Result<()> {
    let test_count = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 5) as usize
        + 5;

    let barrier = Arc::new(tokio::sync::Barrier::new(test_count));
    let queue = QueueManager::<TestData<true>>::create(1);
    let mut join_handles = Vec::with_capacity(test_count);

    let instant = Instant::now();
    for index in 0..test_count {
        join_handles.push({
            let queue = queue.clone();
            let barrier = barrier.clone();
            databend_common_base::runtime::spawn(async move {
                barrier.wait().await;
                let _guard = queue
                    .acquire(TestData::<true>(format!("TestData{}", index)))
                    .await?;
                tokio::time::sleep(Duration::from_secs(1)).await;
                Result::<(), ErrorCode>::Ok(())
            })
        })
    }

    for join_handle in join_handles {
        let _ = join_handle.await;
    }

    assert!(instant.elapsed() < Duration::from_secs(test_count as u64));
    assert_eq!(queue.length(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_serial_acquire() -> Result<()> {
    let test_count = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 5) as usize
        + 5;

    let barrier = Arc::new(tokio::sync::Barrier::new(test_count));
    let queue = QueueManager::<TestData>::create(1);
    let mut join_handles = Vec::with_capacity(test_count);

    let instant = Instant::now();
    for index in 0..test_count {
        join_handles.push({
            let queue = queue.clone();
            let barrier = barrier.clone();
            databend_common_base::runtime::spawn(async move {
                barrier.wait().await;
                let _guard = queue
                    .acquire(TestData(format!("TestData{}", index)))
                    .await?;
                tokio::time::sleep(Duration::from_secs(1)).await;
                Result::<(), ErrorCode>::Ok(())
            })
        })
    }

    for join_handle in join_handles {
        let _ = join_handle.await;
    }

    assert!(instant.elapsed() >= Duration::from_secs(test_count as u64));
    assert_eq!(queue.length(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_acquire() -> Result<()> {
    let test_count = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 5) as usize
        + 5;

    let barrier = Arc::new(tokio::sync::Barrier::new(test_count));
    let queue = QueueManager::<TestData>::create(2);
    let mut join_handles = Vec::with_capacity(test_count);

    let instant = Instant::now();
    for index in 0..test_count {
        join_handles.push({
            let queue = queue.clone();
            let barrier = barrier.clone();
            databend_common_base::runtime::spawn(async move {
                barrier.wait().await;
                let _guard = queue
                    .acquire(TestData(format!("TestData{}", index)))
                    .await?;

                tokio::time::sleep(Duration::from_secs(1)).await;
                Result::<(), ErrorCode>::Ok(())
            })
        })
    }

    for join_handle in join_handles {
        let _ = join_handle.await;
    }

    assert!(instant.elapsed() >= Duration::from_secs((test_count / 2) as u64));
    assert!(instant.elapsed() < Duration::from_secs((test_count) as u64));

    assert_eq!(queue.length(), 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_acquire() -> Result<()> {
    let test_count = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 5) as usize
        + 5;

    let barrier = Arc::new(tokio::sync::Barrier::new(test_count));
    let queue = QueueManager::<TestData>::create(1);
    let mut join_handles = Vec::with_capacity(test_count);

    for index in 0..test_count {
        join_handles.push({
            let queue = queue.clone();
            let barrier = barrier.clone();
            databend_common_base::runtime::spawn(async move {
                barrier.wait().await;
                let _guard = queue
                    .acquire(TestData(format!("TestData{}", index)))
                    .await?;

                tokio::time::sleep(Duration::from_secs(10)).await;
                Result::<(), ErrorCode>::Ok(())
            })
        })
    }

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(queue.length(), test_count - 1);

    Ok(())
}
