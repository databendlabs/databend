// Copyright 2021 Datafuse Labs.
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

use common_base::base::*;
use common_exception::Result;
use common_tracing::tracing;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::time::Duration;

/// A task that takes 100 years to gracefully stop.
#[derive(Default)]
struct FooTask {}

#[async_trait::async_trait]
impl Stoppable for FooTask {
    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn stop(&mut self, force: Option<broadcast::Receiver<()>>) -> Result<()> {
        tracing::info!("--- FooTask stop, force: {:?}", force);

        // block the stop until force stop.

        if let Some(mut force) = force {
            tracing::info!("--- waiting for force");
            let _ = force.recv().await;
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_stoppable() -> Result<()> {
    // - Create a task and start it.
    // - Stop but the task would block.
    // - Signal the task to force stop.

    let (stop_tx, rx) = broadcast::channel::<()>(1024);
    let (fin_tx, mut fin_rx) = oneshot::channel::<()>();

    let mut t = FooTask::default();

    // Start the task

    assert!(t.start().await.is_ok());

    // Gracefully stop blocks.

    tokio::spawn(async move {
        let _ = t.stop(Some(rx)).await;
        fin_tx.send(()).expect("fail to send fin signal");
    });

    // `stop` should not return.

    tokio::time::sleep(Duration::from_millis(100)).await;

    let res = fin_rx.try_recv();
    match res {
        Err(TryRecvError::Empty) => { /* good */ }
        _ => {
            panic!("should not ready");
        }
    };

    // Send force stop

    stop_tx.send(()).expect("fail to send force stop");

    assert!(fin_rx.await.is_ok());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stop_handle() -> Result<()> {
    // - Create 2 tasks and start them.
    // - Stop but the task would block.
    // - Signal the task to force stop.

    common_tracing::init_default_ut_tracing();

    let (stop_tx, _) = broadcast::channel::<()>(1024);

    let mut t1 = FooTask::default();
    let mut t2 = FooTask::default();

    // Start the task

    assert!(t1.start().await.is_ok());
    assert!(t2.start().await.is_ok());

    let (fin_tx, mut fin_rx) = oneshot::channel::<()>();

    let mut h = StopHandle::create();
    h.push(Box::new(t1));
    h.push(Box::new(t2));

    // Block on waiting for the handle to finish.

    let fut = h.wait_to_terminate(stop_tx.clone());
    tokio::spawn(async move {
        fut.await;
        fin_tx.send(()).expect("fail to send fin signal");
    });

    tracing::info!("--- send graceful stop");
    stop_tx.send(()).expect("fail to set graceful stop");

    // Broadcasting receiver can not receive the message sent before subscribing the sender.
    // Wait for a while until the `stop()` method is called for every task.
    tokio::time::sleep(Duration::from_millis(100)).await;

    tracing::info!("--- fin_rx should receive nothing");
    let res = fin_rx.try_recv();
    match res {
        Err(TryRecvError::Empty) => { /* good */ }
        _ => {
            panic!("should not ready");
        }
    };

    tracing::info!("--- send force stop");
    stop_tx.send(()).expect("fail to set force stop");

    assert!(fin_rx.await.is_ok());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stop_handle_drop() -> Result<()> {
    // - Create a task and start it.
    // - Then quit and the Drop should forcibly stop it and the test should not block.

    common_tracing::init_default_ut_tracing();

    let mut t1 = FooTask::default();

    // Start the task

    assert!(t1.start().await.is_ok());

    let mut h = StopHandle::create();
    h.push(Box::new(t1));

    Ok(())
}
