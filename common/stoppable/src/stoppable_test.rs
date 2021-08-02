// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCode;
use common_runtime::tokio;
use common_runtime::tokio::sync::broadcast;
use common_runtime::tokio::sync::oneshot;
use common_runtime::tokio::sync::oneshot::error::TryRecvError;
use common_runtime::tokio::time::Duration;
use common_tracing::tracing;

use crate::stop_handle::StopHandle;
use crate::Stoppable;

/// A task that takes 100 years to gracefully stop.
#[derive(Default)]
struct FooTask {}

#[async_trait::async_trait]
impl Stoppable for FooTask {
    async fn start(&mut self) -> Result<(), ErrorCode> {
        Ok(())
    }

    async fn stop(&mut self, force: Option<broadcast::Receiver<()>>) -> Result<(), ErrorCode> {
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
async fn test_stoppable() -> anyhow::Result<()> {
    // - Create a task and start it.
    // - Stop but the task would block.
    // - Signal the task to force stop.

    let (stop_tx, rx) = broadcast::channel::<()>(1024);
    let (fin_tx, mut fin_rx) = oneshot::channel::<()>();

    let mut t = FooTask::default();

    // Start the task

    t.start().await?;

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

    fin_rx.await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stop_handle() -> anyhow::Result<()> {
    // - Create 2 tasks and start them.
    // - Stop but the task would block.
    // - Signal the task to force stop.

    common_tracing::init_default_tracing();

    let (stop_tx, _) = broadcast::channel::<()>(1024);

    let mut t1 = FooTask::default();
    let mut t2 = FooTask::default();

    // Start the task

    t1.start().await?;
    t2.start().await?;

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

    fin_rx.await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stop_handle_drop() -> anyhow::Result<()> {
    // - Create a task and start it.
    // - Then quit and the Drop should forcibly stop it and the test should not block.

    common_tracing::init_default_tracing();

    let (tx, _rx) = broadcast::channel::<()>(1024);

    let mut t1 = FooTask::default();

    // Start the task

    t1.start().await?;

    let mut h = StopHandle::create();
    h.push(Box::new(t1));

    Ok(())
}
