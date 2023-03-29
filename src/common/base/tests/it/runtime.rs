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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use common_base::runtime::Runtime;
use common_base::runtime::TrySpawn;
use common_exception::Result;
use futures::future::BoxFuture;
use once_cell::sync::Lazy;
use rand::distributions::Distribution;
use rand::distributions::Uniform;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::Semaphore;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_runtime() -> Result<()> {
    let counter = Arc::new(Mutex::new(0));

    let runtime = Runtime::with_default_worker_threads()?;
    let runtime_counter = Arc::clone(&counter);
    let runtime_header = runtime.spawn(async move {
        let rt1 = Runtime::with_default_worker_threads().unwrap();
        let rt1_counter = Arc::clone(&runtime_counter);
        let rt1_header = rt1.spawn(async move {
            let rt2 = Runtime::with_worker_threads(1, None).unwrap();
            let rt2_counter = Arc::clone(&rt1_counter);
            let rt2_header = rt2.spawn(async move {
                let rt3 = Runtime::with_default_worker_threads().unwrap();
                let rt3_counter = Arc::clone(&rt2_counter);
                let rt3_header = rt3.spawn(async move {
                    let mut num = rt3_counter.lock().unwrap();
                    *num += 1;
                });
                rt3_header.await.unwrap();

                let mut num = rt2_counter.lock().unwrap();
                *num += 1;
            });
            rt2_header.await.unwrap();

            let mut num = rt1_counter.lock().unwrap();
            *num += 1;
        });
        rt1_header.await.unwrap();

        let mut num = runtime_counter.lock().unwrap();
        *num += 1;
    });
    runtime_header.await.unwrap();

    let result = *counter.lock().unwrap();
    assert_eq!(result, 4);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_shutdown_long_run_runtime() -> Result<()> {
    let runtime = Runtime::with_default_worker_threads()?;

    runtime.spawn(async move {
        std::thread::sleep(Duration::from_secs(6));
    });

    let instant = Instant::now();
    drop(runtime);
    assert!(instant.elapsed() >= Duration::from_secs(3));
    assert!(instant.elapsed() < Duration::from_secs(4));

    Ok(())
}

static START_TIME: Lazy<Instant> = Lazy::new(Instant::now);

// println can more clearly know if they are parallel
async fn mock_get_page(i: usize) -> Vec<usize> {
    let millis = Uniform::from(0..10).sample(&mut rand::thread_rng());
    println!(
        "[{}] > get_page({}) will complete in {} ms, {:?}",
        START_TIME.elapsed().as_millis(),
        i,
        millis,
        std::thread::current().id(),
    );

    sleep(Duration::from_millis(millis)).await;
    println!(
        "[{}] < get_page({}) completed",
        START_TIME.elapsed().as_millis(),
        i
    );

    (i..(i + 1)).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_runtime_try_spawn_batch() -> Result<()> {
    let runtime = Runtime::with_default_worker_threads()?;

    let mut futs = vec![];
    for i in 0..20 {
        futs.push(mock_get_page(i));
    }

    let max_concurrency = Semaphore::new(3);
    let handlers = runtime.try_spawn_batch(max_concurrency, futs).await?;
    let result = futures::future::try_join_all(handlers).await.unwrap();
    assert_eq!(result.len(), 20);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_aaa() -> Result<()> {
    #[async_backtrace::framed]
    async fn pending(duration: Duration) {
        tokio::time::sleep(duration).await
        // std::future::pending::<()>().await
    }

    #[async_backtrace::framed]
    async fn foo() {
        bar().await;
    }

    #[async_backtrace::framed]
    async fn bar() {
        futures::join!(fiz(), buz());
    }

    #[async_backtrace::framed]
    async fn fiz() {
        tokio::task::yield_now().await;
    }

    #[async_backtrace::framed]
    async fn buz() {
        println!("{}", baz().await);
    }

    #[async_backtrace::framed]
    async fn baz() -> String {
        async_backtrace::taskdump_tree(false)
    }

    // let (tx, rx) = tokio::sync::broadcast::channel(1);
    //
    let handle3 = tokio::spawn(async_backtrace::frame!(pending(Duration::from_secs(2))));
    //
    let handle4 = tokio::spawn(async_backtrace::frame!(pending(Duration::from_secs(4))));

    // let handler = tokio::spawn(async_backtrace::frame!(futures::future::select(
    //     Box::pin(recv(rx)),
    //     Box::pin(pending()),
    // )));

    // let handler1 = tokio::spawn(async move {
    //     let mut index = 0;
    //     for index in 0..3 {
    //         println!("index {}", index);
    //         let (tx2, mut rx2) = tokio::sync::mpsc::channel(1);
    //         if tx.send((true, tx2)).is_ok() {
    //             if let Some(message) = rx2.recv().await {
    //                 println!("{}", message);
    //             }
    //         }
    //
    //         tokio::time::sleep(Duration::from_secs(2)).await;
    //         // if let Err(_) = tx.send((true, tx2)) {
    //         //     break;
    //         // }
    //     }
    // });

    // handler.await;
    // tx.send(())

    handle3.await;
    println!("{}", async_backtrace::taskdump_tree(false));
    handle4.await;

    // tokio::select! {
    //     _ = tokio::spawn(async_backtrace::frame!(pending())) => {}
    //     _ = foo() => {}
    // };

    Ok(())
}

#[async_backtrace::framed]
async fn recv(mut receiver: Receiver<(bool, Sender<String>)>) {
    while let Ok((wait_for_running_tasks, tx)) = receiver.recv().await {
        if tx
            .send(async_backtrace::taskdump_tree(wait_for_running_tasks))
            .await
            .is_err()
        {
            break;
        }
    }
}
