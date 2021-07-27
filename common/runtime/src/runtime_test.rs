// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_runtime() -> Result<()> {
    use crate::*;

    let runtime = Runtime::with_default_worker_threads()?;
    let runtime_header = runtime.spawn(async {
        let rt1 = Runtime::with_default_worker_threads().unwrap();
        let rt1_header = rt1.spawn(async {
            let rt2 = Runtime::with_worker_threads(1).unwrap();
            let rt2_header = rt2.spawn(async {
                let rt3 = Runtime::with_default_worker_threads().unwrap();
                let rt3_header = rt3.spawn(async {
                    println!("rt3 is ok!");
                });
                rt3_header.await.unwrap();
                println!("rt2 is ok!");
            });
            rt2_header.await.unwrap();
            println!("rt1 is ok!");
        });
        rt1_header.await.unwrap();
        println!("runtime is ok!");
    });
    runtime_header.await.unwrap();
    println!("all is ok!");
    Ok(())
}
