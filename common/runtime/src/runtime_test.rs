// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_runtime() -> anyhow::Result<()> {
    use crate::*;

    let runtime = Runtime::with_default_worker_threads()?;
    runtime.spawn(async {
        let rt1 = Runtime::with_default_worker_threads().unwrap();
        rt1.spawn(async {
            let rt2 = Runtime::with_worker_threads(1).unwrap();
            rt2.spawn(async {
                let rt3 = Runtime::with_default_worker_threads().unwrap();
                rt3.spawn(async {});
            });
        });
    });
    Ok(())
}
