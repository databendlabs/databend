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

use std::sync::Arc;
use std::sync::Mutex;

use common_base::*;
use common_exception::Result;

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
