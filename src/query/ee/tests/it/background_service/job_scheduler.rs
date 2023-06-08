// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use enterprise_query::background_service::{Job, JobScheduler};
use common_exception::Result;
use common_base::base::tokio;

#[derive(Clone)]
struct TestJob {
    counter: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl Job for TestJob {
    async fn run(&self) {
        self.counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_one_shot_job() -> Result<()> {
    let scheduler = JobScheduler::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let job = TestJob {
        counter: counter.clone()
    };
    let scheduler = scheduler.add_one_shot_job(job.clone());
    scheduler.start().await?;
    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
    Ok(())
}