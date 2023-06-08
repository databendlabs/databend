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
use futures_util::future::join_all;

use common_base::base::{DummySignalStream, SignalType, tokio};
use common_exception::Result;

use crate::background_service::job::BoxedJob;
use crate::background_service::job::Job;

use databend_query::servers::ShutdownHandle;

pub struct JobScheduler {
    one_shot_jobs: Vec<BoxedJob>,
}

impl JobScheduler {
    /// Creates a new runner based on the given SchedulerConfig
    pub fn new() -> Self {
        Self {
            one_shot_jobs: Vec::new(),
        }
    }
    /// Adds a job to the scheduler
    pub fn add_one_shot_job(mut self, job: impl Job + Send + Sync + Clone + 'static) -> Self {
        self.one_shot_jobs.push(Box::new(job) as BoxedJob);
        self
    }

    pub async fn start(self, handler: &mut ShutdownHandle) -> Result<()> {
        let one_shot_jobs = Arc::new(&self.one_shot_jobs);
        self.check_and_run_jobs(one_shot_jobs).await;
        handler.shutdown(DummySignalStream::create(SignalType::Exit)).await;
        Ok(())
    }
    async fn check_and_run_jobs(&self, jobs: Arc<&Vec<BoxedJob>>) {
        let job_futures = jobs
            .iter()
            .map(|job| {
                let j = job.box_clone();
                self.check_and_run_job(j)
            })
            .collect::<Vec<_>>();
        join_all(job_futures).await;
    }
    // Checks and runs a single [Job](crate::Job)
    async fn check_and_run_job(&self, job: BoxedJob) -> Result<()> {
        tokio::spawn(async move {
            job.run().await;
        });
        Ok(())
    }
}
