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
use common_base::base::tokio;
use crate::background_service::configs::JobSchedulerConfig;
use crate::background_service::job::{BoxedJob, Job};
use common_exception::Result;

pub struct JobScheduler {
    jobs: Vec<BoxedJob>,
    config: JobSchedulerConfig,
}

impl JobScheduler {
    /// Creates a new runner based on the given SchedulerConfig
    pub fn new(config: JobSchedulerConfig) -> Self {
        Self {
            config,
            jobs: Vec::new(),
        }
    }
    /// Adds a job to the scheduler
    pub fn add_job(mut self, job: impl Job + Send + Sync + Clone + 'static) -> Self {
        self.jobs.push(Box::new(job) as BoxedJob);
        self
    }

    pub async fn start(self) -> Result<()> {
        let mut job_interval = tokio::time::interval(self.config.poll_interval);
        let jobs = Arc::new(&self.jobs);
        loop {
            job_interval.tick().await;
            self.check_and_run_jobs(jobs.clone()).await;
        }
    }

    // Checks and runs, if necessary, all jobs concurrently
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