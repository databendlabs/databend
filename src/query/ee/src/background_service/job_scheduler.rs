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

use common_base::base::tokio;
use common_exception::Result;

use crate::background_service::job::BoxedJob;
use crate::background_service::job::Job;

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

    pub async fn start(self) -> Result<()> {
        let one_shot_jobs = Arc::new(&self.one_shot_jobs);
        for job in one_shot_jobs.iter() {
            self.check_and_run_job(job.box_clone()).await?;
        }

        Ok(())
    }

    // Checks and runs a single [Job](crate::Job)
    async fn check_and_run_job(&self, job: BoxedJob) -> Result<()> {
        tokio::spawn(async move {
            job.run().await;
        });
        Ok(())
    }
}
