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
use chrono::{DateTime, Utc};
use tracing::info;
use common_base::base::{tokio};
use common_base::base::tokio::sync::Mutex;
use common_base::base::tokio::sync::mpsc::{Receiver, Sender};
use common_exception::Result;
use common_meta_app::background::{BackgroundJobInfo, BackgroundJobState, BackgroundJobType};

use crate::background_service::job::BoxedJob;
use crate::background_service::job::Job;

use databend_query::servers::ShutdownHandle;

pub struct JobScheduler {
    one_shot_jobs: Vec<BoxedJob>,
    scheduled_jobs: Vec<BoxedJob>,
    pub finish_tx: Arc<Mutex<Sender<u64>>>,
    pub finish_rx: Arc<Mutex<Receiver<u64>>>
}

impl JobScheduler {
    /// Creates a new runner based on the given SchedulerConfig
    pub fn new() -> Self {
        let (finish_tx, finish_rx) = tokio::sync::mpsc::channel(100);
        Self {
            one_shot_jobs: Vec::new(),
            scheduled_jobs: Vec::new(),
            finish_tx: Arc::new(Mutex::new(finish_tx)),
            finish_rx: Arc::new(Mutex::new(finish_rx)),
        }
    }

    pub async fn add_job(&mut self, job: impl Job + Send + Sync + Clone + 'static) -> Result<()> {
        if job.get_info().await.job_params.is_none() {
            return Ok(())
        }
        match job.get_info().await.job_params.unwrap().job_type  {
            BackgroundJobType::ONESHOT => {
                self.one_shot_jobs.push(Box::new(job) as BoxedJob);
            }
            BackgroundJobType::CRON | BackgroundJobType::INTERVAL => {
                self.scheduled_jobs.push(Box::new(job) as BoxedJob);
            }
        }
        Ok(())
    }

    pub async fn start(&self, handler: &mut ShutdownHandle) -> Result<()> {
        let one_shot_jobs = Arc::new(&self.one_shot_jobs);
        self.check_and_run_jobs(one_shot_jobs.clone()).await;
        let mut finished_one_shot_jobs = vec![];
        while let Some(i) = self.finish_rx.clone().lock().await.recv().await {
            finished_one_shot_jobs.push(i);
            if finished_one_shot_jobs.len() == one_shot_jobs.len() {
                break;
            }
        }

        info!(background = true, "start scheduled jobs");
        let scheduled_jobs = Arc::new(&self.scheduled_jobs);
        let scheduled_jobs_clone = scheduled_jobs.clone();

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
        for job in job_futures {
            job.await.unwrap();
        }

    }
    // Checks and runs a single [Job](crate::Job)
    async fn check_and_run_job(&self, job: BoxedJob) -> Result<()> {
        tokio::spawn(async move {
            job.run().await;
        });
        Ok(())
    }

    // returns true if the job should be run
    pub fn should_run_job(job_info: &BackgroundJobInfo, time: DateTime<Utc>) -> bool {
        if job_info.job_status.clone().is_none() || job_info.job_params.clone().is_none() {
            return false;
        }

        let job_params = &job_info.job_params.clone().unwrap();
        let job_status = &job_info.job_status.clone().unwrap();
        if job_status.job_state == BackgroundJobState::FAILED || job_status.job_state == BackgroundJobState::SUSPENDED {
            return false;
        }
        return match job_params.job_type {
            BackgroundJobType::INTERVAL => {
                match job_status.next_task_scheduled_time {
                    None => {
                        true
                    }
                    Some(expected) => {
                        expected < time
                    }
                }
            }
            BackgroundJobType::CRON => {
                match job_status.next_task_scheduled_time {
                    None => {
                        true
                    }
                    Some(expected) => {
                        expected < time
                    }
                }
            }
            BackgroundJobType::ONESHOT => { true }
        }
    }
}
