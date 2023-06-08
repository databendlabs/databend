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

use std::process;
use std::process::exit;
use std::sync::Arc;
use futures_util::future::join_all;
use futures_util::TryFutureExt;
use tracing::info;
use common_base::base::tokio::runtime::Handle;
use common_base::base::{DummySignalStream, signal_stream, SignalType, tokio};
use common_base::base::tokio::sync::{futures, Mutex, RwLock};
use common_base::base::tokio::sync::mpsc::{Receiver, Sender};
use common_exception::Result;

use crate::background_service::job::BoxedJob;
use crate::background_service::job::Job;

use databend_query::servers::ShutdownHandle;

pub struct JobScheduler {
    one_shot_jobs: Vec<BoxedJob>,
    pub finish_tx: Arc<Mutex<Sender<u64>>>,
    pub finish_rx: Arc<Mutex<Receiver<u64>>>
}

impl JobScheduler {
    /// Creates a new runner based on the given SchedulerConfig
    pub fn new() -> Self {
        let (finish_tx, finish_rx) = tokio::sync::mpsc::channel(100);
        Self {
            one_shot_jobs: Vec::new(),
            finish_tx: Arc::new(Mutex::new(finish_tx)),
            finish_rx: Arc::new(Mutex::new(finish_rx)),
        }
    }
    /// Adds a job to the scheduler
    pub fn add_one_shot_job(mut self, job: impl Job + Send + Sync + Clone + 'static) -> Self {
        self.one_shot_jobs.push(Box::new(job) as BoxedJob);
        self
    }

    pub async fn start(&self, handler: &mut ShutdownHandle) -> Result<()> {
        let one_shot_jobs = Arc::new(&self.one_shot_jobs);
        self.check_and_run_jobs(one_shot_jobs.clone()).await;
        let mut finished_jobs = vec![];
        while let Some(i) = self.finish_rx.clone().lock().await.recv().await {
            finished_jobs.push(i);
            if finished_jobs.len() == one_shot_jobs.len() {
                break;
            }
        }
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
}
