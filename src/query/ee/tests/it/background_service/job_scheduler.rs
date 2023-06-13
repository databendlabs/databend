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
use chrono::{TimeZone, Utc};
use chrono_tz::Tz;
use enterprise_query::background_service::{Job, JobScheduler};
use common_exception::Result;
use common_base::base::{GlobalInstance, tokio};
use common_meta_app::background::{BackgroundJobInfo, BackgroundJobParams, BackgroundJobStatus};
use databend_query::servers::ShutdownHandle;
use databend_query::test_kits::TestGlobalServices;

#[derive(Clone)]
struct TestJob {
    counter: Arc<AtomicUsize>,
    info: BackgroundJobInfo,
}

fn new_info(params: BackgroundJobParams, status: BackgroundJobStatus) -> BackgroundJobInfo {
        BackgroundJobInfo{
            job_params: Some(params),
            job_status: Some(status),
            task_type: Default::default(),
            last_updated: None,
            message: "".to_string(),
            creator: None,
            created_at: Default::default(),
        }
}

#[async_trait::async_trait]
impl Job for TestJob {
    async fn run(&self) {
        self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    async fn get_info(&self) -> BackgroundJobInfo {
        todo!()
    }

    async fn update_job_status(&mut self, status: BackgroundJobStatus) -> Result<()> {
        todo!()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_one_shot_job() -> Result<()> {
    let _guard =
        TestGlobalServices::setup(databend_query::test_kits::ConfigBuilder::create().build())
            .await?;
    let mut scheduler = JobScheduler::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let job = TestJob {
        counter: counter.clone(),
        info: Default::default(),
    };
    scheduler.add_job(job.clone()).await?;
    let mut shutdown_handle = ShutdownHandle::create()?;
    scheduler.start(&mut shutdown_handle).await?;
    // atomic operation is flaky, so we need to sleep for a while
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_should_run_job() -> Result<()> {
    let current_time = chrono::Utc::now();

    let interval_job = new_info(
        BackgroundJobParams::new_interval_job(1000),
        BackgroundJobStatus{
            job_state: Default::default(),
            last_task_id: None,
            last_task_run_at: None,
            next_task_scheduled_time:  Some(current_time.clone() - chrono::Duration::seconds(1)),
        }
    );
    assert!(JobScheduler::should_run_job(&interval_job, current_time));

    let interval_job = new_info(
        BackgroundJobParams::new_interval_job(1000),
        BackgroundJobStatus{
            job_state: Default::default(),
            last_task_id: None,
            last_task_run_at: None,
            next_task_scheduled_time:  Some(current_time.clone() + chrono::Duration::seconds(1)),
        }
    );
    assert!(!JobScheduler::should_run_job(&interval_job, current_time));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_should_run_job_cron() -> Result<()> {
    let fixed_time = Utc.with_ymd_and_hms(2023, 6, 15, 10, 0, 0).unwrap();
    let cron_expression = "0 0 * * * *"; // Every hour

    let params = BackgroundJobParams::new_cron_job(cron_expression.to_string(), None);
    let scheduled_time = params.get_next_running_time(fixed_time).unwrap();
    assert_eq!(scheduled_time, Utc.with_ymd_and_hms(2023, 6, 15, 11, 0, 0).unwrap());

    let should_run = JobScheduler::should_run_job(&new_info(
        params.clone(),
        BackgroundJobStatus{
            job_state: Default::default(),
            last_task_id: None,
            last_task_run_at: None,
            next_task_scheduled_time: Some(Utc.with_ymd_and_hms(2023, 6, 15, 11, 0, 0).unwrap()),
        }
    ), Utc.with_ymd_and_hms(2023, 6, 15, 10, 30, 0).unwrap());
    assert!(!should_run);

    let params = BackgroundJobParams::new_cron_job(cron_expression.to_string(), Some(Tz::America__Los_Angeles));
    let local_time = Tz::America__Los_Angeles.with_ymd_and_hms(2023, 6, 15, 10, 30, 0).unwrap().with_timezone(&Utc);
    let scheduled_time = params.get_next_running_time(local_time).unwrap();
    assert_eq!(scheduled_time, Tz::America__Los_Angeles.with_ymd_and_hms(2023, 6, 15, 11, 00, 0).unwrap().with_timezone(&Utc));

    Ok(())
}