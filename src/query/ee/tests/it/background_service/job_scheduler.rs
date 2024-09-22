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

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use chrono::TimeZone;
use chrono::Utc;
use chrono_tz::Tz;
use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::mpsc::Sender;
use databend_common_base::base::tokio::sync::Mutex;
use databend_common_exception::Result;
use databend_common_meta_app::background::BackgroundJobIdent;
use databend_common_meta_app::background::BackgroundJobInfo;
use databend_common_meta_app::background::BackgroundJobParams;
use databend_common_meta_app::background::BackgroundJobStatus;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::SeqV;
use databend_enterprise_query::background_service::Job;
use databend_enterprise_query::background_service::JobScheduler;

#[derive(Clone)]
struct TestJob {
    counter: Arc<AtomicUsize>,
    info: SeqV<BackgroundJobInfo>,
    finish_tx: Arc<Mutex<Sender<u64>>>,
}

fn new_info(params: BackgroundJobParams, status: BackgroundJobStatus) -> BackgroundJobInfo {
    BackgroundJobInfo {
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
    async fn run(&mut self) {
        self.counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let _ = self.finish_tx.clone().lock().await.send(1).await;
    }

    async fn get_info(&self) -> Result<SeqV<BackgroundJobInfo>> {
        Ok(self.info.clone())
    }

    fn get_name(&self) -> BackgroundJobIdent {
        BackgroundJobIdent::new(Tenant::new_literal("test"), "test")
    }

    async fn update_job_status(&mut self, _status: BackgroundJobStatus) -> Result<()> {
        Ok(())
    }

    async fn update_job_params(&mut self, _params: BackgroundJobParams) -> Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_one_shot_job() -> Result<()> {
    let mut scheduler = JobScheduler::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let job = TestJob {
        counter: counter.clone(),
        info: SeqV::new(
            0,
            BackgroundJobInfo::new_compactor_job(
                BackgroundJobParams::new_one_shot_job(),
                UserIdentity::default(),
            ),
        ),
        finish_tx: scheduler.finish_tx.clone(),
    };
    scheduler.add_job(job.clone()).await?;
    // println!("what happened");
    scheduler.start().await?;
    // // atomic operation is flaky, so we need to sleep for a while
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 1);
    Ok(())
}

// test interval job behavior with suspend support
#[tokio::test(flavor = "multi_thread")]
async fn test_interval_job() -> Result<()> {
    let mut scheduler = JobScheduler::new();
    scheduler.job_tick_interval = Duration::from_millis(5);
    let counter = Arc::new(AtomicUsize::new(0));
    let job = TestJob {
        counter: counter.clone(),
        info: SeqV::new(
            0,
            BackgroundJobInfo::new_compactor_job(
                BackgroundJobParams::new_interval_job(Duration::from_millis(10)),
                UserIdentity::default(),
            ),
        ),
        finish_tx: scheduler.finish_tx.clone(),
    };
    scheduler.add_job(job.clone()).await?;
    let suspend_tx = scheduler.suspend_tx.clone();
    databend_common_base::runtime::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = suspend_tx.lock().await.send(()).await;
    });
    scheduler.start().await?;
    // atomic operation is flaky, so we need to sleep for a while
    assert!(counter.load(std::sync::atomic::Ordering::Relaxed) > 5);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_should_run_job() -> Result<()> {
    let current_time = chrono::Utc::now();

    let interval_job = new_info(
        BackgroundJobParams::new_interval_job(std::time::Duration::from_secs(1000)),
        BackgroundJobStatus {
            job_state: Default::default(),
            last_task_id: None,
            last_task_run_at: None,
            next_task_scheduled_time: Some(current_time - chrono::Duration::seconds(1)),
        },
    );
    assert!(JobScheduler::should_run_job(
        &interval_job,
        current_time,
        false
    ));

    let interval_job = new_info(
        BackgroundJobParams::new_interval_job(std::time::Duration::from_secs(1000)),
        BackgroundJobStatus {
            job_state: Default::default(),
            last_task_id: None,
            last_task_run_at: None,
            next_task_scheduled_time: Some(current_time + chrono::Duration::seconds(1)),
        },
    );
    assert!(!JobScheduler::should_run_job(
        &interval_job,
        current_time,
        false
    ));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_should_run_job_cron() -> Result<()> {
    let fixed_time = Utc.with_ymd_and_hms(2023, 6, 15, 10, 0, 0).unwrap();
    let cron_expression = "0 0 * * * *"; // Every hour

    let params = BackgroundJobParams::new_cron_job(cron_expression.to_string(), None);
    let scheduled_time = params.get_next_running_time(fixed_time).unwrap();
    assert_eq!(
        scheduled_time,
        Utc.with_ymd_and_hms(2023, 6, 15, 11, 0, 0).unwrap()
    );

    let should_run = JobScheduler::should_run_job(
        &new_info(params.clone(), BackgroundJobStatus {
            job_state: Default::default(),
            last_task_id: None,
            last_task_run_at: None,
            next_task_scheduled_time: Some(Utc.with_ymd_and_hms(2023, 6, 15, 11, 0, 0).unwrap()),
        }),
        Utc.with_ymd_and_hms(2023, 6, 15, 10, 30, 0).unwrap(),
        false,
    );
    assert!(!should_run);
    let should_run = JobScheduler::should_run_job(
        &new_info(params, BackgroundJobStatus {
            job_state: Default::default(),
            last_task_id: None,
            last_task_run_at: None,
            next_task_scheduled_time: Some(Utc.with_ymd_and_hms(2023, 6, 15, 11, 0, 0).unwrap()),
        }),
        Utc.with_ymd_and_hms(2023, 6, 15, 10, 30, 0).unwrap(),
        true,
    );
    assert!(should_run);
    let params = BackgroundJobParams::new_cron_job(
        cron_expression.to_string(),
        Some(Tz::America__Los_Angeles),
    );
    let local_time = Tz::America__Los_Angeles
        .with_ymd_and_hms(2023, 6, 15, 10, 30, 0)
        .unwrap()
        .with_timezone(&Utc);
    let scheduled_time = params.get_next_running_time(local_time).unwrap();
    assert_eq!(
        scheduled_time,
        Tz::America__Los_Angeles
            .with_ymd_and_hms(2023, 6, 15, 11, 00, 0)
            .unwrap()
            .with_timezone(&Utc)
    );

    Ok(())
}
