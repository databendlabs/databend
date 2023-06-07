
use chrono::DateTime;
use chrono::Utc;
use common_meta_app::background::BackgroundJobIdent;
use common_meta_app::background::BackgroundJobInfo;
use common_meta_app::background::BackgroundJobState;
use common_meta_app::background::BackgroundJobType::ONESHOT;
use common_meta_app::background::BackgroundTaskIdent;
use common_meta_app::background::BackgroundTaskInfo;
use common_meta_app::background::BackgroundTaskState;
use common_meta_app::background::CreateBackgroundJobReq;
use common_meta_app::background::GetBackgroundJobReq;
use common_meta_app::background::GetBackgroundTaskReq;
use common_meta_app::background::ListBackgroundJobsReq;
use common_meta_app::background::ListBackgroundTasksReq;
use common_meta_app::background::UpdateBackgroundJobReq;
use common_meta_app::background::UpdateBackgroundTaskReq;
use common_meta_kvapi::kvapi;
use common_meta_types::MetaError;
use tracing::info;

use crate::background_api::BackgroundApi;
use crate::SchemaApi;

fn new_background_task(
    state: BackgroundTaskState,
    created_at: DateTime<Utc>,
) -> BackgroundTaskInfo {
    BackgroundTaskInfo {
        last_updated: None,
        task_type: Default::default(),
        task_state: state,
        message: "".to_string(),
        compaction_task_stats: None,
        vacuum_stats: None,
        creator: None,
        created_at,
    }
}

fn new_background_job(state: BackgroundJobState, created_at: DateTime<Utc>) -> BackgroundJobInfo {
    BackgroundJobInfo {
        job_type: ONESHOT,
        last_updated: None,
        task_type: Default::default(),
        message: "".to_string(),
        creator: None,
        created_at,
        job_state: state,
    }
}

/// Test suite of `BackgroundApi`.
///
/// It is not used by this crate, but is used by other crate that impl `ShareApi`,
/// to ensure an impl works as expected,
/// such as `meta/embedded` and `metasrv`.
#[derive(Copy, Clone)]
pub struct BackgroundApiTestSuite {}

impl BackgroundApiTestSuite {
    /// Test BackgroundTaskApi on a single node
    pub async fn test_single_node<B, MT>(b: B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: BackgroundApi + kvapi::AsKVApi<Error = MetaError> + SchemaApi,
    {
        let suite = BackgroundApiTestSuite {};

        suite.update_background_tasks(&b.build().await).await?;
        suite.update_background_jobs(&b.build().await).await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn update_background_tasks<MT: BackgroundApi + kvapi::AsKVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let task_id = "uuid1";
        let task_name = BackgroundTaskIdent {
            tenant: tenant.to_string(),
            task_id: task_id.to_string(),
        };

        info!("--- list background tasks when their is no tasks");
        {
            let req = ListBackgroundTasksReq {
                tenant: tenant.to_string(),
            };

            let res = mt.list_background_tasks(req).await;
            assert!(res.is_ok());
            let resp = res.unwrap();
            assert!(resp.is_empty());
        }

        info!("--- create a background task");
        let create_on = Utc::now();
        // expire after 5 secs
        let expire_at = create_on + chrono::Duration::seconds(5);
        {
            let req = UpdateBackgroundTaskReq {
                task_name: task_name.clone(),
                task_info: new_background_task(BackgroundTaskState::STARTED, create_on),
                expire_at: expire_at.timestamp() as u64,
            };

            let res = mt.update_background_task(req).await;
            info!("update log res: {:?}", res);
            let res = mt
                .get_background_task(GetBackgroundTaskReq {
                    name: task_name.clone(),
                })
                .await;
            info!("get log res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(
                BackgroundTaskState::STARTED,
                res.task_info.unwrap().task_state,
                "first state is started"
            );
        }
        {
            let req = UpdateBackgroundTaskReq {
                task_name: task_name.clone(),
                task_info: new_background_task(BackgroundTaskState::DONE, create_on),
                expire_at: expire_at.timestamp() as u64,
            };

            let res = mt.update_background_task(req).await;
            info!("update log res: {:?}", res);
            let res = mt
                .get_background_task(GetBackgroundTaskReq {
                    name: task_name.clone(),
                })
                .await;
            info!("get log res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(
                BackgroundTaskState::DONE,
                res.task_info.unwrap().task_state,
                "first state is done"
            );
        }
        {
            let req = ListBackgroundTasksReq {
                tenant: tenant.to_string(),
            };

            let res = mt.list_background_tasks(req).await;
            info!("update log res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.len(), "there is one task");
            assert_eq!(
                BackgroundTaskState::DONE,
                res[0].1.task_state,
                "first state is done"
            );
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn update_background_jobs<MT: BackgroundApi + kvapi::AsKVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let job_name = "uuid1";
        let job_ident = BackgroundJobIdent {
            tenant: tenant.to_string(),
            name: job_name.to_string(),
        };

        info!("--- list background jobs when their is no tasks");
        {
            let req = ListBackgroundJobsReq {
                tenant: tenant.to_string(),
            };

            let res = mt.list_background_jobs(req).await;
            assert!(res.is_ok());
            let resp = res.unwrap();
            assert!(resp.is_empty());
        }

        info!("--- create a background job");
        let create_on = Utc::now();
        {
            let req = CreateBackgroundJobReq {
                if_not_exists: true,
                job_name: job_ident.clone(),
                job_info: new_background_job(BackgroundJobState::RUNNING, create_on),
            };

            let res = mt.create_background_job(req).await;
            info!("update log res: {:?}", res);
            let res = mt
                .get_background_job(GetBackgroundJobReq {
                    name: job_ident.clone(),
                })
                .await;
            info!("get log res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(
                BackgroundJobState::RUNNING,
                res.info.job_state,
                "first state is started"
            );
        }
        info!("--- update a background job");
        {
            let req = UpdateBackgroundJobReq {
                job_name: job_ident.clone(),
                info: new_background_job(BackgroundJobState::SUSPENDED, create_on),
            };

            let res = mt.update_background_job(req).await;
            info!("update log res: {:?}", res);
            let res = mt
                .get_background_job(GetBackgroundJobReq {
                    name: job_ident.clone(),
                })
                .await;
            info!("get log res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(
                BackgroundJobState::SUSPENDED,
                res.info.job_state,
                "first state is started"
            );
        }
        info!("--- list background jobs when their is 1 tasks");
        {
            let req = ListBackgroundJobsReq {
                tenant: tenant.to_string(),
            };

            let res = mt.list_background_jobs(req).await;
            assert!(res.is_ok());
            let resp = res.unwrap();
            assert_eq!(1, resp.len());
            assert_eq!(
                BackgroundJobState::SUSPENDED,
                resp[0].1.job_state,
                "first state is started"
            );
        }
        Ok(())
    }
}
