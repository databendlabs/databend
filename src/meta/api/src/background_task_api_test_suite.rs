use chrono::{DateTime, Utc};
use common_meta_app::background::{BackgroundTaskIdent, BackgroundTaskInfo, BackgroundTaskState, GetBackgroundTaskReq, ListBackgroundTasksReq, UpdateBackgroundTaskReq};
use common_meta_kvapi::kvapi;
use common_meta_types::MetaError;
use crate::background_task_api::BackgroundTaskApi;
use crate::SchemaApi;
use tracing::info;


fn new_background_task(state: BackgroundTaskState, created_at: DateTime<Utc>) -> BackgroundTaskInfo {
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

/// Test suite of `BackgroundTaskApi`.
///
/// It is not used by this crate, but is used by other crate that impl `ShareApi`,
/// to ensure an impl works as expected,
/// such as `meta/embedded` and `metasrv`.
#[derive(Copy, Clone)]
pub struct BackgroundTaskApiTestSuite {}

impl BackgroundTaskApiTestSuite {
    /// Test BackgroundTaskApi on a single node
    pub async fn test_single_node<B, MT>(b: B) -> anyhow::Result<()>
        where
            B: kvapi::ApiBuilder<MT>,
            MT: BackgroundTaskApi + kvapi::AsKVApi<Error = MetaError> + SchemaApi,
    {
        let suite = BackgroundTaskApiTestSuite {};

        suite.update_background_tasks(&b.build().await).await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn update_background_tasks<MT: BackgroundTaskApi + kvapi::AsKVApi<Error = MetaError>>(
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
            info!("show share res: {:?}", res);
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
                task_info: new_background_task( BackgroundTaskState::STARTED, create_on),
                expire_at: expire_at.timestamp() as u64,
            };

            let res = mt.update_background_task(req).await;
            info!("update log res: {:?}", res);
            let res = mt.get_background_task(GetBackgroundTaskReq {
                name: task_name.clone(),
            }).await;
            info!("get log res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(BackgroundTaskState::STARTED, res.task_info.unwrap().task_state, "first state is started");
        }
        {
            let req = UpdateBackgroundTaskReq {
                task_name: task_name.clone(),
                task_info: new_background_task( BackgroundTaskState::DONE, create_on),
                expire_at: expire_at.timestamp() as u64,
            };

            let res = mt.update_background_task(req).await;
            info!("update log res: {:?}", res);
            let res = mt.get_background_task(GetBackgroundTaskReq {

                name: task_name.clone(),
            }).await;
            info!("get log res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(BackgroundTaskState::DONE, res.task_info.unwrap().task_state, "first state is done");
        }
        {
            let req = ListBackgroundTasksReq {
                tenant: tenant.to_string(),
            };

            let res = mt.list_background_tasks(req).await;
            info!("update log res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.len(), "there is one task");
            assert_eq!(BackgroundTaskState::DONE, res[0].1.task_state, "first state is done");
        }
        Ok(())
    }


}
