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

use arrow_array::RecordBatch;
use background_service::background_service::BackgroundServiceHandlerWrapper;
use background_service::BackgroundServiceHandler;
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::base::tokio::sync::Mutex;
use common_base::base::GlobalInstance;
use common_catalog::table_context::TableContext;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_license::license_manager::get_license_manager;
use common_meta_api::BackgroundApi;
use common_meta_app::background::BackgroundJobIdent;
use common_meta_app::background::BackgroundJobInfo;
use common_meta_app::background::BackgroundJobState;
use common_meta_app::background::CreateBackgroundJobReq;
use common_meta_app::background::GetBackgroundJobReq;
use common_meta_app::background::UpdateBackgroundJobParamsReq;
use common_meta_app::background::UpdateBackgroundJobStatusReq;
use common_meta_app::principal::UserIdentity;
use common_meta_app::principal::UserInfo;
use common_meta_store::MetaStore;
use common_users::UserApiProvider;
use common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::Session;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sql::Planner;
use futures_util::StreamExt;
use tracing::error;
use tracing::info;

use crate::background_service::session::create_session;
use crate::background_service::CompactionJob;
use crate::background_service::JobScheduler;

pub struct RealBackgroundService {
    conf: InnerConfig,
    session: Arc<Session>,
    scheduler: Arc<JobScheduler>,
}

#[async_trait::async_trait]
impl BackgroundServiceHandler for RealBackgroundService {
    #[async_backtrace::framed]
    async fn execute_sql(&self, sql: &str) -> Result<Option<RecordBatch>> {
        let ctx = self.session.create_query_context().await?;
        let mut planner = Planner::new(ctx.clone());
        let (plan, plan_extras) = planner.plan_sql(sql).await?;
        ctx.attach_query_str(plan.to_string(), plan_extras.statement.to_mask_sql());
        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let data_schema = interpreter.schema();
        let stream = interpreter.execute(ctx.clone()).await?;
        let blocks = stream.map(|v| v).collect::<Vec<_>>().await;

        let mut result = vec![];
        for block in blocks {
            match block {
                Ok(block) => {
                    result.push(block);
                }
                Err(e) => {
                    error!("execute sql error: {:?}", e);
                }
            }
        }
        if result.is_empty() {
            return Ok(None);
        }
        let record = DataBlock::concat(&result)?;
        let record = record
            .to_record_batch(data_schema.as_ref())
            .map_err(|e| ErrorCode::Internal(format!("{e:?}")))?;
        Ok(Some(record))
    }

    #[async_backtrace::framed]
    async fn execute_scheduled_job(&self, name: String) -> Result<()> {
        self.check_license().await?;
        return if let Some(job) = self.scheduler.get_scheduled_job(name.as_str()) {
            JobScheduler::check_and_run_job(job, true).await
        } else {
            Err(ErrorCode::UnknownBackgroundJob(format!(
                "background job {} not found",
                name
            )))
        };
    }
    #[async_backtrace::framed]
    async fn start(&self) -> Result<()> {
        let enterprise_enabled = self.check_license().await.is_ok();
        if !enterprise_enabled {
            panic!("Background service is only available in enterprise edition.");
        }
        let scheduler = self.scheduler.clone();
        scheduler.start().await?;
        info!("all one shot jobs finished");
        Ok(())
    }
}

impl RealBackgroundService {
    pub async fn new(conf: &InnerConfig) -> Result<Option<Self>> {
        if !conf.background.enable {
            return Ok(None);
        }
        let session = create_session().await?;
        let user = UserInfo::new_no_auth(
            format!(
                "{}-{}-background-svc",
                conf.query.tenant_id.clone(),
                conf.query.cluster_id.clone()
            )
            .as_str(),
            "0.0.0.0",
        );
        session
            .set_authed_user(user.clone(), Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()))
            .await?;
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let mut scheduler = JobScheduler::new();

        let compactor_job = RealBackgroundService::get_compactor_job(
            meta_api.clone(),
            conf,
            &user.identity(),
            scheduler.finish_tx.clone(),
        )
        .await?;
        scheduler.add_job(compactor_job)?;

        let rm = RealBackgroundService {
            conf: conf.clone(),
            session: session.clone(),
            scheduler: Arc::new(scheduler),
        };
        Ok(Some(rm))
    }

    async fn get_compactor_job(
        meta: Arc<MetaStore>,
        conf: &InnerConfig,
        creator: &UserIdentity,
        finish_tx: Arc<Mutex<Sender<u64>>>,
    ) -> Result<CompactionJob> {
        // background compactor job
        let name = format!(
            "{}-{}-compactor-job",
            conf.query.tenant_id, conf.query.cluster_id
        );
        let params = conf.background.compaction.params.clone();
        let info = BackgroundJobInfo::new_compactor_job(params.clone(), creator.clone());

        let id = BackgroundJobIdent {
            tenant: conf.query.tenant_id.clone(),
            name: name.clone(),
        };
        meta.create_background_job(CreateBackgroundJobReq {
            if_not_exists: true,
            job_name: id.clone(),
            job_info: info,
        })
        .await?;
        Self::update_compaction_job_params(meta.clone(), &id, conf).await?;
        let info = Self::suspend_job(meta.clone(), &id, false).await?;

        let job = CompactionJob::create(conf, name, info, finish_tx).await;
        Ok(job)
    }

    async fn update_compaction_job_params(
        meta: Arc<MetaStore>,
        id: &BackgroundJobIdent,
        conf: &InnerConfig,
    ) -> Result<()> {
        // create job if not exist
        let info = meta
            .get_background_job(GetBackgroundJobReq { name: id.clone() })
            .await?
            .info;
        if info.job_params.is_some() {
            meta.update_background_job_params(UpdateBackgroundJobParamsReq {
                job_name: id.clone(),
                params: conf.background.compaction.params.clone(),
            })
            .await?;
        }
        Ok(())
    }
    async fn suspend_job(
        meta: Arc<MetaStore>,
        id: &BackgroundJobIdent,
        suspend: bool,
    ) -> Result<BackgroundJobInfo> {
        // create job if not exist
        let info = meta
            .get_background_job(GetBackgroundJobReq { name: id.clone() })
            .await?
            .info;
        if info.job_status.is_some() {
            let mut status = info.job_status.clone().unwrap();
            if suspend {
                status.job_state = BackgroundJobState::SUSPENDED;
            } else {
                status.job_state = BackgroundJobState::RUNNING;
            }
            meta.update_background_job_status(UpdateBackgroundJobStatusReq {
                job_name: id.clone(),
                status,
            })
            .await?;
        }
        let info = meta
            .get_background_job(GetBackgroundJobReq { name: id.clone() })
            .await?
            .info;
        Ok(info)
    }

    pub async fn init(conf: &InnerConfig) -> Result<()> {
        let rm = RealBackgroundService::new(conf).await?;
        if let Some(rm) = rm {
            let wrapper = BackgroundServiceHandlerWrapper::new(Box::new(rm));
            GlobalInstance::set(Arc::new(wrapper));
        }
        Ok(())
    }

    async fn check_license(&self) -> Result<()> {
        let settings = SessionManager::create(&self.conf)
            .create_session(SessionType::Dummy)
            .await
            .unwrap()
            .get_settings();
        // check for valid license
        get_license_manager().manager.check_enterprise_enabled(
            &settings,
            self.conf.query.tenant_id.clone(),
            "background_service".to_string(),
        )
    }
}
