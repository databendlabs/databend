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
use common_base::base::{GlobalInstance, tokio};
use common_catalog::table_context::TableContext;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_meta_app::principal::{UserIdentity};
use common_meta_app::principal::UserInfo;
use databend_query::interpreters::InterpreterFactory;
use databend_query::servers::flight_sql::flight_sql_service::FlightSqlServiceImpl;
use databend_query::sessions::{Session, SessionManager, SessionType};
use databend_query::sql::Planner;
use futures_util::StreamExt;
use tracing::{error, info, Instrument};
use common_base::runtime::TrySpawn;
use common_license::license_manager::get_license_manager;
use common_meta_store::MetaStore;
use common_users::{BUILTIN_ROLE_ACCOUNT_ADMIN, UserApiProvider};
use databend_query::servers::ShutdownHandle;
use crate::background_service::{CompactionJob, JobScheduler};

use crate::background_service::session::create_session;

pub struct RealBackgroundService {
    conf: InnerConfig,
    session: Arc<Session>,
    meta: Arc<MetaStore>,
    user: UserIdentity,
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
        let mut stream = interpreter.execute(ctx.clone()).await?;
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
    async fn start(&self, shutdown_handler: &mut ShutdownHandle) -> Result<()> {
        let settings = SessionManager::create(&conf).create_session(SessionType::Dummy).await.unwrap().get_settings();
        // check for valid license
        let enterprise_enabled = get_license_manager()
            .manager
            .check_enterprise_enabled(
                &settings,
                "background_service".to_string(),
            )
            .is_ok();
        if !enterprise_enabled {
            panic!("Background service is only available in enterprise edition.");
        }
        let scheduler = self.scheduler.clone();
        scheduler.start(shutdown_handler).await?;
        info!("all one shot jobs finished");
        Ok(())
    }

}

impl RealBackgroundService {
    pub async fn new(conf: &InnerConfig) -> Result<Self> {
        let session = create_session().await?;
        let user = UserInfo::new_no_auth(format!("{}-{}-background-svc", conf.query.tenant_id.clone(), conf.query.cluster_id.clone()).as_str(), "0.0.0.0");
        session.set_authed_user(user.clone(), Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string())).await?;
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let mut scheduler = JobScheduler::new();
        let finish_tx = scheduler.finish_tx.clone();
        let job = CompactionJob::create(&conf, format!("{}-{}-compactor-job", conf.query.tenant_id, conf.query.cluster_id), finish_tx).await;
        let scheduler = scheduler.add_one_shot_job(job.clone());
        let rm = RealBackgroundService {
            conf: conf.clone(),
            session: session.clone(),
            meta: meta_api,
            user: user.identity(),
            scheduler: Arc::new(scheduler),
        };
        Ok(rm)
    }
    pub async fn init(conf: &InnerConfig) -> Result<()> {
        let rm = RealBackgroundService::new(conf).await?;
        let wrapper = BackgroundServiceHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }

}
