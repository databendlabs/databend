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

use std::net::SocketAddr;
use std::sync::Arc;
use arrow_array::RecordBatch;
use futures_util::StreamExt;
use background_service::background_service::BackgroundServiceHandlerWrapper;
use background_service::BackgroundServiceHandler;
use common_base::base::GlobalInstance;
use common_catalog::table_context::TableContext;
use common_config::InnerConfig;
use databend_query::servers::flight_sql::flight_sql_service::FlightSqlServiceImpl;
use databend_query::servers::Server;
use databend_query::sessions::Session;
use common_exception::{ErrorCode, Result};
use common_expression::DataBlock;
use databend_query::interpreters::InterpreterFactory;
use databend_query::status;
use crate::background_service::session::create_session;

pub struct RealBackgroundService {
    conf: InnerConfig,
    executor: FlightSqlServiceImpl,
    session: Arc<Session>,
}
#[async_trait::async_trait]
impl Server for RealBackgroundService {
    #[async_backtrace::framed]
    async fn shutdown(&mut self, graceful: bool) {
        todo!()
    }
    #[async_backtrace::framed]
    async fn start(&mut self, listening: SocketAddr) -> common_exception::Result<SocketAddr> {
        todo!()
    }
}

#[async_trait::async_trait]
impl BackgroundServiceHandler for RealBackgroundService {

    async fn create_service(&self, conf: &InnerConfig) -> common_exception::Result<Box<dyn Server>> {
        let background_service_sql_executor = FlightSqlServiceImpl::create();
        let session = create_session().await?;

        Ok(Box::new(RealBackgroundService{
            conf: conf.clone(),
            executor: background_service_sql_executor,
            session
        }))
    }
}

impl RealBackgroundService {
    pub async fn init( conf: &InnerConfig) -> Result<()> {
        let background_service_sql_executor = FlightSqlServiceImpl::create();
        let session = create_session().await?;
        let rm = RealBackgroundService{
            conf: conf.clone(),
            executor: background_service_sql_executor,
            session
        };
        let wrapper = BackgroundServiceHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
    #[async_backtrace::framed]
    pub async fn execute_sql(
        &self,
        sql: &str,
    ) -> Result<RecordBatch> {
        let (plan, plan_extras) = self.executor.plan_sql( &self.session, sql).await?;
        let context = self.session
            .create_query_context()
            .await?;

        context.attach_query_str(plan.to_string(), plan_extras.statement.to_mask_sql());
        let interpreter = InterpreterFactory::get(context.clone(), &plan).await?;
        let data_schema = interpreter.schema();
        let mut blocks = interpreter.execute(context.clone()).await?;
        let mut result = vec![];
        while let Some(block) = blocks.next().await {
            let block = block?;
            result.push(block);
        }
        let record = DataBlock::concat(&result)?;
        let record = record.to_record_batch(data_schema.as_ref()).map_err(|e| ErrorCode::Internal(format!("{e:?}")))?;
        Ok(record.into())
    }
}
