// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::format;
use std::sync::Arc;
use std::vec;

use arrow_array::RecordBatch;
use chrono::DateTime;
use chrono::Utc;
use common_base::base::GlobalInstance;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::UserInfo;
use common_storages_fuse::FuseTable;
use databend_query::interpreters::InterpreterFactory;
use databend_query::servers::Server;
use databend_query::sessions::QueryContext;

#[async_trait::async_trait]
pub trait BackgroundServiceHandler: Sync + Send {
    async fn execute_sql(&self, sql: &str) -> Result<Option<RecordBatch>>;
}

pub struct BackgroundServiceHandlerWrapper {
    pub handler: Box<dyn BackgroundServiceHandler>,
}

impl BackgroundServiceHandlerWrapper {
    pub fn new(handler: Box<dyn BackgroundServiceHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn execute_sql(&self, sql: &str) -> Result<Option<RecordBatch>> {
        self.handler.execute_sql(sql).await
    }
    // #[async_backtrace::framed]
    // pub async fn create(
    //     &self, conf: &common_config::InnerConfig
    // ) -> Result<Box<dyn Server>> {
    //     self.handler
    //         .create_service(conf)
    //         .await
    // }
}

pub fn get_background_service_handler() -> Arc<BackgroundServiceHandlerWrapper> {
    GlobalInstance::get()
}
