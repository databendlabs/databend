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

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;

#[async_trait::async_trait]
pub trait FailSafeHandler: Sync + Send {
    async fn recover_table_data(&self, table_info: TableInfo) -> Result<()>;
}

pub struct FailSafeHandlerWrapper {
    handler: Box<dyn FailSafeHandler>,
}

impl FailSafeHandlerWrapper {
    pub fn new(handler: Box<dyn FailSafeHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn recover(&self, table_info: TableInfo) -> Result<()> {
        self.handler.recover_table_data(table_info).await
    }
}

pub fn get_fail_safe_handler() -> Arc<FailSafeHandlerWrapper> {
    GlobalInstance::get()
}
