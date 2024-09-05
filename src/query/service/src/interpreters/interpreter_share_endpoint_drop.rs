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

use databend_common_exception::Result;
use databend_common_meta_api::ShareApi;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sql::plans::DropShareEndpointPlan;

pub struct DropShareEndpointInterpreter {
    plan: DropShareEndpointPlan,
}

impl DropShareEndpointInterpreter {
    pub fn try_create(_ctx: Arc<QueryContext>, plan: DropShareEndpointPlan) -> Result<Self> {
        Ok(DropShareEndpointInterpreter { plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropShareEndpointInterpreter {
    fn name(&self) -> &str {
        "DropShareEndpointInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let _resp = meta_api
            .drop_share_endpoint(self.plan.clone().into())
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
