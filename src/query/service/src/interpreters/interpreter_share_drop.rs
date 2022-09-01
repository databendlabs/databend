// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;
use common_meta_api::ShareApi;
use common_storages_share::ModShareSpec;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::share::DropSharePlan;

pub struct DropShareInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropSharePlan,
}

impl DropShareInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropSharePlan) -> Result<Self> {
        Ok(DropShareInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropShareInterpreter {
    fn name(&self) -> &str {
        "DropShareInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let user_mgr = self.ctx.get_user_manager();
        let meta_api = user_mgr.get_meta_store_client();
        let resp = meta_api.drop_share(self.plan.clone().into()).await?;

        if let Some(share_id) = resp.share_id {
            let mod_spec = ModShareSpec::DropShare {};
            mod_spec
                .save(
                    self.ctx.get_tenant(),
                    share_id,
                    self.ctx.get_storage_operator()?,
                )
                .await?;
        }

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
