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

use common_datavalues::chrono::Utc;
use common_exception::Result;
use common_meta_api::ShareApi;
use common_meta_app::share::GrantShareObjectReq;
use common_meta_app::share::ShareNameIdent;
use common_storages_share::save_share_spec;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::share::GrantShareObjectPlan;

pub struct GrantShareObjectInterpreter {
    ctx: Arc<QueryContext>,
    plan: GrantShareObjectPlan,
}

impl GrantShareObjectInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: GrantShareObjectPlan) -> Result<Self> {
        Ok(GrantShareObjectInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for GrantShareObjectInterpreter {
    fn name(&self) -> &str {
        "GrantShareObjectInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let tenant = self.ctx.get_tenant();
        let user_mgr = self.ctx.get_user_manager();
        let meta_api = user_mgr.get_meta_store_client();
        let req = GrantShareObjectReq {
            share_name: ShareNameIdent {
                tenant,
                share_name: self.plan.share.clone(),
            },
            object: self.plan.object.clone(),
            privilege: self.plan.privilege,
            grant_on: Utc::now(),
        };
        let resp = meta_api.grant_share_object(req).await?;

        save_share_spec(
            resp.share_id,
            self.ctx.get_storage_operator()?,
            Some(resp.spec),
        )
        .await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
