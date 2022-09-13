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

use common_datablocks::DataBlock;
use common_datavalues::prelude::DataSchemaRef;
use common_datavalues::prelude::DataSchemaRefExt;
use common_datavalues::prelude::Series;
use common_datavalues::SeriesFrom;
use common_exception::Result;
use common_meta_api::ShareApi;
use common_meta_app::share::GetObjectGrantPrivilegesReq;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::share::ShowObjectGrantPrivilegesPlan;

pub struct ShowObjectGrantPrivilegesInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowObjectGrantPrivilegesPlan,
}

impl ShowObjectGrantPrivilegesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowObjectGrantPrivilegesPlan) -> Result<Self> {
        Ok(ShowObjectGrantPrivilegesInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowObjectGrantPrivilegesInterpreter {
    fn name(&self) -> &str {
        "ShowObjectGrantPrivilegesInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let user_mgr = self.ctx.get_user_manager();
        let meta_api = user_mgr.get_meta_store_client();
        let req = GetObjectGrantPrivilegesReq {
            tenant: self.ctx.get_tenant(),
            object: self.plan.object.clone(),
        };
        let resp = meta_api.get_grant_privileges_of_object(req).await?;
        if resp.privileges.is_empty() {
            return Ok(PipelineBuildResult::create());
        }
        let desc_schema = self.plan.schema();
        let mut share_names: Vec<String> = vec![];
        let mut privileges: Vec<String> = vec![];
        let mut created_ons: Vec<String> = vec![];

        for privilege in resp.privileges {
            share_names.push(privilege.share_name);
            privileges.push(privilege.privileges.to_string());
            created_ons.push(privilege.grant_on.to_string());
        }

        PipelineBuildResult::from_blocks(vec![
            DataBlock::create(desc_schema.clone(), vec![
                Series::from_data(created_ons),
                Series::from_data(privileges),
                Series::from_data(share_names),
            ])
        ])
    }
}
