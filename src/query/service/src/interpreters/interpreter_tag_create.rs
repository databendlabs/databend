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

use chrono::Utc;
use databend_common_exception::Result;
use databend_common_meta_api::tag_api::TagApi;
use databend_common_meta_app::schema::CreateTagReq;
use databend_common_meta_app::schema::TagMeta;
use databend_common_meta_app::schema::TagNameIdent;
use databend_common_sql::plans::CreateTagPlan;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct CreateTagInterpreter {
    _ctx: Arc<QueryContext>,
    plan: CreateTagPlan,
}

impl CreateTagInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateTagPlan) -> Result<Self> {
        Ok(Self { _ctx: ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateTagInterpreter {
    fn name(&self) -> &str {
        "CreateTagInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let meta_client = UserApiProvider::instance().get_meta_store_client();
        let comment = self.plan.comment.clone().unwrap_or_default();
        let meta = TagMeta {
            allowed_values: self.plan.allowed_values.clone(),
            comment,
            created_on: Utc::now(),
            updated_on: None,
        };
        let req = CreateTagReq {
            create_option: self.plan.create_option,
            name_ident: TagNameIdent::new(&self.plan.tenant, &self.plan.name),
            meta,
        };
        meta_client.create_tag(req).await??;
        Ok(PipelineBuildResult::create())
    }
}
