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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::tag_api::TagApi;
use databend_common_meta_app::schema::TagNameIdent;
use databend_common_sql::plans::DropTagPlan;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct DropTagInterpreter {
    _ctx: Arc<QueryContext>,
    plan: DropTagPlan,
}

impl DropTagInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTagPlan) -> Result<Self> {
        Ok(Self { _ctx: ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTagInterpreter {
    fn name(&self) -> &str {
        "DropTagInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let meta_client = UserApiProvider::instance().get_meta_store_client();
        match meta_client
            .drop_tag(TagNameIdent::new(&self.plan.tenant, &self.plan.name))
            .await?
        {
            Ok(Some(_)) => Ok(PipelineBuildResult::create()),
            Ok(None) => {
                if self.plan.if_exists {
                    Ok(PipelineBuildResult::create())
                } else {
                    Err(ErrorCode::UnknownTag(format!(
                        "Tag '{}' does not exist",
                        self.plan.name
                    )))
                }
            }
            Err(e) => Err(e.into()),
        }
    }
}
