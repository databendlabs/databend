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
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_meta_api::ShareApi;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sql::plans::share::ShowShareEndpointPlan;

pub struct ShowShareEndpointInterpreter {
    plan: ShowShareEndpointPlan,
}

impl ShowShareEndpointInterpreter {
    pub fn try_create(_ctx: Arc<QueryContext>, plan: ShowShareEndpointPlan) -> Result<Self> {
        Ok(ShowShareEndpointInterpreter { plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowShareEndpointInterpreter {
    fn name(&self) -> &str {
        "ShowShareEndpointInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let resp = meta_api
            .get_share_endpoint(self.plan.clone().into())
            .await?;

        let mut endpoints: Vec<String> = vec![];
        let mut urls: Vec<String> = vec![];
        let mut credentials: Vec<String> = vec![];
        let mut args: Vec<String> = vec![];
        let mut comments: Vec<String> = vec![];
        for (endpoint, meta) in resp.share_endpoint_meta_vec {
            endpoints.push(endpoint.name().to_string());
            urls.push(meta.url.clone());
            if let Some(credential) = &meta.credential {
                credentials.push(format!("{}", credential));
            } else {
                credentials.push("{}".to_string());
            }
            args.push(format!("{:?}", meta.args));
            comments.push(meta.comment.unwrap_or_default());
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(endpoints),
            StringType::from_data(urls),
            StringType::from_data(credentials),
            StringType::from_data(args),
            StringType::from_data(comments),
        ])])
    }
}
