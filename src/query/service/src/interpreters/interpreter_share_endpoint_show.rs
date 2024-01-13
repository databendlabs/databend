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

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let resp = meta_api
            .get_share_endpoint(self.plan.clone().into())
            .await?;

        let mut endpoints: Vec<Vec<u8>> = vec![];
        let mut urls: Vec<Vec<u8>> = vec![];
        let mut to_tenants: Vec<Vec<u8>> = vec![];
        let mut args: Vec<Vec<u8>> = vec![];
        let mut comments: Vec<Vec<u8>> = vec![];
        let mut created_on_vec: Vec<Vec<u8>> = vec![];
        for (endpoint, meta) in resp.share_endpoint_meta_vec {
            endpoints.push(endpoint.endpoint.clone().as_bytes().to_vec());
            urls.push(meta.url.clone().as_bytes().to_vec());
            to_tenants.push(meta.tenant.clone().as_bytes().to_vec());
            args.push(format!("{:?}", meta.args).as_bytes().to_vec());
            comments.push(meta.comment.unwrap_or_default().as_bytes().to_vec());
            created_on_vec.push(meta.create_on.to_string().as_bytes().to_vec());
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(endpoints),
            StringType::from_data(urls),
            StringType::from_data(to_tenants),
            StringType::from_data(args),
            StringType::from_data(comments),
            StringType::from_data(created_on_vec),
        ])])
    }
}
