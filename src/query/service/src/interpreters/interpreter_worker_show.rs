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

use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb::ListWorkersRequest;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::StringType;
use databend_common_storages_fuse::TableContext;
use serde_json::to_string;

use crate::interpreters::Interpreter;
use crate::interpreters::common::get_worker_client_config;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct ShowWorkersInterpreter {
    ctx: Arc<QueryContext>,
}

impl ShowWorkersInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(ShowWorkersInterpreter { ctx })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowWorkersInterpreter {
    fn name(&self) -> &str {
        "ShowWorkersInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let config = GlobalConfig::instance();
        if config
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot show workers without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let worker_client = cloud_api.get_worker_client();
        let req = ListWorkersRequest {
            tenant_id: self.ctx.get_tenant().tenant_name().to_string(),
        };
        let config = get_worker_client_config(self.ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config);
        let resp = worker_client.list_workers(req).await?;
        let mut names = Vec::new();
        let mut tags = Vec::new();
        let mut options = Vec::new();
        let mut created_at = Vec::new();
        let mut updated_at = Vec::new();
        for worker in resp.workers {
            names.push(worker.name);
            tags.push(to_string(&worker.tags).unwrap_or_default());
            options.push(to_string(&worker.options).unwrap_or_default());
            created_at.push(worker.created_at);
            updated_at.push(worker.updated_at);
        }
        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(tags),
            StringType::from_data(options),
            StringType::from_data(created_at),
            StringType::from_data(updated_at),
        ]);
        PipelineBuildResult::from_blocks(vec![block])
    }
}
