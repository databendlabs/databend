// Copyright 2021 Datafuse Labs.
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
use common_meta_types::StageStorage;
use common_meta_types::UserStageInfo;
use common_planners::CopyPlan;
use common_streams::SendableDataBlockStream;
use opendal::credential::Credential;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

#[allow(dead_code)]
pub struct CopyInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyPlan,
}

impl CopyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CopyInterpreter { ctx, plan }))
    }

    async fn get_dal(&self, stage: UserStageInfo) -> Result<DalOperator> {
        match stage.stage_params.storage {
            StageStorage::S3(s3) => {
                let key_id = s3.credentials_aws_key_id;
                let secret_key = s3.credentials_aws_secret_key;
                let credential = Credential::hmac(&key_id, &secret_key);

                let mut builder = opendal::services::s3::Backend::build();
                Ok(opendal::Operator::new(
                    builder
                        .region(&conf.region)
                        .endpoint(&conf.endpoint_url)
                        .bucket(&conf.bucket)
                        .credential(credential)
                        .finish()
                        .await
                        .unwrap(),
                ))
            }
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyInterpreter {
    fn name(&self) -> &str {
        "CopyInterpreter"
    }

    async fn execute(
        &self,
        mut _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let stage_info = self.plan.stage_plan.stage_info.clone();
        match stage_info.stage_params.storage {
            StageStorage::S3(s3) => {}
        }
    }
}
