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
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::StringType;
use databend_common_sql::plans::ShowPublicKeysPlan;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

#[derive(Debug)]
pub struct ShowPublicKeysInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowPublicKeysPlan,
}

impl ShowPublicKeysInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowPublicKeysPlan) -> Result<Self> {
        Ok(ShowPublicKeysInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowPublicKeysInterpreter {
    fn name(&self) -> &str {
        "ShowPublicKeysInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();
        let user = user_mgr.get_user(&tenant, self.plan.user.clone()).await?;

        let keys = user.auth_info.get_public_keys();
        let mut fingerprints = Vec::with_capacity(keys.len());
        let mut labels = Vec::with_capacity(keys.len());
        let mut created_ats = Vec::with_capacity(keys.len());

        for k in keys {
            fingerprints.push(k.fingerprint()?);
            labels.push(k.label.clone());
            created_ats.push(
                chrono::DateTime::from_timestamp(k.created_at, 0)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default(),
            );
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(fingerprints),
            StringType::from_data(labels),
            StringType::from_data(created_ats),
        ])])
    }
}
