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

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::PrincipalIdentity;
use common_planners::ShowGrantsPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;

pub struct ShowGrantsInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowGrantsPlan,
}

impl ShowGrantsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowGrantsPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ShowGrantsInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowGrantsInterpreter {
    fn name(&self) -> &str {
        "ShowGrantsInterpreter"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let schema = DataSchemaRefExt::create(vec![DataField::new("Grants", Vu8::to_data_type())]);
        let tenant = self.ctx.get_tenant();
        let user_mgr = self.ctx.get_user_manager();
        let role_cache_mgr = self.ctx.get_role_cache_manager();

        // TODO: add permission check on reading user grants
        let (identity, grant_set) = match self.plan.principal {
            None => {
                let user = self.ctx.get_current_user()?;
                (user.identity().to_string(), user.grants)
            }
            Some(ref principal) => match principal {
                PrincipalIdentity::User(user) => {
                    let user = user_mgr.get_user(&tenant, user.clone()).await?;
                    (user.identity().to_string(), user.grants)
                }
                PrincipalIdentity::Role(role) => {
                    let role = user_mgr.get_role(&tenant, role.clone()).await?;
                    (format!("'{}'", role.identity()), role.grants)
                }
            },
        };
        let grant_list = role_cache_mgr
            .find_related_roles(&tenant, &grant_set.roles())
            .await?
            .into_iter()
            .map(|role| role.grants)
            .fold(grant_set, |a, b| a | b)
            .entries()
            .iter()
            .map(|e| format!("{} TO {}", e, identity).into_bytes())
            .collect::<Vec<_>>();

        let block = DataBlock::create(schema.clone(), vec![Series::from_data(grant_list)]);
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }
}
