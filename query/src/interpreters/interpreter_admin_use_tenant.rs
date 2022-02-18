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

use common_datavalues::DataSchema;
use common_exception::Result;
use common_planners::AdminUseTenantPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct UseTenantInterpreter {
    ctx: Arc<QueryContext>,
    plan: AdminUseTenantPlan,
}

impl UseTenantInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AdminUseTenantPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(UseTenantInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for UseTenantInterpreter {
    fn name(&self) -> &str {
        "UseTenantInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        tracing::info!("SUDO USE tenant:{}", self.plan.tenant);

        self.ctx
            .set_current_tenant(self.plan.tenant.clone())
            .await?;
        let schema = Arc::new(DataSchema::empty());
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![])))
    }
}
