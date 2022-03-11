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
use common_planners::KillPlan;
use common_planners::PlanNode;
use common_tracing::tracing;
use sqlparser::ast::Ident;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfKillStatement {
    pub object_id: Ident,
    pub kill_query: bool,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfKillStatement {
    #[tracing::instrument(level = "debug", skip(self, _ctx), fields(ctx.id = _ctx.get_id().as_str()))]
    async fn analyze(&self, _ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let id = self.object_id.value.clone();
        let kill_connection = !self.kill_query;
        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::Kill(
            KillPlan {
                id,
                kill_connection,
            },
        ))))
    }
}
