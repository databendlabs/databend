// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;
use common_planners::KillPlan;
use common_planners::PlanNode;
use sqlparser::ast::Ident;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfKillStatement {
    pub object_id: Ident,
    pub kill_query: bool,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfKillStatement {
    async fn analyze(&self, _ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let id = self.object_id.value.clone();
        let kill_connection = !self.kill_query;
        Ok(AnalyzedResult::SimpleQuery(PlanNode::Kill(KillPlan {
            id,
            kill_connection,
        })))
    }
}
