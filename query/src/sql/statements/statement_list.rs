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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::ListPlan;
use common_planners::PlanNode;

use super::parse_stage_location;
use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfList {
    pub location: String,
    pub pattern: String,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfList {
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        if !self.location.starts_with('@') {
            return Err(ErrorCode::SyntaxException(
                "List stage uri must be started with @, for example: '@stage_name[/<path>/]'",
            ));
        }
        let (stage, path) = parse_stage_location(&ctx, &self.location).await?;

        let plan_node = ListPlan {
            path,
            stage,
            pattern: self.pattern.clone(),
        };

        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::List(
            plan_node,
        ))))
    }
}
