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
use common_planners::PlanNode;
use common_planners::PlanShowKind;
use common_planners::ShowFunctionsPlan;
use common_planners::ShowPlan;
use common_tracing::tracing;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfShowKind;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowFunctions {
    pub kind: DfShowKind,
}

impl DfShowFunctions {
    pub fn create(kind: DfShowKind) -> DfShowFunctions {
        DfShowFunctions { kind }
    }
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowFunctions {
    #[tracing::instrument(level = "debug", skip(self, _ctx), fields(ctx.id = _ctx.get_id().as_str()))]
    async fn analyze(&self, _ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let mut kind = PlanShowKind::All;
        match &self.kind {
            DfShowKind::All => {}
            DfShowKind::Like(v) => {
                kind = PlanShowKind::Like(format!("{}", v));
            }
            DfShowKind::Where(v) => {
                kind = PlanShowKind::Where(format!("{}", v));
            }
            DfShowKind::FromOrIn(v) => {
                kind = PlanShowKind::FromOrIn(v.0[0].value.clone());
            }
        }

        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::Show(
            ShowPlan::ShowFunctions(ShowFunctionsPlan { kind }),
        ))))
    }
}
