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
use common_planners::ShowPlan;
use common_planners::ShowTabStatPlan;
use common_tracing::tracing;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfShowKind;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowTabStat {
    pub kind: DfShowKind,
    pub fromdb: Option<String>,
}

impl DfShowTabStat {
    pub fn create(kind: DfShowKind, fromdb: Option<String>) -> DfShowTabStat {
        DfShowTabStat { kind, fromdb }
    }
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowTabStat {
    #[tracing::instrument(level = "debug", skip(self, _ctx), fields(ctx.id = _ctx.get_id().as_str()))]
    async fn analyze(&self, _ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let mut kind = PlanShowKind::All;
        let fromdb = self.fromdb.clone();
        match &self.kind {
            DfShowKind::All => {}
            DfShowKind::Like(v) => {
                kind = PlanShowKind::Like(v.to_string());
            }
            DfShowKind::Where(v) => {
                kind = PlanShowKind::Where(v.to_string());
            }
        }

        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::Show(
            ShowPlan::ShowTabStat(ShowTabStatPlan { kind, fromdb }),
        ))))
    }
}
