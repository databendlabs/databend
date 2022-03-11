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
use common_planners::ExplainType;
use common_tracing::tracing;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfQueryStatement;
use crate::sql::statements::QueryAnalyzeState;
use crate::sql::DfStatement;

#[derive(Debug, Clone, PartialEq)]
pub struct DfExplain {
    pub typ: ExplainType,
    pub statement: Box<DfStatement>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfExplain {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        match self.statement.as_ref() {
            DfStatement::Query(v) => {
                let explain_type = self.typ;
                let explain_query_state = Self::analyze_explain(ctx, v).await?;
                Ok(AnalyzedResult::ExplainQuery((
                    explain_type,
                    explain_query_state,
                )))
            }
            _ => Err(ErrorCode::SyntaxException("Only support EXPLAIN SELECT")),
        }
    }
}

impl DfExplain {
    async fn analyze_explain(
        ctx: Arc<QueryContext>,
        v: &DfQueryStatement,
    ) -> Result<Box<QueryAnalyzeState>> {
        match v.analyze(ctx).await? {
            AnalyzedResult::SelectQuery(v) => Ok(v),
            _ => Err(ErrorCode::LogicalError(
                "Logical error: analyze select must be return select query analyze result.",
            )),
        }
    }
}
