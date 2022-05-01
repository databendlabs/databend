// Copyright 2022 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::ShowClusterInfoPlan;
use common_tracing::tracing;
use sqlparser::ast::Expr;
use sqlparser::ast::ObjectName;

use super::analyzer_expr::ExpressionAnalyzer;
use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfCreateTable;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowClusterInfo {
    pub table_name: ObjectName,
    pub cluster_key: Option<Vec<Expr>>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowClusterInfo {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let schema = Self::schema();
        let (db, table) = DfCreateTable::resolve_table(ctx.clone(), &self.table_name, "Table")?;
        let cluster_keys = match &self.cluster_key {
            Some(exprs) => {
                let mut keys = vec![];
                let expression_analyzer = ExpressionAnalyzer::create(ctx.clone());
                for expr in exprs.iter() {
                    let key = expression_analyzer.analyze(expr).await?;
                    keys.push(key);
                }
                Some(keys)
            }
            None => None,
        };
        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::ShowClusterInfo(ShowClusterInfoPlan {
                db,
                table,
                cluster_keys,
                schema,
            }),
        )))
    }
}

impl DfShowClusterInfo {
    fn schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("cluster_keys", Vu8::to_data_type()),
            DataField::new("total_block_count", u64::to_data_type()),
            // DataField::new("constant_block_count", u64::to_data_type()),
            // DataField::new("average_overlaps", f64::to_data_type()),
            // DataField::new("average_depth", f64::to_data_type()),
        ])
    }
}
