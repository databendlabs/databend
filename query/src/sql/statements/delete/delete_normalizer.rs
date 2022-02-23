//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_exception::Result;

use crate::sessions::QueryContext;
use crate::sql::statements::analyzer_expr::ExpressionAnalyzer;
use crate::sql::statements::query::QueryASTIR;
use crate::sql::statements::DfDeleteStatement;

pub struct DeleteNormalizer {
    query_ast_ir: QueryASTIR,
    expression_analyzer: ExpressionAnalyzer,
}

impl DeleteNormalizer {
    fn create(ctx: Arc<QueryContext>) -> DeleteNormalizer {
        DeleteNormalizer {
            expression_analyzer: ExpressionAnalyzer::create(ctx),
            query_ast_ir: QueryASTIR {
                filter_predicate: None,
                group_by_expressions: vec![],
                having_predicate: None,
                aggregate_expressions: vec![],
                order_by_expressions: vec![],
                projection_expressions: vec![],
                limit: None,
                offset: None,
            },
        }
    }

    pub async fn normalize(ctx: Arc<QueryContext>, v: &DfDeleteStatement) -> Result<QueryASTIR> {
        let query_normalizer = DeleteNormalizer::create(ctx);
        query_normalizer.transform(v).await
    }

    pub async fn transform(mut self, stmt: &DfDeleteStatement) -> Result<QueryASTIR> {
        if let Err(cause) = self.visit_filter(stmt).await {
            return Err(cause.add_message_back(" (while in analyze delete filter)"));
        }
        Ok(self.query_ast_ir)
    }

    async fn visit_filter(&mut self, query: &DfDeleteStatement) -> Result<()> {
        if let Some(predicate) = &query.selection {
            let analyzer = &self.expression_analyzer;
            self.query_ast_ir.filter_predicate = Some(analyzer.analyze(predicate).await?);
        }
        Ok(())
    }
}
