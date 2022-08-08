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

use common_exception::Result;
use common_planners::Expression;

use crate::analyzer_expr_sync::ExpressionSyncAnalyzer;
use crate::ExprParser;

pub struct ExpressionParser;

impl ExpressionParser {
    pub fn parse_expr(expr: &str) -> Result<Expression> {
        let expr = ExprParser::parse_expr(expr)?;
        let analyzer = ExpressionSyncAnalyzer::create();
        analyzer.analyze(&expr)
    }

    pub fn parse_exprs(expr: &str) -> Result<Vec<Expression>> {
        let exprs = ExprParser::parse_exprs(expr)?;
        let analyzer = ExpressionSyncAnalyzer::create();

        let results = exprs
            .iter()
            .map(|expr| analyzer.analyze(expr))
            .collect::<Result<Vec<_>>>();
        results
    }
}
