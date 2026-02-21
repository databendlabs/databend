// Copyright 2021 Datafuse Labs
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

use databend_common_ast::Span;
use databend_common_ast::ast::ExplainKind;
use databend_common_ast::ast::ExplainOption;
use databend_common_ast::ast::ExplainTraceOptions;
use databend_common_ast::ast::Statement;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::BindContext;
use crate::Binder;
use crate::plans::Plan;

/// Configuration for the EXPLAIN statement.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExplainConfig {
    pub verbose: bool,
    pub logical: bool,
    pub optimized: bool,
    pub decorrelated: bool,
}

impl ExplainConfig {
    fn validate(&self, span: Span, kind: &ExplainKind) -> Result<()> {
        if self.logical
            && !matches!(
                kind,
                ExplainKind::Plan
                    | ExplainKind::Raw
                    | ExplainKind::Optimized
                    | ExplainKind::Decorrelated
            )
        {
            return Err(ErrorCode::SyntaxException(
                "This EXPLAIN statement does not support LOGICAL options".to_string(),
            )
            .set_span(span));
        }

        if self.decorrelated && !matches!(kind, ExplainKind::Plan | ExplainKind::Decorrelated) {
            return Err(ErrorCode::SyntaxException(
                "This EXPLAIN statement does not support DECORRELATED options".to_string(),
            )
            .set_span(span));
        }

        if self.optimized && !matches!(kind, ExplainKind::Plan | ExplainKind::Optimized) {
            return Err(ErrorCode::SyntaxException(
                "This EXPLAIN statement does not support OPTIMIZED option".to_string(),
            )
            .set_span(span));
        }

        Ok(())
    }
}

#[derive(Default)]
struct ExplainConfigBuilder {
    verbose: bool,
    logical: bool,
    optimized: bool,
    decorrelated: bool,
}

impl ExplainConfigBuilder {
    pub fn add_option(mut self, option: ExplainOption) -> Self {
        match option {
            ExplainOption::Verbose => self.verbose = true,
            ExplainOption::Logical => self.logical = true,
            ExplainOption::Optimized => {
                self.logical = true;
                self.optimized = true;
            }
            ExplainOption::Decorrelated => {
                self.logical = true;
                self.decorrelated = true;
            }
        }

        self
    }

    pub fn build(self) -> ExplainConfig {
        ExplainConfig {
            verbose: self.verbose,
            logical: self.logical,
            optimized: self.optimized,
            decorrelated: self.decorrelated,
        }
    }
}

impl Binder {
    pub async fn bind_explain(
        &mut self,
        bind_context: &mut BindContext,
        kind: &ExplainKind,
        (span, options): &(Span, Vec<ExplainOption>),
        inner: &Statement,
    ) -> Result<Plan> {
        let mut builder = ExplainConfigBuilder::default();
        if let Statement::Explain { .. }
        | Statement::ExplainAnalyze { .. }
        | Statement::ReportIssue { .. } = inner
        {
            return Err(ErrorCode::SyntaxException("Invalid statement"));
        }

        for option in options {
            builder = builder.add_option(*option);
        }

        match kind {
            ExplainKind::Raw => {
                // Rewrite `EXPLAIN RAW` to `EXPLAIN(LOGICAL)`
                builder = builder.add_option(ExplainOption::Logical);
            }
            ExplainKind::Decorrelated => {
                // Rewrite `EXPLAIN DECORRELATED` to `EXPLAIN(LOGICAL, DECORRELATED)`
                builder = builder.add_option(ExplainOption::Decorrelated);
            }
            ExplainKind::Optimized => {
                // Rewrite `EXPLAIN OPTIMIZED` to `EXPLAIN(LOGICAL, OPTIMIZED)`
                builder = builder.add_option(ExplainOption::Optimized);
            }
            _ => {}
        };

        let config = builder.build();
        config.validate(span.to_owned(), kind)?;

        match kind {
            ExplainKind::Ast(formatted_stmt) => Ok(Plan::ExplainAst {
                formatted_string: formatted_stmt.clone(),
            }),
            ExplainKind::Syntax(formatted_sql) => Ok(Plan::ExplainSyntax {
                formatted_sql: formatted_sql.clone(),
            }),
            ExplainKind::Raw | ExplainKind::Decorrelated | ExplainKind::Optimized => {
                Ok(Plan::Explain {
                    kind: ExplainKind::Plan,
                    config,
                    plan: Box::new(self.bind_statement(bind_context, inner).await?),
                })
            }
            ExplainKind::Perf => Ok(Plan::ExplainPerf {
                sql: inner.to_string(),
            }),
            _ => Ok(Plan::Explain {
                kind: kind.clone(),
                config,
                plan: Box::new(self.bind_statement(bind_context, inner).await?),
            }),
        }
    }

    pub async fn bind_explain_trace(
        &mut self,
        _bind_context: &mut BindContext,
        options: &ExplainTraceOptions,
        inner: &Statement,
    ) -> Result<Plan> {
        if let Statement::Explain { .. }
        | Statement::ExplainTrace { .. }
        | Statement::ExplainAnalyze { .. }
        | Statement::ReportIssue { .. } = inner
        {
            return Err(ErrorCode::SyntaxException("Invalid statement"));
        }

        Ok(Plan::ExplainTrace {
            sql: inner.to_string(),
            options: options.clone(),
        })
    }
}
