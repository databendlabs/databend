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

use databend_common_ast::ast::ExplainKind;
use databend_common_ast::ast::ExplainOption;
use databend_common_ast::ast::Statement;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::plans::Plan;
use crate::BindContext;
use crate::Binder;

/// Configuration for the EXPLAIN statement.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExplainConfig {
    pub verbose: bool,
    pub logical: bool,
    pub optimized: bool,
}

struct ExplainConfigBuilder {
    verbose: bool,
    logical: bool,
    optimized: bool,
}

impl ExplainConfigBuilder {
    pub fn new() -> Self {
        ExplainConfigBuilder {
            verbose: false,
            logical: false,
            optimized: false,
        }
    }

    pub fn add_option(mut self, option: &ExplainOption) -> Self {
        match option {
            ExplainOption::Verbose => self.verbose = true,
            ExplainOption::Logical => self.logical = true,
            ExplainOption::Optimized => {
                self.logical = true;
                self.optimized = true;
            }
        }

        self
    }

    pub fn build(self) -> ExplainConfig {
        ExplainConfig {
            verbose: self.verbose,
            logical: self.logical,
            optimized: self.optimized,
        }
    }
}

impl Binder {
    pub async fn bind_explain(
        &mut self,
        bind_context: &mut BindContext,
        kind: &ExplainKind,
        options: &[ExplainOption],
        inner: &Statement,
    ) -> Result<Plan> {
        let mut builder = ExplainConfigBuilder::new();

        // Rewrite `EXPLAIN RAW` to `EXPLAIN(LOGICAL)`
        if matches!(kind, ExplainKind::Raw) {
            builder = builder.add_option(&ExplainOption::Logical);
        }

        // Rewrite `EXPLAIN OPTIMIZED` to `EXPLAIN(LOGICAL, OPTIMIZED)`
        if matches!(kind, ExplainKind::Optimized) {
            builder = builder.add_option(&ExplainOption::Logical);
            builder = builder.add_option(&ExplainOption::Optimized);
        }

        for option in options {
            builder = builder.add_option(option);
        }

        let config = builder.build();

        // Validate the configuration
        validate_explain_config(kind, &config)?;

        let plan = match kind {
            ExplainKind::Ast(formatted_stmt) => Plan::ExplainAst {
                formatted_string: formatted_stmt.clone(),
            },
            ExplainKind::Syntax(formatted_sql) => Plan::ExplainSyntax {
                formatted_sql: formatted_sql.clone(),
            },
            _ => Plan::Explain {
                kind: kind.clone(),
                config,
                plan: Box::new(self.bind_statement(bind_context, inner).await?),
            },
        };

        Ok(plan)
    }
}

fn validate_explain_config(kind: &ExplainKind, config: &ExplainConfig) -> Result<()> {
    if !matches!(
        kind,
        ExplainKind::Plan | ExplainKind::Raw | ExplainKind::Optimized
    ) && config.logical
    {
        return Err(ErrorCode::SyntaxException(
            "LOGICAL option is only supported for EXPLAIN SELECT statement".to_string(),
        ));
    }

    if !matches!(
        kind,
        ExplainKind::Plan | ExplainKind::Raw | ExplainKind::Optimized
    ) && config.optimized
    {
        return Err(ErrorCode::SyntaxException(
            "OPTIMIZED option is only supported for EXPLAIN SELECT statement".to_string(),
        ));
    }

    if !matches!(
        kind,
        ExplainKind::Plan | ExplainKind::Raw | ExplainKind::Optimized
    ) && config.verbose
    {
        return Err(ErrorCode::SyntaxException(
            "VERBOSE option is only supported for EXPLAIN SELECT statement".to_string(),
        ));
    }

    Ok(())
}
