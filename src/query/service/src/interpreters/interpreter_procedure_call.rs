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

use std::sync::Arc;

use databend_common_ast::ast::DeclareItem;
use databend_common_ast::ast::DeclareVar;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::ScriptStatement;
use databend_common_ast::parser::run_parser;
use databend_common_ast::parser::script::script_block;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::ParseMode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_script::compile;
use databend_common_script::Executor;
use databend_common_script::ReturnValue;
use databend_common_sql::plans::CallProcedurePlan;
use databend_common_storages_fuse::TableContext;

use crate::interpreters::interpreter_execute_immediate::ProcedureState;
use crate::interpreters::util::ScriptClient;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct CallProcedureInterpreter {
    ctx: Arc<QueryContext>,
    plan: CallProcedurePlan,
    state: ProcedureState,
}

impl CallProcedureInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CallProcedurePlan) -> Result<Self> {
        Ok(CallProcedureInterpreter {
            ctx,
            plan,
            state: ProcedureState::new(),
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for CallProcedureInterpreter {
    fn name(&self) -> &str {
        "ProcedureCall"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    async fn get_dynamic_schema(&self) -> Option<DataSchemaRef> {
        self.state.get_schema().await
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let res: Result<_> = try {
            let mut src = vec![];
            for (arg, arg_name) in self.plan.args.iter().zip(self.plan.arg_names.iter()) {
                src.push(ScriptStatement::LetVar {
                    declare: DeclareVar {
                        span: None,
                        name: Identifier::from_name(None, arg_name),
                        data_type: None,
                        default: Some(arg.clone()),
                    },
                });
            }
            let settings = self.ctx.get_settings();
            let sql_dialect = settings.get_sql_dialect()?;
            let tokens = tokenize_sql(&self.plan.script)?;
            let mut ast = run_parser(
                &tokens,
                sql_dialect,
                ParseMode::Template,
                false,
                script_block,
            )?;

            for declare in ast.declares {
                match declare {
                    DeclareItem::Var(declare) => src.push(ScriptStatement::LetVar { declare }),
                    DeclareItem::Set(declare) => {
                        src.push(ScriptStatement::LetStatement { declare })
                    }
                }
            }
            src.append(&mut ast.body);
            let compiled = compile(&src)?;

            let client = ScriptClient {
                ctx: self.ctx.clone(),
            };
            let mut executor = Executor::load(ast.span, client, compiled);
            let script_max_steps = settings.get_script_max_steps()?;
            let result = executor.run(script_max_steps as usize).await?;

            match result {
                Some(ReturnValue::Var(scalar)) => {
                    self.state.set_scalar_schema(&scalar).await;
                    PipelineBuildResult::from_blocks(vec![ProcedureState::scalar_result(scalar)])?
                }
                Some(ReturnValue::Set(set)) => {
                    self.state.set_schema(set.schema).await;
                    PipelineBuildResult::from_blocks(vec![set.block])?
                }
                None => {
                    self.state.set_null_schema().await;
                    PipelineBuildResult::from_blocks(vec![ProcedureState::null_result()])?
                }
            }
        };

        res.map_err(|err| err.display_with_sql(&self.plan.script))
    }
}
