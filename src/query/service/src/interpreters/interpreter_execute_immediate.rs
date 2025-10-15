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
use databend_common_ast::ast::ScriptStatement;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Scalar;
use databend_common_script::compile;
use databend_common_script::Executor;
use databend_common_script::ReturnValue;
use databend_common_sql::plans::ExecuteImmediatePlan;
use databend_common_storages_fuse::TableContext;
use tokio::sync::Mutex;

use crate::interpreters::util::ScriptClient;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct ExecuteImmediateInterpreter {
    ctx: Arc<QueryContext>,
    plan: ExecuteImmediatePlan,
    // schema is only known after execute
    schema: Mutex<Option<DataSchemaRef>>,
}

impl ExecuteImmediateInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ExecuteImmediatePlan) -> Result<Self> {
        Ok(ExecuteImmediateInterpreter {
            ctx,
            plan,
            schema: Mutex::new(None),
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for ExecuteImmediateInterpreter {
    fn name(&self) -> &str {
        "ExecuteImmediateInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    async fn get_dynamic_schema(&self) -> Option<DataSchemaRef> {
        self.schema.lock().await.clone()
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let res: Result<_> = try {
            let mut ast = self.plan.script_block.clone();
            let mut src = vec![];
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
            let settings = self.ctx.get_settings();
            let script_max_steps = settings.get_script_max_steps()?;
            let result = executor.run(script_max_steps as usize).await?;

            match result {
                Some(ReturnValue::Var(scalar)) => {
                    let typ = scalar.as_ref().infer_data_type();
                    let value = BlockEntry::new_const_column(typ.clone(), scalar, 1);

                    let mut w = self.schema.lock().await;
                    *w = Some(DataSchemaRefExt::create(vec![DataField::new(
                        "Result", typ,
                    )]));
                    PipelineBuildResult::from_blocks(vec![DataBlock::new(vec![value], 1)])?
                }
                Some(ReturnValue::Set(set)) => {
                    let block = set.block;

                    let mut w = self.schema.lock().await;
                    *w = Some(set.schema);

                    PipelineBuildResult::from_blocks(vec![block])?
                }
                None => {
                    let value = BlockEntry::new_const_column(
                        DataType::String.wrap_nullable(),
                        Scalar::Null,
                        1,
                    );

                    let mut w = self.schema.lock().await;
                    *w = Some(DataSchemaRefExt::create(vec![DataField::new(
                        "Result",
                        DataType::String.wrap_nullable(),
                    )]));
                    PipelineBuildResult::from_blocks(vec![DataBlock::new(vec![value], 1)])?
                }
            }
        };

        res.map_err(|err| err.display_with_sql(&self.plan.script))
    }
}
