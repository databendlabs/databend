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
    state: ProcedureState,
}

#[derive(Debug)]
pub(crate) struct ProcedureState {
    schema: Mutex<Option<DataSchemaRef>>,
}

impl ProcedureState {
    pub fn new() -> Self {
        Self {
            schema: Mutex::new(None),
        }
    }

    pub async fn get_schema(&self) -> Option<DataSchemaRef> {
        self.schema.lock().await.clone()
    }

    pub async fn set_null_schema(&self) {
        let mut w = self.schema.lock().await;
        *w = Some(DataSchemaRefExt::create(vec![DataField::new(
            "Result",
            DataType::String.wrap_nullable(),
        )]));
    }

    pub fn null_result() -> DataBlock {
        DataBlock::new(
            vec![BlockEntry::new_const_column(
                DataType::String.wrap_nullable(),
                Scalar::Null,
                1,
            )],
            1,
        )
    }

    pub async fn set_scalar_schema(&self, scalar: &Scalar) {
        let mut w = self.schema.lock().await;
        *w = Some(DataSchemaRefExt::create(vec![DataField::new(
            "Result",
            scalar.as_ref().infer_data_type(),
        )]));
    }

    pub fn scalar_result(scalar: Scalar) -> DataBlock {
        let typ = scalar.as_ref().infer_data_type();
        DataBlock::new(vec![BlockEntry::new_const_column(typ, scalar, 1)], 1)
    }

    pub async fn set_schema(&self, schema: DataSchemaRef) {
        let mut w = self.schema.lock().await;
        *w = Some(schema);
    }
}

impl ExecuteImmediateInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ExecuteImmediatePlan) -> Result<Self> {
        Ok(ExecuteImmediateInterpreter {
            ctx,
            plan,
            state: ProcedureState::new(),
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
        self.state.get_schema().await
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
                    self.state.set_scalar_schema(&scalar).await;
                    let block = ProcedureState::scalar_result(scalar);
                    PipelineBuildResult::from_blocks(vec![block])?
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
