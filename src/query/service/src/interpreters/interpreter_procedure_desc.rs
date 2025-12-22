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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::StringType;
use databend_common_meta_app::principal::GetProcedureReq;
use databend_common_sql::plans::DescProcedurePlan;
use databend_common_users::UserApiProvider;
use itertools::Itertools;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;

#[derive(Debug)]
pub struct DescProcedureInterpreter {
    pub(crate) plan: DescProcedurePlan,
}

impl DescProcedureInterpreter {
    pub fn try_create(plan: DescProcedurePlan) -> Result<Self> {
        Ok(DescProcedureInterpreter { plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescProcedureInterpreter {
    fn name(&self) -> &str {
        "DescProcedureInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.plan.tenant.clone();

        let req: GetProcedureReq = self.plan.clone().into();
        let procedure = UserApiProvider::instance()
            .procedure_api(&tenant)
            .get_procedure(&req)
            .await?;

        if let Some(procedure) = procedure {
            let script = format!("{}", procedure.procedure_meta.script);
            let returns = format!(
                "({})",
                procedure.procedure_meta.return_types.iter().join(",")
            );
            let signature = format!("({})", procedure.procedure_meta.arg_names.iter().join(","));
            let language = procedure.procedure_meta.procedure_language;

            PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                StringType::from_data(vec!["signature", "returns", "language", "body"]),
                StringType::from_data(vec![signature, returns, language, script]),
            ])])
        } else {
            return Err(ErrorCode::UnknownProcedure(format!(
                "Unknown procedure {}",
                self.plan.name.procedure_name()
            )));
        }
    }
}
