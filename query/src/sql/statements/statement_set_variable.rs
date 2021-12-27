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
use common_planners::PlanNode;
use common_planners::SettingPlan;
use common_planners::VarValue;
use common_tracing::tracing;
use sqlparser::ast::Ident;
use sqlparser::ast::SetVariableValue;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfSetVariable {
    pub local: bool,
    pub hivevar: bool,
    pub variable: Ident,
    pub value: Vec<SetVariableValue>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfSetVariable {
    #[tracing::instrument(level = "debug", skip(self, _ctx), fields(ctx.id = _ctx.get_id().as_str()))]
    async fn analyze(&self, _ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        if self.hivevar {
            return Err(ErrorCode::SyntaxException(
                "Unsupport hive style set varible",
            ));
        }

        // TODO: session variable and local variable
        let vars = self.mapping_set_vars();
        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::SetVariable(SettingPlan { vars }),
        )))
    }
}

impl DfSetVariable {
    fn mapping_set_var(variable: String, value: &SetVariableValue) -> VarValue {
        VarValue {
            variable,
            value: match value {
                sqlparser::ast::SetVariableValue::Ident(v) => v.value.clone(),
                sqlparser::ast::SetVariableValue::Literal(v) => v.to_string(),
            },
        }
    }

    fn mapping_set_vars(&self) -> Vec<VarValue> {
        let variable = self.variable.value.clone();
        self.value
            .iter()
            .map(|value| DfSetVariable::mapping_set_var(variable.clone(), value))
            .collect()
    }
}
