use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::SettingPlan;
use common_planners::VarValue;
use sqlparser::ast::Ident;
use sqlparser::ast::SetVariableValue;

use crate::sessions::DatabendQueryContextRef;
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
    async fn analyze(&self, _: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        if self.hivevar {
            return Err(ErrorCode::SyntaxException(
                "Unsupport hive style set varible",
            ));
        }

        // TODO: session variable and local variable
        let vars = self.mapping_set_vars();
        Ok(AnalyzedResult::SimpleQuery(PlanNode::SetVariable(
            SettingPlan { vars },
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
