use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use sqlparser::ast::{SetVariableValue, Ident};
use crate::sessions::DatabendQueryContextRef;
use common_exception::{Result, ErrorCode};
use common_planners::{VarValue, PlanNode, SettingPlan};

#[derive(Debug, Clone, PartialEq)]
pub struct DfSetVariable {
    local: bool,
    hivevar: bool,
    variable: Ident,
    value: Vec<SetVariableValue>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfSetVariable {
    async fn analyze(&self, _: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        if self.hivevar {
            return Err(ErrorCode::SyntaxException("Unsupport hive style set varible"));
        }

        // TODO: session variable and local variable
        let vars = self.mapping_set_vars();
        Ok(AnalyzedResult::SimpleQuery(PlanNode::SetVariable(SettingPlan { vars })))
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

    fn mapping_set_vars(self) -> Vec<VarValue> {
        let variable = self.variable.value.clone();
        self.value.iter()
            .map(|value| DfSetVariable::mapping_set_var(variable.clone(), value))
            .collect()
    }
}