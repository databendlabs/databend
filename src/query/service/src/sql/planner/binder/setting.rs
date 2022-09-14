// Copyright 2022 Datafuse Labs.
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

use common_ast::ast::Identifier;
use common_ast::ast::Literal;
use common_exception::Result;
use common_legacy_planners::SettingPlan;
use common_legacy_planners::VarValue;

use super::BindContext;
use super::Binder;
use crate::sql::planner::semantic::TypeChecker;
use crate::sql::plans::Plan;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_set_variable(
        &mut self,
        bind_context: &BindContext,
        is_global: bool,
        variable: &Identifier<'a>,
        value: &Literal,
    ) -> Result<Plan> {
        let type_checker = TypeChecker::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );

        let variable = variable.name.clone();

        let box (value, _data_type) = type_checker.resolve_literal(value, None)?;
        let value = String::from_utf8(value.as_string()?)?;

        let vars = vec![VarValue {
            is_global,
            variable,
            value,
        }];
        Ok(Plan::SetVariable(Box::new(SettingPlan { vars })))
    }
}
