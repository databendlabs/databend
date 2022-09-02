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

use common_ast::ast::CreateTabularFunctionStmt;
use common_catalog::table_context::TableContext;
use common_meta_types::UserTabularFunction;
use common_planners::CreateTabularFunctionPlan;

use crate::sql::normalize_identifier;
use crate::sql::plans::Plan;
use crate::sql::Binder;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_create_tabular_function(
        &mut self,
        stmt: &CreateTabularFunctionStmt<'a>,
    ) -> Result<Plan> {
        let CreateTabularFunctionStmt {
            if_not_exists,
            name,
            args,
            source,
            as_query,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let func_name = normalize_identifier(name, &self.name_resolution_ctx).name;
        let subquery = format!("{}", query);

        let plan = CreateTabularFunctionPlan {
            if_not_exists: *if_not_exists,
            tenant,
            user_tabular_function: UserTabularFunction {
                name: func_name,
                parameters: vec![],
                schema: Arc::new(Default::default()),
                as_query: "".to_string(),
            },
        };
        Ok(Plan::CreateTabularFunction(Box::new(plan)))
    }
}
