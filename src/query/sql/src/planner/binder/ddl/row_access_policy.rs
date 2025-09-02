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

use databend_common_ast::ast::CreateRowAccessPolicyStmt;
use databend_common_ast::ast::DescRowAccessPolicyStmt;
use databend_common_ast::ast::DropRowAccessPolicyStmt;
use databend_common_ast::ast::Expr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_functions::is_builtin_function;
use derive_visitor::Drive;
use derive_visitor::Visitor;
use unicase::Ascii;

use crate::normalize_identifier;
use crate::plans::CreateRowAccessPolicyPlan;
use crate::plans::DescRowAccessPolicyPlan;
use crate::plans::DropRowAccessPolicyPlan;
use crate::plans::Plan;
use crate::Binder;
use crate::NameResolutionContext;
use crate::TypeChecker;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_row_access(
        &mut self,
        stmt: &CreateRowAccessPolicyStmt,
    ) -> Result<Plan> {
        let CreateRowAccessPolicyStmt {
            create_option,
            name,
            description,
            definition,
        } = stmt;

        #[derive(Visitor)]
        #[visitor(Expr(enter))]
        struct PolicyVisitor {
            name_resolution_ctx: NameResolutionContext,
            error: Option<ErrorCode>,
        }

        impl PolicyVisitor {
            fn enter_expr(&mut self, expr: &Expr) {
                match expr {
                    Expr::FunctionCall { func, span: _ } => {
                        let func_name =
                            normalize_identifier(&func.name, &self.name_resolution_ctx).to_string();
                        let func_name = func_name.as_str();
                        let uni_case_func_name = Ascii::new(func_name);
                        if !(is_builtin_function(func_name)
                            || TypeChecker::all_sugar_functions().contains(&uni_case_func_name)
                            || func.window.is_none()
                            || func.lambda.is_none()
                            || func.order_by.is_empty())
                        {
                            self.error = Some(ErrorCode::InvalidArgument(
                                "Row access policy expr only builtin function",
                            ));
                        }
                    }
                    Expr::InSubquery { .. } | Expr::LikeSubquery { .. } => {
                        self.error = Some(ErrorCode::InvalidArgument(
                            "Row access policy expr only builtin function",
                        ));
                    }
                    _ => {}
                }
            }
        }

        let stmt = stmt.clone();
        let name_resolution_ctx = self.name_resolution_ctx.clone();
        let mut visitor = PolicyVisitor {
            name_resolution_ctx,
            error: None,
        };
        stmt.drive(&mut visitor);

        if let Some(e) = visitor.error {
            return Err(e);
        }

        let tenant = self.ctx.get_tenant();
        let plan = CreateRowAccessPolicyPlan {
            create_option: create_option.clone().into(),
            tenant,
            name: normalize_identifier(name, &self.name_resolution_ctx).to_string(),
            row_access: definition.clone(),
            description: description.clone(),
        };
        Ok(Plan::CreateRowAccessPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_row_access(
        &mut self,
        stmt: &DropRowAccessPolicyStmt,
    ) -> Result<Plan> {
        let DropRowAccessPolicyStmt { if_exists, name } = stmt;

        let tenant = self.ctx.get_tenant();
        let plan = DropRowAccessPolicyPlan {
            if_exists: *if_exists,
            tenant,
            name: name.to_string(),
        };
        Ok(Plan::DropRowAccessPolicy(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_desc_row_access(
        &mut self,
        stmt: &DescRowAccessPolicyStmt,
    ) -> Result<Plan> {
        let DescRowAccessPolicyStmt { name } = stmt;

        let plan = DescRowAccessPolicyPlan {
            name: name.to_string(),
        };
        Ok(Plan::DescRowAccessPolicy(Box::new(plan)))
    }
}
