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

use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;

pub fn find_eq_filter(expr: &Expr<String>, visitor: &mut impl FnMut(&str, &Scalar)) {
    match expr {
        Expr::Constant { .. } | Expr::ColumnRef { .. } => {}
        Expr::Cast { expr, .. } => find_eq_filter(expr, visitor),
        Expr::FunctionCall { function, args, .. } => {
            // Like: select * from (select * from system.tables where database='default') where name='t'
            // push downs: [filters: [and_filters(and_filters(tables.database (#1) = 'default', tables.name (#2) = 't'), tables.database (#1) = 'default')], limit: NONE]
            // database generate twice, so when call find_eq_filter, should check uniq.
            if function.signature.name == "eq" {
                match args.as_slice() {
                    [Expr::ColumnRef { id, .. }, Expr::Constant { scalar, .. }]
                    | [Expr::Constant { scalar, .. }, Expr::ColumnRef { id, .. }] => {
                        visitor(id, scalar);
                    }
                    _ => {}
                }
            } else if function.signature.name == "and_filters" {
                // only support this:
                // 1. where xx and xx and xx
                // 2. filter: Column `table`, Column `database`
                for arg in args {
                    find_eq_filter(arg, visitor)
                }
            }
        }
        Expr::LambdaFunctionCall { args, .. } => {
            for arg in args {
                find_eq_filter(arg, visitor)
            }
        }
    }
}

pub fn find_gt_filter(expr: &Expr<String>, visitor: &mut impl FnMut(&str, &Scalar)) {
    match expr {
        Expr::Constant { .. } | Expr::ColumnRef { .. } => {}
        Expr::Cast { expr, .. } => find_gt_filter(expr, visitor),
        Expr::FunctionCall { function, args, .. } => {
            if function.signature.name == "gt" || function.signature.name == "gte" {
                match args.as_slice() {
                    [Expr::ColumnRef { id, .. }, Expr::Constant { scalar, .. }]
                    | [Expr::Constant { scalar, .. }, Expr::ColumnRef { id, .. }] => {
                        visitor(id, scalar);
                    }
                    _ => {}
                }
            } else if function.signature.name == "and_filters" {
                // only support this:
                // 1. where xx and xx and xx
                // 2. filter: Column `table`, Column `database`
                for arg in args {
                    find_gt_filter(arg, visitor)
                }
            }
        }
        Expr::LambdaFunctionCall { args, .. } => {
            for arg in args {
                find_gt_filter(arg, visitor)
            }
        }
    }
}

pub fn find_lt_filter(expr: &Expr<String>, visitor: &mut impl FnMut(&str, &Scalar)) {
    match expr {
        Expr::Constant { .. } | Expr::ColumnRef { .. } => {}
        Expr::Cast { expr, .. } => find_lt_filter(expr, visitor),
        Expr::FunctionCall { function, args, .. } => {
            if function.signature.name == "lt" || function.signature.name == "lte" {
                match args.as_slice() {
                    [Expr::ColumnRef { id, .. }, Expr::Constant { scalar, .. }]
                    | [Expr::Constant { scalar, .. }, Expr::ColumnRef { id, .. }] => {
                        visitor(id, scalar);
                    }
                    _ => {}
                }
            } else if function.signature.name == "and_filters" {
                // only support this:
                // 1. where xx and xx and xx
                // 2. filter: Column `table`, Column `database`
                for arg in args {
                    find_lt_filter(arg, visitor)
                }
            }
        }
        Expr::LambdaFunctionCall { args, .. } => {
            for arg in args {
                find_lt_filter(arg, visitor)
            }
        }
    }
}

pub(crate) async fn get_owned_task_names(
    user_api: Arc<UserApiProvider>,
    tenant: &Tenant,
    all_effective_roles: &[String],
    has_admin_role: bool,
) -> Vec<String> {
    let mut owned_tasks_names = vec![];
    if !has_admin_role {
        // Note: In old version databend-query the hashmap maybe empty
        let task_ownerships = user_api
            .list_tasks_ownerships(tenant)
            .await
            .unwrap_or_default();
        for (ownership, role) in task_ownerships {
            match ownership {
                OwnershipObject::Database { .. } => {}
                OwnershipObject::Table { .. } => {}
                OwnershipObject::Stage { .. } => {}
                OwnershipObject::UDF { .. } => {}
                OwnershipObject::Task { name } => {
                    if all_effective_roles.contains(&role) && !owned_tasks_names.contains(&name) {
                        owned_tasks_names.push(name);
                    }
                }
            }
        }
    }
    owned_tasks_names
}
