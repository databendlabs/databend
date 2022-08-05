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

use common_ast::ast::*;
use common_exception::Result;

use crate::sessions::TableContext;
use crate::sql::binder::Binder;
use crate::sql::plans::CreateSharePlan;
use crate::sql::plans::DropSharePlan;
use crate::sql::plans::Plan;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_create_share(
        &mut self,
        stmt: &CreateShareStmt<'a>,
    ) -> Result<Plan> {
        let CreateShareStmt {
            if_not_exists,
            share,
            comment,
        } = stmt;

        let share = share.name.to_lowercase();

        let plan = CreateSharePlan {
            if_not_exists: *if_not_exists,
            tenant: self.ctx.get_tenant(),
            share,
            comment: comment.as_ref().cloned(),
        };
        Ok(Plan::CreateShare(Box::new(plan)))
    }

    pub(in crate::sql::planner::binder) async fn bind_drop_share(
        &mut self,
        stmt: &DropShareStmt<'a>,
    ) -> Result<Plan> {
        let DropShareStmt { if_exists, share } = stmt;

        let share = share.name.to_lowercase();

        let plan = DropSharePlan {
            if_exists: *if_exists,
            tenant: self.ctx.get_tenant(),
            share,
        };
        Ok(Plan::DropShare(Box::new(plan)))
    }
}
