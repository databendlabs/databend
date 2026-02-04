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

use std::collections::BTreeMap;

use databend_common_ast::ast::AlterWorkerAction;
use databend_common_ast::ast::AlterWorkerStmt;
use databend_common_ast::ast::CreateWorkerStmt;
use databend_common_ast::ast::DropWorkerStmt;
use databend_common_ast::ast::ShowWorkersStmt;
use databend_common_exception::Result;

use crate::Binder;
use crate::plans::AlterWorkerPlan;
use crate::plans::CreateWorkerPlan;
use crate::plans::DropWorkerPlan;
use crate::plans::Plan;

impl Binder {
    pub(in crate::planner::binder) fn bind_show_workers(
        &mut self,
        _stmt: &ShowWorkersStmt,
    ) -> Result<Plan> {
        Ok(Plan::ShowWorkers)
    }

    pub(in crate::planner::binder) fn bind_create_worker(
        &mut self,
        stmt: &CreateWorkerStmt,
    ) -> Result<Plan> {
        let tenant = self.ctx.get_tenant();
        Ok(Plan::CreateWorker(Box::new(CreateWorkerPlan {
            if_not_exists: stmt.if_not_exists,
            tenant,
            name: stmt.name.to_string(),
            tags: BTreeMap::new(),
            options: stmt
                .options
                .iter()
                .map(|(k, v)| (k.to_lowercase(), v.clone()))
                .collect(),
        })))
    }

    pub(in crate::planner::binder) fn bind_drop_worker(
        &mut self,
        stmt: &DropWorkerStmt,
    ) -> Result<Plan> {
        let tenant = self.ctx.get_tenant();
        Ok(Plan::DropWorker(Box::new(DropWorkerPlan {
            if_exists: stmt.if_exists,
            tenant,
            name: stmt.name.to_string(),
        })))
    }

    pub(in crate::planner::binder) fn bind_alter_worker(
        &mut self,
        stmt: &AlterWorkerStmt,
    ) -> Result<Plan> {
        let tenant = self.ctx.get_tenant();
        let mut set_tags = BTreeMap::new();
        let mut unset_tags = Vec::new();
        match &stmt.action {
            AlterWorkerAction::SetTag { tags } => {
                for tag in tags {
                    set_tags.insert(tag.tag_name.to_string(), tag.tag_value.clone());
                }
            }
            AlterWorkerAction::UnsetTag { tags } => {
                unset_tags.extend(tags.iter().map(|tag| tag.to_string()));
            }
        }
        Ok(Plan::AlterWorker(Box::new(AlterWorkerPlan {
            tenant,
            name: stmt.name.to_string(),
            set_tags,
            unset_tags,
        })))
    }
}
