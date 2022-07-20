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


use common_ast::ast::KillTarget;
use common_exception::Result;
use common_planners::KillPlan;

use crate::sql::planner::binder::BindContext;
use crate::sql::planner::binder::Binder;
use crate::sql::plans::Plan;

impl<'a> Binder {
    pub(super) async fn bind_kill_stmt(
        &mut self,
        _bind_context: &BindContext,
        kill_target: &KillTarget,
        object_id: &String,
    ) -> Result<Plan> {
        let kill_connection = matches!(kill_target, KillTarget::Connection);
        let plan = Box::new(KillPlan {
            id: object_id.clone(),
            kill_connection,
        });

        Ok(Plan::Kill(plan))
    }
}
