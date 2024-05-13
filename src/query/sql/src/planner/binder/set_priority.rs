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

use databend_common_ast::ast::Priority;
use databend_common_exception::Result;

use crate::planner::binder::Binder;
use crate::plans::Plan;
use crate::plans::SetPriorityPlan;

impl Binder {
    #[async_backtrace::framed]
    pub(super) async fn bind_set_priority(
        &mut self,
        priority: &Priority,
        object_id: &str,
    ) -> Result<Plan> {
        let priority_num: u8 = match &priority {
            Priority::HIGH => 5,
            Priority::MEDIUM => 3,
            Priority::LOW => 2,
        };

        let plan = Box::new(SetPriorityPlan {
            id: object_id.to_string(),
            priority: priority_num,
        });

        Ok(Plan::SetPriority(plan))
    }
}
