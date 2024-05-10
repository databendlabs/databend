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

use databend_common_ast::ast::ExecuteImmediateStmt;
use databend_common_exception::Result;

use crate::plans::ExecuteImmediatePlan;
use crate::plans::Plan;
use crate::Binder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_execute_immediate(
        &mut self,
        stmt: &ExecuteImmediateStmt,
    ) -> Result<Plan> {
        let ExecuteImmediateStmt { script } = stmt;
        Ok(Plan::ExecuteImmediate(Box::new(ExecuteImmediatePlan {
            script: script.clone(),
        })))
    }
}
