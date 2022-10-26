// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_planners::PlanNode;

use crate::interpreters::access::Accessor;
use crate::interpreters::DeleteInterpreter;
use crate::interpreters::EmptyInterpreter;
use crate::interpreters::ExplainInterpreter;
use crate::interpreters::InsertInterpreter;
use crate::interpreters::InterceptorInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::SelectInterpreter;
use crate::sessions::QueryContext;

/// InterpreterFactory is the entry of Interpreter.
pub struct InterpreterFactory;

/// InterpreterFactory provides `get` method which transforms the PlanNode into the corresponding interpreter.
/// Such as: SelectPlan -> SelectInterpreter, ExplainPlan -> ExplainInterpreter, ...
impl InterpreterFactory {
    pub fn get(ctx: Arc<QueryContext>, plan: PlanNode) -> Result<Arc<dyn Interpreter>> {
        // Check the access permission.
        let access_checker = Accessor::create(ctx.clone());
        access_checker.check(&plan)?;

        let inner = Self::create_interpreter(ctx.clone(), &plan)?;
        let query_kind = plan.name().to_string();
        Ok(Arc::new(InterceptorInterpreter::create(
            ctx, inner, query_kind,
        )))
    }

    fn create_interpreter(ctx: Arc<QueryContext>, plan: &PlanNode) -> Result<InterpreterPtr> {
        match plan.clone() {
            PlanNode::Select(v) => Ok(Arc::new(SelectInterpreter::try_create(ctx, v)?)),
            PlanNode::Explain(v) => Ok(Arc::new(ExplainInterpreter::try_create(ctx, v)?)),
            PlanNode::Insert(v) => Ok(Arc::new(InsertInterpreter::try_create(ctx, v, false)?)),
            PlanNode::Delete(v) => Ok(Arc::new(DeleteInterpreter::try_create(ctx, v)?)),
            PlanNode::Empty(v) => Ok(Arc::new(EmptyInterpreter::try_create(ctx, v)?)),
            _ => Err(ErrorCode::UnknownTypeOfQuery(format!(
                "Can't get the interpreter by plan: {}",
                plan.name()
            ))),
        }
    }
}
