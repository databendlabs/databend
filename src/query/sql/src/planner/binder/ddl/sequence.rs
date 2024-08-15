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

use databend_common_ast::ast::CreateSequenceStmt;
use databend_common_ast::ast::DropSequenceStmt;
use databend_common_exception::Result;
use databend_common_meta_app::schema::SequenceIdent;

use crate::plans::CreateSequencePlan;
use crate::plans::DropSequencePlan;
use crate::plans::Plan;
use crate::Binder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_sequence(
        &mut self,
        stmt: &CreateSequenceStmt,
    ) -> Result<Plan> {
        let CreateSequenceStmt {
            create_option,
            sequence,
            comment,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let sequence = sequence.normalized_name();

        let plan = CreateSequencePlan {
            create_option: create_option.clone().into(),
            ident: SequenceIdent::new(tenant, sequence),
            comment: comment.clone(),
        };
        Ok(Plan::CreateSequence(plan.into()))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_sequence(
        &mut self,
        stmt: &DropSequenceStmt,
    ) -> Result<Plan> {
        let DropSequenceStmt {
            sequence,
            if_exists,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let sequence = sequence.normalized_name();

        let plan = DropSequencePlan {
            ident: SequenceIdent::new(tenant, sequence),
            if_exists: *if_exists,
        };
        Ok(Plan::DropSequence(plan.into()))
    }
}
