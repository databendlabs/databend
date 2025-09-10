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
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::ShowOptions;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::SequenceIdent;

use crate::binder::show::get_show_options;
use crate::plans::CreateSequencePlan;
use crate::plans::DescSequencePlan;
use crate::plans::DropSequencePlan;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::BindContext;
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
            start,
            increment,
            comment,
        } = stmt;

        let start = start.unwrap_or(1);
        let increment = increment.unwrap_or(1);
        if increment == 0 {
            return Err(ErrorCode::InvalidArgument(
                "Increment is an invalid value '0'",
            ));
        }

        let tenant = self.ctx.get_tenant();
        let sequence = self.normalize_object_identifier(sequence);

        let plan = CreateSequencePlan {
            create_option: create_option.clone().into(),
            ident: SequenceIdent::new(tenant, sequence),
            start,
            increment,
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
        let sequence = self.normalize_object_identifier(sequence);

        let plan = DropSequencePlan {
            ident: SequenceIdent::new(tenant, sequence),
            if_exists: *if_exists,
        };
        Ok(Plan::DropSequence(plan.into()))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_sequences(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) = get_show_options(show_options, None);
        let query = format!(
            "SELECT * FROM show_sequences() {} {} order by name",
            show_limit, limit_str
        );

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowSequences)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_desc_sequence(
        &mut self,
        name: &Identifier,
    ) -> Result<Plan> {
        let tenant = self.ctx.get_tenant();
        let name = self.normalize_object_identifier(name);
        let plan = DescSequencePlan {
            ident: SequenceIdent::new(tenant, name),
        };
        Ok(Plan::DescSequence(Box::new(plan)))
    }
}
