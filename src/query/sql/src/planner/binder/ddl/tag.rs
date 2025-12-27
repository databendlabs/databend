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

use databend_common_ast::ast::CreateTagStmt;
use databend_common_ast::ast::DropTagStmt;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowTagsStmt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::SelectBuilder;
use crate::binder::BindContext;
use crate::binder::Binder;
use crate::planner::semantic::normalize_identifier;
use crate::plans::CreateTagPlan;
use crate::plans::DropTagPlan;
use crate::plans::Plan;
use crate::plans::RewriteKind;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_tag(
        &self,
        stmt: &CreateTagStmt,
    ) -> Result<Plan> {
        let mut allowed_values = None;
        if let Some(values) = &stmt.allowed_values {
            let mut normalized = Vec::with_capacity(values.len());
            for literal in values {
                normalized.push(self.literal_to_tag_value(literal)?);
            }
            allowed_values = Some(normalized);
        }

        let name = normalize_identifier(&stmt.name, &self.name_resolution_ctx).name;
        let plan = CreateTagPlan {
            tenant: self.ctx.get_tenant(),
            create_option: stmt.create_option.clone().into(),
            name,
            allowed_values,
            comment: stmt.comment.clone(),
        };
        Ok(Plan::CreateTag(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_tag(
        &self,
        stmt: &DropTagStmt,
    ) -> Result<Plan> {
        let plan = DropTagPlan {
            tenant: self.ctx.get_tenant(),
            if_exists: stmt.if_exists,
            name: normalize_identifier(&stmt.name, &self.name_resolution_ctx).name,
        };
        Ok(Plan::DropTag(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_tags(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowTagsStmt,
    ) -> Result<Plan> {
        let default_catalog = self.ctx.get_default_catalog()?.name();
        let mut select_builder = SelectBuilder::from(&format!("{}.system.tags", default_catalog));
        select_builder
            .with_column("name")
            .with_column("allowed_values")
            .with_column("comment")
            .with_column("created_on")
            .with_order_by("name");

        match &stmt.filter {
            Some(ShowLimit::Like { pattern }) => {
                select_builder
                    .with_filter(format!("LOWER(name) LIKE '{}'", pattern.to_lowercase()));
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
            }
            None => {}
        }

        let mut query = select_builder.build();

        if let Some(limit) = stmt.limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowTags)
            .await
    }

    fn literal_to_tag_value(&self, literal: &Literal) -> Result<String> {
        match literal {
            Literal::String(val) => Ok(val.clone()),
            _ => Err(ErrorCode::SyntaxException(
                "Tag values must be string literals",
            )),
        }
    }
}
