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

use databend_common_ast::ast::AlterObjectTagAction;
use databend_common_ast::ast::AlterObjectTagStmt;
use databend_common_ast::ast::AlterObjectTagTarget;
use databend_common_ast::ast::CreateTagStmt;
use databend_common_ast::ast::DropTagStmt;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::ProcedureIdentity as AstProcedureIdentity;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowTagsStmt;
use databend_common_ast::ast::TagSetItem;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::SelectBuilder;
use crate::binder::BindContext;
use crate::binder::Binder;
use crate::planner::binder::ddl::procedure::generate_procedure_name_ident;
use crate::planner::semantic::normalize_identifier;
use crate::plans::ConnectionTagSetTarget;
use crate::plans::CreateTagPlan;
use crate::plans::DatabaseTagSetTarget;
use crate::plans::DropTagPlan;
use crate::plans::Plan;
use crate::plans::ProcedureTagSetTarget;
use crate::plans::RewriteKind;
use crate::plans::SetObjectTagsPlan;
use crate::plans::StageTagSetTarget;
use crate::plans::TableTagSetTarget;
use crate::plans::TagSetObject;
use crate::plans::TagSetPlanItem;
use crate::plans::UDFTagSetTarget;
use crate::plans::UnsetObjectTagsPlan;
use crate::plans::ViewTagSetTarget;

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

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_object_tag(
        &self,
        stmt: &AlterObjectTagStmt,
    ) -> Result<Plan> {
        match &stmt.action {
            AlterObjectTagAction::Set { tags } => {
                let normalized_tags = self.normalize_tag_set_items(tags)?;
                let object = self.build_tag_set_object(&stmt.object)?;
                Ok(Plan::SetObjectTags(Box::new(SetObjectTagsPlan {
                    tenant: self.ctx.get_tenant(),
                    object,
                    tags: normalized_tags,
                })))
            }
            AlterObjectTagAction::Unset { tags } => {
                let normalized_tags = self.normalize_tag_unset_items(tags);
                let object = self.build_tag_set_object(&stmt.object)?;
                Ok(Plan::UnsetObjectTags(Box::new(UnsetObjectTagsPlan {
                    tenant: self.ctx.get_tenant(),
                    object,
                    tags: normalized_tags,
                })))
            }
        }
    }

    pub(in crate::planner::binder) fn normalize_tag_set_items(
        &self,
        tags: &[TagSetItem],
    ) -> Result<Vec<TagSetPlanItem>> {
        let mut normalized = Vec::with_capacity(tags.len());
        for tag in tags {
            normalized.push(TagSetPlanItem {
                name: normalize_identifier(&tag.tag_name, &self.name_resolution_ctx).name,
                value: tag.tag_value.clone(),
            });
        }
        Ok(normalized)
    }

    pub(in crate::planner::binder) fn normalize_tag_unset_items(
        &self,
        tags: &[Identifier],
    ) -> Vec<String> {
        tags.iter()
            .map(|tag| normalize_identifier(tag, &self.name_resolution_ctx).name)
            .collect()
    }

    fn build_tag_set_object(&self, target: &AlterObjectTagTarget) -> Result<TagSetObject> {
        match target {
            AlterObjectTagTarget::Database {
                if_exists,
                catalog,
                database,
            } => {
                let catalog = catalog
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let database = normalize_identifier(database, &self.name_resolution_ctx).name;
                Ok(TagSetObject::Database(DatabaseTagSetTarget {
                    if_exists: *if_exists,
                    catalog,
                    database,
                }))
            }
            AlterObjectTagTarget::Table {
                if_exists,
                catalog,
                database,
                table,
            } => {
                let (catalog, database, table) =
                    self.normalize_object_identifier_triple(catalog, database, table);
                Ok(TagSetObject::Table(TableTagSetTarget {
                    if_exists: *if_exists,
                    catalog,
                    database,
                    table,
                }))
            }
            AlterObjectTagTarget::Stage {
                if_exists,
                stage_name,
            } => Ok(TagSetObject::Stage(StageTagSetTarget {
                if_exists: *if_exists,
                stage_name: stage_name.clone(),
            })),
            AlterObjectTagTarget::Connection {
                if_exists,
                connection_name,
            } => Ok(TagSetObject::Connection(ConnectionTagSetTarget {
                if_exists: *if_exists,
                connection_name: normalize_identifier(connection_name, &self.name_resolution_ctx)
                    .name,
            })),
            AlterObjectTagTarget::View {
                if_exists,
                catalog,
                database,
                view,
            } => {
                let (catalog, database, view) =
                    self.normalize_object_identifier_triple(catalog, database, view);
                Ok(TagSetObject::View(ViewTagSetTarget {
                    if_exists: *if_exists,
                    catalog,
                    database,
                    view,
                }))
            }
            AlterObjectTagTarget::Function {
                if_exists,
                udf_name,
            } => Ok(TagSetObject::UDF(UDFTagSetTarget {
                if_exists: *if_exists,
                udf_name: normalize_identifier(udf_name, &self.name_resolution_ctx).name,
            })),
            AlterObjectTagTarget::Procedure {
                if_exists,
                name,
                arg_types,
            } => {
                let ast_identity = AstProcedureIdentity {
                    name: name.to_string(),
                    args_type: arg_types.clone(),
                };
                let name_ident =
                    generate_procedure_name_ident(&self.ctx.get_tenant(), &ast_identity)?;
                let proc_identity = name_ident.procedure_name();
                Ok(TagSetObject::Procedure(ProcedureTagSetTarget {
                    if_exists: *if_exists,
                    name: proc_identity.name.clone(),
                    args: proc_identity.args.clone(),
                }))
            }
        }
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
