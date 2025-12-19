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

use databend_common_ast::ast::FormatTreeNode;
use databend_common_exception::Result;
use databend_common_functions::BUILTIN_FUNCTIONS;
use itertools::Itertools;

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::MutationManipulate;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;

pub struct MutationManipulateFormatter<'a> {
    inner: &'a MutationManipulate,
}

impl<'a> MutationManipulateFormatter<'a> {
    pub fn create(inner: &'a MutationManipulate) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(MutationManipulateFormatter { inner })
    }
}

impl<'a> PhysicalFormat for MutationManipulateFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let table_entry = ctx.metadata.table(self.inner.target_table_index).clone();
        let target_schema = table_entry.table().schema_with_stream();

        // Matched clauses.
        let mut matched_children = Vec::with_capacity(self.inner.matched.len());
        for evaluator in &self.inner.matched {
            let condition_format = evaluator.0.as_ref().map_or_else(
                || "condition: None".to_string(),
                |predicate| {
                    format!(
                        "condition: {}",
                        predicate.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                    )
                },
            );

            if evaluator.1.is_none() {
                matched_children.push(FormatTreeNode::new(format!(
                    "matched delete: [{}]",
                    condition_format
                )));
            } else {
                let mut update_list = evaluator.1.as_ref().unwrap().clone();
                update_list.sort_by(|a, b| a.0.cmp(&b.0));
                let update_format = update_list
                    .iter()
                    .map(|(field_idx, expr)| {
                        format!(
                            "{} = {}",
                            target_schema.field(*field_idx).name(),
                            expr.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                        )
                    })
                    .join(",");
                matched_children.push(FormatTreeNode::new(format!(
                    "matched update: [{}, update set {}]",
                    condition_format, update_format
                )));
            }
        }

        // UnMatched clauses.
        let mut unmatched_children = Vec::with_capacity(self.inner.unmatched.len());
        for evaluator in &self.inner.unmatched {
            let condition_format = evaluator.1.as_ref().map_or_else(
                || "condition: None".to_string(),
                |predicate| {
                    format!(
                        "condition: {}",
                        predicate.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                    )
                },
            );
            let insert_schema_format = evaluator
                .0
                .fields
                .iter()
                .map(|field| field.name())
                .join(",");

            let values_format = evaluator
                .2
                .iter()
                .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                .join(",");

            let unmatched_format = format!(
                "insert into ({}) values({})",
                insert_schema_format, values_format
            );

            unmatched_children.push(FormatTreeNode::new(format!(
                "unmatched insert: [{}, {}]",
                condition_format, unmatched_format
            )));
        }

        let mut node_children = vec![];

        node_children.extend(matched_children);
        node_children.extend(unmatched_children);

        let input_formatter = self.inner.input.formatter()?;
        node_children.push(input_formatter.dispatch(ctx)?);

        Ok(FormatTreeNode::with_children(
            "MutationManipulate".to_string(),
            node_children,
        ))
    }

    #[recursive::recursive]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        self.inner.input.formatter()?.format_join(ctx)
    }

    #[recursive::recursive]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        self.inner.input.formatter()?.partial_format(ctx)
    }
}
