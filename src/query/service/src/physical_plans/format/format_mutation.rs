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

use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::Mutation;

pub struct MutationFormatter<'a> {
    inner: &'a Mutation,
}

impl<'a> MutationFormatter<'a> {
    pub fn create(inner: &'a Mutation) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(MutationFormatter { inner })
    }
}

impl<'a> PhysicalFormat for MutationFormatter<'a> {
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let table_entry = ctx.metadata.table(self.inner.target_table_index).clone();
        let mut node_children = vec![FormatTreeNode::new(format!(
            "target table: [catalog: {}] [database: {}] [table: {}]",
            table_entry.catalog(),
            table_entry.database(),
            table_entry.name()
        ))];

        let input_formatter = self.inner.input.formater()?;
        node_children.push(input_formatter.format(ctx)?);

        Ok(FormatTreeNode::with_children(
            "DataMutation".to_string(),
            node_children,
        ))
    }
}
