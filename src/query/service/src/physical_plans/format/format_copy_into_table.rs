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

use crate::physical_plans::CopyIntoTable;
use crate::physical_plans::CopyIntoTableSource;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;

pub struct CopyIntoTableFormatter<'a> {
    inner: &'a CopyIntoTable,
}

impl<'a> CopyIntoTableFormatter<'a> {
    pub fn create(inner: &'a CopyIntoTable) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(CopyIntoTableFormatter { inner })
    }
}

impl<'a> PhysicalFormat for CopyIntoTableFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let mut children = vec![];

        // Format child based on source type
        match &self.inner.source {
            CopyIntoTableSource::Query(input) => {
                let formatter = input.formatter()?;
                children.push(formatter.dispatch(ctx)?);
            }
            CopyIntoTableSource::Stage(input) => {
                let formatter = input.formatter()?;
                children.push(formatter.dispatch(ctx)?);
            }
        }

        Ok(FormatTreeNode::with_children(
            format!("CopyIntoTable: {}", self.inner.table_info.inner),
            children,
        ))
    }

    #[recursive::recursive]
    fn format_join(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        match &self.inner.source {
            CopyIntoTableSource::Query(input) => {
                let formatter = input.formatter()?;
                formatter.format_join(ctx)
            }
            CopyIntoTableSource::Stage(input) => {
                let formatter = input.formatter()?;
                formatter.format_join(ctx)
            }
        }
    }

    #[recursive::recursive]
    fn partial_format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        match &self.inner.source {
            CopyIntoTableSource::Query(input) => {
                let formatter = input.formatter()?;
                formatter.partial_format(ctx)
            }
            CopyIntoTableSource::Stage(input) => {
                let formatter = input.formatter()?;
                formatter.partial_format(ctx)
            }
        }
    }
}
