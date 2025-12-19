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
use databend_common_sql::plans::CacheSource;

use crate::physical_plans::CacheScan;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::format_output_columns;

pub struct CacheScanFormatter<'a> {
    inner: &'a CacheScan,
}

impl<'a> CacheScanFormatter<'a> {
    pub fn create(inner: &'a CacheScan) -> Box<dyn PhysicalFormat + 'a> {
        Box::new(CacheScanFormatter { inner })
    }
}

impl<'a> PhysicalFormat for CacheScanFormatter<'a> {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        self.inner.get_meta()
    }

    #[recursive::recursive]
    fn format(&self, ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        let mut children = Vec::with_capacity(2);
        children.push(FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(self.inner.output_schema()?, ctx.metadata, true)
        )));

        match &self.inner.cache_source {
            CacheSource::HashJoinBuild((cache_index, column_indexes)) => {
                let mut column_indexes = column_indexes.clone();
                column_indexes.sort();
                children.push(FormatTreeNode::new(format!("cache index: {}", cache_index)));
                children.push(FormatTreeNode::new(format!(
                    "column indexes: {:?}",
                    column_indexes
                )));
            }
        }

        Ok(FormatTreeNode::with_children(
            "CacheScan".to_string(),
            children,
        ))
    }

    fn format_join(&self, _ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::with_children(self.inner.get_name(), vec![]))
    }

    fn partial_format(&self, _ctx: &mut FormatContext<'_>) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::with_children(self.inner.get_name(), vec![]))
    }
}
