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

use std::any::Any;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use itertools::Itertools;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::format::format_output_columns;
use crate::executor::format::plan_stats_info_to_format_tree;
use crate::executor::format::FormatContext;
use crate::executor::physical_plan::DeriveHandle;
use crate::executor::IPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanMeta;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RowFetch {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    // cloned from `input`.
    pub source: Box<DataSourcePlan>,
    // projection on the source table schema.
    pub cols_to_fetch: Projection,
    pub row_id_col_offset: usize,
    pub fetched_fields: Vec<DataField>,
    pub need_wrap_nullable: bool,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for RowFetch {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = self.input.output_schema()?.fields().clone();
        fields.extend_from_slice(&self.fetched_fields);
        Ok(DataSchemaRefExt::create(fields))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn to_format_node(
        &self,
        ctx: &mut FormatContext<'_>,
        children: Vec<FormatTreeNode<String>>,
    ) -> Result<FormatTreeNode<String>> {
        let table_schema = self.source.source_info.schema();
        let projected_schema = self.cols_to_fetch.project_schema(&table_schema);
        let fields_to_fetch = projected_schema.fields();

        let mut node_children = vec![
            FormatTreeNode::new(format!(
                "output columns: [{}]",
                format_output_columns(self.output_schema()?, &ctx.metadata, true)
            )),
            FormatTreeNode::new(format!(
                "columns to fetch: [{}]",
                fields_to_fetch.iter().map(|f| f.name()).join(", ")
            )),
        ];

        if let Some(info) = &self.stat_info {
            node_children.extend(plan_stats_info_to_format_tree(info));
        }

        node_children.extend(children);

        Ok(FormatTreeNode::with_children(
            "RowFetch".to_string(),
            node_children,
        ))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        let table_schema = self.source.source_info.schema();
        let projected_schema = self.cols_to_fetch.project_schema(&table_schema);
        Ok(projected_schema.fields.iter().map(|f| f.name()).join(", "))
    }

    fn derive(&self, mut children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        let mut new_physical_plan = self.clone();
        assert_eq!(children.len(), 1);
        new_physical_plan.input = children.pop().unwrap();
        Box::new(new_physical_plan)
    }
}
