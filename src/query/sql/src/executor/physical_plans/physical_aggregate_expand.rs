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
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::format::format_output_columns;
use crate::executor::format::plan_stats_info_to_format_tree;
use crate::executor::format::FormatContext;
use crate::executor::physical_plan::DeriveHandle;
use crate::executor::IPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanMeta;
use crate::plans::GroupingSets;
use crate::IndexType;

/// Add dummy data before `GROUPING SETS`.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct AggregateExpand {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub group_bys: Vec<IndexType>,
    pub grouping_sets: GroupingSets,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for AggregateExpand {
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
        let input_schema = self.input.output_schema()?;
        let mut output_fields = input_schema.fields().clone();
        // Add virtual columns to group by.
        output_fields.reserve(self.group_bys.len() + 1);

        for (group_by, (actual, ty)) in self
            .group_bys
            .iter()
            .zip(self.grouping_sets.dup_group_items.iter())
        {
            // All group by columns will wrap nullable.
            let i = input_schema.index_of(&group_by.to_string())?;
            let f = &mut output_fields[i];
            debug_assert!(f.data_type() == ty || f.data_type().wrap_nullable() == *ty);
            *f = DataField::new(f.name(), f.data_type().wrap_nullable());
            let new_field = DataField::new(&actual.to_string(), ty.clone());
            output_fields.push(new_field);
        }

        output_fields.push(DataField::new(
            &self.grouping_sets.grouping_id_index.to_string(),
            DataType::Number(NumberDataType::UInt32),
        ));
        Ok(DataSchemaRefExt::create(output_fields))
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
        let sets = self
            .grouping_sets
            .sets
            .iter()
            .map(|set| {
                set.iter()
                    .map(|&index| ctx.metadata.column(index).name())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .map(|s| format!("({})", s))
            .collect::<Vec<_>>()
            .join(", ");

        let mut node_children = vec![
            FormatTreeNode::new(format!(
                "output columns: [{}]",
                format_output_columns(self.output_schema()?, &ctx.metadata, true)
            )),
            FormatTreeNode::new(format!("grouping sets: [{sets}]")),
        ];

        if let Some(info) = &self.stat_info {
            let items = plan_stats_info_to_format_tree(info);
            node_children.extend(items);
        }

        node_children.extend(children);
        Ok(FormatTreeNode::with_children(
            "AggregateExpand".to_string(),
            node_children,
        ))
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self
            .grouping_sets
            .sets
            .iter()
            .map(|set| {
                set.iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .map(|s| format!("({})", s))
            .collect::<Vec<_>>()
            .join(", "))
    }

    fn derive(&self, mut children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        let mut new_physical_plan = self.clone();
        assert_eq!(children.len(), 1);
        new_physical_plan.input = children.pop().unwrap();
        Box::new(new_physical_plan)
    }
}
