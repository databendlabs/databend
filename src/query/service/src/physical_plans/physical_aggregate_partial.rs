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
use std::collections::HashMap;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
#[allow(unused_imports)]
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_sql::IndexType;
use itertools::Itertools;

use super::AggregateFunctionDesc;
use super::SortDesc;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::plan_stats_info_to_format_tree;
use crate::physical_plans::format::pretty_display_agg_desc;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct AggregatePartial {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub group_by: Vec<IndexType>,
    pub agg_funcs: Vec<AggregateFunctionDesc>,
    pub enable_experimental_aggregate_hashtable: bool,
    pub group_by_display: Vec<String>,

    // Order by keys if keys are subset of group by key, then we can use rank to filter data in previous
    pub rank_limit: Option<(Vec<SortDesc>, usize)>,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for AggregatePartial {
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

        let mut fields = Vec::with_capacity(self.agg_funcs.len() + self.group_by.len());

        fields.extend(self.agg_funcs.iter().map(|func| {
            let name = func.output_column.to_string();
            DataField::new(&name, DataType::Binary)
        }));

        for (idx, field) in self.group_by.iter().zip(
            self.group_by
                .iter()
                .map(|index| input_schema.field_with_name(&index.to_string())),
        ) {
            fields.push(DataField::new(&idx.to_string(), field?.data_type().clone()));
        }

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
        let group_by = self
            .group_by
            .iter()
            .map(|&index| ctx.metadata.column(index).name())
            .join(", ");

        let agg_funcs = self
            .agg_funcs
            .iter()
            .map(|agg| pretty_display_agg_desc(agg, &ctx.metadata))
            .collect::<Vec<_>>()
            .join(", ");

        let mut node_children = vec![
            FormatTreeNode::new(format!("group by: [{group_by}]")),
            FormatTreeNode::new(format!("aggregate functions: [{agg_funcs}]")),
        ];

        if let Some(info) = &self.stat_info {
            node_children.extend(plan_stats_info_to_format_tree(info));
        }

        if let Some((_, r)) = &self.rank_limit {
            node_children.push(FormatTreeNode::new(format!("rank limit: {r}")));
        }

        node_children.extend(children);

        Ok(FormatTreeNode::with_children(
            "AggregatePartial".to_string(),
            node_children,
        ))
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self.agg_funcs.iter().map(|x| x.display.clone()).join(", "))
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        let mut labels = HashMap::with_capacity(2);

        if !self.group_by_display.is_empty() {
            labels.insert(String::from("Grouping keys"), self.group_by_display.clone());
        }

        if !self.agg_funcs.is_empty() {
            labels.insert(
                String::from("Aggregate Functions"),
                self.agg_funcs.iter().map(|x| x.display.clone()).collect(),
            );
        }

        Ok(labels)
    }

    fn derive(&self, mut children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        let mut new_physical_plan = self.clone();
        assert_eq!(children.len(), 1);
        new_physical_plan.input = children.pop().unwrap();
        Box::new(new_physical_plan)
    }
}
