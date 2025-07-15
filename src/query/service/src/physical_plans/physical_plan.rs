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
use std::any::TypeId;
use std::collections::HashMap;
use std::fmt::Debug;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_base::runtime::profile::get_statistics_desc;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::PlanProfile;
use databend_common_sql::Metadata;
use itertools::Itertools;

use crate::physical_plans::format::FormatContext;
use crate::physical_plans::MutationSource;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PhysicalPlanMeta {
    plan_id: u32,
    name: String,
}

impl PhysicalPlanMeta {
    pub fn new(name: impl Into<String>) -> PhysicalPlanMeta {
        PhysicalPlanMeta::with_plan_id(name, 0)
    }

    pub fn with_plan_id(name: impl Into<String>, plan_id: u32) -> PhysicalPlanMeta {
        PhysicalPlanMeta {
            plan_id,
            name: name.into(),
        }
    }
}

pub trait DeriveHandle: Send + Sync + 'static {
    fn as_any(&mut self) -> &mut dyn Any;

    fn derive(
        &mut self,
        v: &Box<dyn IPhysicalPlan>,
        children: Vec<Box<dyn IPhysicalPlan>>,
    ) -> std::result::Result<Box<dyn IPhysicalPlan>, Vec<Box<dyn IPhysicalPlan>>>;
}

#[typetag::serde]
pub trait IPhysicalPlan: Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn get_meta(&self) -> &PhysicalPlanMeta;

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta;

    // For methods with default implementations, the default implementation is usually sufficient.
    fn get_id(&self) -> u32 {
        self.get_meta().plan_id
    }

    fn get_name(&self) -> String {
        self.get_meta().name.clone()
    }

    /// Adjust the plan_id of the physical plan.
    /// This function will assign a unique plan_id to each physical plan node in a top-down manner.
    /// Which means the plan_id of a node is always greater than the plan_id of its parent node.
    // #[recursive::recursive]
    fn adjust_plan_id(&mut self, next_id: &mut u32) {
        self.get_meta_mut().plan_id = *next_id;
        *next_id += 1;

        for child in self.children_mut() {
            child.adjust_plan_id(next_id);
        }
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        match self.children().next() {
            None => Ok(DataSchemaRef::default()),
            Some(child) => child.output_schema(),
        }
    }

    fn children(&self) -> Box<dyn Iterator<Item=&'_ Box<dyn IPhysicalPlan>> + '_> {
        Box::new(std::iter::empty())
    }

    fn children_mut(&mut self) -> Box<dyn Iterator<Item=&'_ mut Box<dyn IPhysicalPlan>> + '_> {
        Box::new(std::iter::empty())
    }

    fn to_format_node(
        &self,
        _ctx: &mut FormatContext<'_>,
        children: Vec<FormatTreeNode<String>>,
    ) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::with_children(self.get_name(), children))
    }

    /// Used to find data source info in a non-aggregation and single-table query plan.
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        None
    }

    // #[recursive::recursive]
    fn try_find_mutation_source(&self) -> Option<MutationSource> {
        for child in self.children() {
            if let Some(plan) = child.try_find_mutation_source() {
                return Some(plan);
            }
        }

        None
    }

    // #[recursive::recursive]
    fn get_all_data_source(&self, sources: &mut Vec<(u32, Box<DataSourcePlan>)>) {
        for child in self.children() {
            child.get_all_data_source(sources);
        }
    }

    // #[recursive::recursive]
    fn set_pruning_stats(&mut self, stats: &mut HashMap<u32, PartStatistics>) {
        for child in self.children_mut() {
            child.set_pruning_stats(stats)
        }
    }

    // #[recursive::recursive]
    fn is_distributed_plan(&self) -> bool {
        self.children().any(|child| child.is_distributed_plan())
    }

    // #[recursive::recursive]
    fn is_warehouse_distributed_plan(&self) -> bool {
        self.children()
            .any(|child| child.is_warehouse_distributed_plan())
    }

    fn display_in_profile(&self) -> bool {
        true
    }

    fn get_desc(&self) -> Result<String> {
        Ok(String::new())
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        Ok(HashMap::new())
    }

    fn derive(&self, children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan>;

    fn build_pipeline(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.build_pipeline2(builder)
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let _ = builder;
        Err(ErrorCode::Unimplemented(format!(
            "UnImplement build_pipeline method for {:?}",
            self.get_name()
        )))
    }
}

pub trait PhysicalPlanVisitor: Send + Sync + 'static {
    fn as_any(&mut self) -> &mut dyn Any;

    fn visit(&mut self, plan: &Box<dyn IPhysicalPlan>) -> Result<()>;
}

pub trait PhysicalPlanDynExt {
    fn format(
        &self,
        metadata: &Metadata,
        profs: HashMap<u32, PlanProfile>,
    ) -> Result<FormatTreeNode<String>> {
        let mut context = FormatContext {
            metadata,
            scan_id_to_runtime_filters: HashMap::new(),
        };

        self.to_format_tree(&profs, &mut context)
    }

    fn to_format_tree(
        &self,
        profs: &HashMap<u32, PlanProfile>,
        ctx: &mut FormatContext<'_>,
    ) -> Result<FormatTreeNode<String>>;

    fn downcast_ref<To: 'static>(&self) -> Option<&To>;

    fn downcast_mut_ref<To: 'static>(&mut self) -> Option<&mut To>;

    fn derive_with(&self, handle: &mut Box<dyn DeriveHandle>) -> Box<dyn IPhysicalPlan>;

    fn visit(&self, visitor: &mut Box<dyn PhysicalPlanVisitor>) -> Result<()>;
}

impl PhysicalPlanDynExt for Box<dyn IPhysicalPlan + 'static> {
    fn to_format_tree(
        &self,
        profs: &HashMap<u32, PlanProfile>,
        ctx: &mut FormatContext<'_>,
    ) -> Result<FormatTreeNode<String>> {
        let mut children = Vec::with_capacity(4);
        for child in self.children() {
            children.push(child.to_format_tree(profs, ctx)?);
        }

        let mut format_tree_node = self.to_format_node(ctx, children)?;

        if let Some(prof) = profs.get(&self.get_id()) {
            let mut children = Vec::with_capacity(format_tree_node.children.len() + 10);
            for (_, desc) in get_statistics_desc().iter() {
                if desc.display_name != "output rows" {
                    continue;
                }
                if prof.statistics[desc.index] != 0 {
                    children.push(FormatTreeNode::new(format!(
                        "{}: {}",
                        desc.display_name.to_lowercase(),
                        desc.human_format(prof.statistics[desc.index])
                    )));
                }
                break;
            }

            children.append(&mut format_tree_node.children);
            format_tree_node.children = children;
        }

        Ok(format_tree_node)
    }

    fn downcast_ref<To: 'static>(&self) -> Option<&To> {
        self.as_any().downcast_ref()
    }

    fn downcast_mut_ref<To: 'static>(&mut self) -> Option<&mut To> {
        unsafe {
            match self.downcast_ref::<To>() {
                None => None,
                #[allow(invalid_reference_casting)]
                Some(v) => Some(&mut *(v as *const To as *mut To))
            }
        }
    }

    fn derive_with(&self, handle: &mut Box<dyn DeriveHandle>) -> Box<dyn IPhysicalPlan> {
        let mut children = vec![];
        for child in self.children() {
            children.push(child.derive_with(handle));
        }

        match handle.derive(self, children) {
            Ok(v) => v,
            Err(children) => self.derive(children),
        }
    }

    fn visit(&self, visitor: &mut Box<dyn PhysicalPlanVisitor>) -> Result<()> {
        for child in self.children() {
            child.visit(visitor)?;
        }

        visitor.visit(self)
    }
}

impl Clone for Box<dyn IPhysicalPlan> {
    fn clone(&self) -> Self {
        let mut children = vec![];
        for child in self.children() {
            children.push(child.clone());
        }

        self.derive(children)
    }
}
