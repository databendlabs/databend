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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_base::runtime::profile::ProfileLabel;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_core::PlanProfile;
use databend_common_pipeline_core::PlanScope;
use databend_common_sql::Metadata;
use dyn_clone::DynClone;
use serde::Deserializer;
use serde::Serializer;

use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::SimplePhysicalFormat;
use crate::physical_plans::ExchangeSink;
use crate::physical_plans::MutationSource;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PhysicalPlanMeta {
    pub plan_id: u32,
    pub name: String,
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
        v: &PhysicalPlan,
        children: Vec<PhysicalPlan>,
    ) -> std::result::Result<PhysicalPlan, Vec<PhysicalPlan>>;
}

#[typetag::serde]
pub trait IPhysicalPlan: DynClone + Debug + Send + Sync + 'static {
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
    #[recursive::recursive]
    fn adjust_plan_id(&mut self, next_id: &mut u32) {
        self.get_meta_mut().plan_id = *next_id;
        *next_id += 1;

        for child in self.children_mut() {
            child.adjust_plan_id(next_id);
        }
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        match self.children().next() {
            None => Ok(DataSchemaRef::default()),
            Some(child) => child.output_schema(),
        }
    }

    fn children(&self) -> Box<dyn Iterator<Item = &'_ PhysicalPlan> + '_> {
        Box::new(std::iter::empty())
    }

    fn children_mut(&mut self) -> Box<dyn Iterator<Item = &'_ mut PhysicalPlan> + '_> {
        Box::new(std::iter::empty())
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        self.formatter_with_depth(0)
    }

    fn formatter_with_depth(&self, depth: usize) -> Result<Box<dyn PhysicalFormat + '_>> {
        const MAX_DEPTH: usize = 4096; // Consistent with query binder depth limit
        if depth > MAX_DEPTH {
            return Err(ErrorCode::Internal(format!(
                "Physical plan formatting depth exceeded maximum limit of {}", 
                MAX_DEPTH
            )));
        }
        
        let mut children = vec![];
        for child in self.children() {
            children.push(child.formatter_with_depth(depth + 1)?);
        }

        Ok(SimplePhysicalFormat::create(self.get_meta(), children))
    }

    /// Used to find data source info in a non-aggregation and single-table query plan.
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        None
    }

    fn try_find_mutation_source(&self) -> Option<MutationSource> {
        self.try_find_mutation_source_with_depth(0)
    }

    fn try_find_mutation_source_with_depth(&self, depth: usize) -> Option<MutationSource> {
        const MAX_DEPTH: usize = 4096;
        if depth > MAX_DEPTH {
            return None; // Prevent stack overflow, return None as safe fallback
        }
        
        for child in self.children() {
            if let Some(plan) = child.try_find_mutation_source_with_depth(depth + 1) {
                return Some(plan);
            }
        }

        None
    }

    fn get_all_data_source(&self, sources: &mut Vec<(u32, Box<DataSourcePlan>)>) {
        self.get_all_data_source_with_depth(sources, 0);
    }

    fn get_all_data_source_with_depth(&self, sources: &mut Vec<(u32, Box<DataSourcePlan>)>, depth: usize) {
        const MAX_DEPTH: usize = 4096;
        if depth > MAX_DEPTH {
            return; // Prevent stack overflow
        }
        
        for child in self.children() {
            child.get_all_data_source_with_depth(sources, depth + 1);
        }
    }

    fn set_pruning_stats(&mut self, stats: &mut HashMap<u32, PartStatistics>) {
        self.set_pruning_stats_with_depth(stats, 0);
    }

    fn set_pruning_stats_with_depth(&mut self, stats: &mut HashMap<u32, PartStatistics>, depth: usize) {
        const MAX_DEPTH: usize = 4096;
        if depth > MAX_DEPTH {
            return; // Prevent stack overflow
        }
        
        for child in self.children_mut() {
            child.set_pruning_stats_with_depth(stats, depth + 1);
        }
    }

    fn is_distributed_plan(&self) -> bool {
        self.is_distributed_plan_with_depth(0)
    }

    fn is_distributed_plan_with_depth(&self, depth: usize) -> bool {
        const MAX_DEPTH: usize = 4096;
        if depth > MAX_DEPTH {
            return false; // Safe default to prevent stack overflow
        }
        
        self.children().any(|child| child.is_distributed_plan_with_depth(depth + 1))
    }

    fn is_warehouse_distributed_plan(&self) -> bool {
        self.is_warehouse_distributed_plan_with_depth(0)
    }

    fn is_warehouse_distributed_plan_with_depth(&self, depth: usize) -> bool {
        const MAX_DEPTH: usize = 4096;
        if depth > MAX_DEPTH {
            return false; // Safe default to prevent stack overflow
        }
        
        self.children()
            .any(|child| child.is_warehouse_distributed_plan_with_depth(depth + 1))
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

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan;

    #[recursive::recursive]
    fn build_pipeline(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let is_exchange_sink = self.as_any().downcast_ref::<ExchangeSink>().is_some();
        builder.is_exchange_stack.push(is_exchange_sink);

        if !self.display_in_profile() {
            self.build_pipeline2(builder)?;
            builder.is_exchange_stack.pop();
            return Ok(());
        }

        let desc = self.get_desc()?;
        let plan_labels = self.get_labels()?;
        let mut profile_labels = Vec::with_capacity(plan_labels.len());
        for (name, value) in plan_labels {
            profile_labels.push(ProfileLabel::create(name, value));
        }

        let scope = PlanScope::create(
            self.get_id(),
            self.get_name(),
            Arc::new(desc),
            Arc::new(profile_labels),
        );

        let _guard = scope.enter_scope_guard();
        self.build_pipeline2(builder)?;
        builder.is_exchange_stack.pop();
        Ok(())
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

    fn visit(&mut self, plan: &PhysicalPlan) -> Result<()>;
}

pub trait VisitorCast {
    fn from_visitor(x: &mut Box<dyn PhysicalPlanVisitor>) -> &mut Self;
}

impl<T: PhysicalPlanVisitor> VisitorCast for T {
    fn from_visitor(x: &mut Box<dyn PhysicalPlanVisitor>) -> &mut T {
        match x.as_any().downcast_mut::<T>() {
            Some(x) => x,
            None => unreachable!(),
        }
    }
}

pub trait PhysicalPlanCast {
    fn check_physical_plan(plan: &PhysicalPlan) -> bool;

    fn from_physical_plan(plan: &PhysicalPlan) -> Option<&Self>;

    fn from_mut_physical_plan(plan: &mut PhysicalPlan) -> Option<&mut Self>;
}

impl<T: IPhysicalPlan> PhysicalPlanCast for T {
    fn check_physical_plan(plan: &PhysicalPlan) -> bool {
        plan.as_any().downcast_ref::<T>().is_some()
    }

    fn from_physical_plan(plan: &PhysicalPlan) -> Option<&T> {
        plan.as_any().downcast_ref()
    }

    fn from_mut_physical_plan(plan: &mut PhysicalPlan) -> Option<&mut T> {
        unsafe {
            match T::from_physical_plan(plan) {
                None => None,
                #[allow(invalid_reference_casting)]
                Some(v) => Some(&mut *(v as *const T as *mut T)),
            }
        }
    }
}

pub struct PhysicalPlan {
    inner: Box<dyn IPhysicalPlan + 'static>,
}

dyn_clone::clone_trait_object!(IPhysicalPlan);

impl Clone for PhysicalPlan {
    fn clone(&self) -> Self {
        PhysicalPlan {
            inner: self.inner.clone(),
        }
    }
}

impl Debug for PhysicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Use a simple format to avoid potential recursion issues
        write!(f, "PhysicalPlan({})", self.get_name())
    }
}

impl Deref for PhysicalPlan {
    type Target = dyn IPhysicalPlan;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl DerefMut for PhysicalPlan {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.deref_mut()
    }
}

impl serde::Serialize for PhysicalPlan {
    #[recursive::recursive]
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        self.inner.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for PhysicalPlan {
    #[recursive::recursive]
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Ok(PhysicalPlan {
            inner: Box::<dyn IPhysicalPlan + 'static>::deserialize(deserializer)?,
        })
    }
}

impl PhysicalPlan {
    pub fn new<T: IPhysicalPlan + 'static>(inner: T) -> PhysicalPlan {
        PhysicalPlan {
            inner: Box::new(inner),
        }
    }

    pub fn derive_with(&self, handle: &mut Box<dyn DeriveHandle>) -> PhysicalPlan {
        self.derive_with_depth(handle, 0)
    }

    fn derive_with_depth(&self, handle: &mut Box<dyn DeriveHandle>, depth: usize) -> PhysicalPlan {
        const MAX_DEPTH: usize = 4096;
        if depth > MAX_DEPTH {
            // Return a copy of self as safe fallback to prevent stack overflow
            return self.clone();
        }
        
        let mut children = vec![];
        for child in self.children() {
            children.push(child.derive_with_depth(handle, depth + 1));
        }

        match handle.derive(self, children) {
            Ok(v) => v,
            Err(children) => self.derive(children),
        }
    }

    pub fn visit(&self, visitor: &mut Box<dyn PhysicalPlanVisitor>) -> Result<()> {
        self.visit_with_depth(visitor, 0)
    }

    fn visit_with_depth(&self, visitor: &mut Box<dyn PhysicalPlanVisitor>, depth: usize) -> Result<()> {
        const MAX_DEPTH: usize = 4096;
        if depth > MAX_DEPTH {
            return Err(ErrorCode::Internal(format!(
                "Physical plan visit depth exceeded maximum limit of {}", 
                MAX_DEPTH
            )));
        }
        
        for child in self.children() {
            child.visit_with_depth(visitor, depth + 1)?;
        }

        visitor.visit(self)
    }

    pub fn format(
        &self,
        metadata: &Metadata,
        profs: HashMap<u32, PlanProfile>,
    ) -> Result<FormatTreeNode<String>> {
        let mut context = FormatContext {
            profs,
            metadata,
            scan_id_to_runtime_filters: HashMap::new(),
        };

        self.formatter()?.format(&mut context)
    }
}
