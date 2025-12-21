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
use std::sync::Arc;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_base::runtime::profile::ProfileLabel;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline::core::PlanProfile;
use databend_common_pipeline::core::PlanScope;
use databend_common_sql::Metadata;
use dyn_clone::DynClone;
use serde::Deserializer;
use serde::Serializer;
use serde::de::Error as DeError;
use serde_json::Value as JsonValue;

use crate::physical_plans::ExchangeSink;
use crate::physical_plans::MutationSource;
use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::SimplePhysicalFormat;
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

pub(crate) trait IPhysicalPlan: DynClone + Debug + Send + Sync + 'static {
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

    #[recursive::recursive]
    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        let mut children = vec![];
        for child in self.children() {
            children.push(child.formatter()?);
        }

        Ok(SimplePhysicalFormat::create(self.get_meta(), children))
    }

    /// Used to find data source info in a non-aggregation and single-table query plan.
    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        None
    }

    #[recursive::recursive]
    fn try_find_mutation_source(&self) -> Option<MutationSource> {
        for child in self.children() {
            if let Some(plan) = child.try_find_mutation_source() {
                return Some(plan);
            }
        }

        None
    }

    #[recursive::recursive]
    fn get_all_data_source(&self, sources: &mut Vec<(u32, Box<DataSourcePlan>)>) {
        for child in self.children() {
            child.get_all_data_source(sources);
        }
    }

    #[recursive::recursive]
    fn set_pruning_stats(&mut self, stats: &mut HashMap<u32, PartStatistics>) {
        for child in self.children_mut() {
            child.set_pruning_stats(stats)
        }
    }

    #[recursive::recursive]
    fn is_distributed_plan(&self) -> bool {
        self.children().any(|child| child.is_distributed_plan())
    }

    #[recursive::recursive]
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

macro_rules! define_physical_plan_impl {
    ( $( $(#[$meta:meta])? $variant:ident => $path:path ),+ $(,)? ) => {
        #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
        /// static dispatch replacement for typetag-based dynamic dispatch, performance improvement via reduced stack depth
        pub(crate) enum PhysicalPlanSerde {
            $( $(#[$meta])? $variant($path), )+
        }

        $( $(#[$meta])? impl From<$path> for PhysicalPlanSerde {
            fn from(v: $path) -> Self {
                PhysicalPlanSerde::$variant(v)
            }
        })+
    };
}

include!(concat!(env!("OUT_DIR"), "/physical_plan_impls.rs"));

include!(concat!(env!("OUT_DIR"), "/physical_plan_dispatch.rs"));

pub struct PhysicalPlan {
    inner: Box<PhysicalPlanSerde>,
}

dyn_clone::clone_trait_object!(IPhysicalPlan);

impl Clone for PhysicalPlan {
    #[recursive::recursive]
    fn clone(&self) -> Self {
        PhysicalPlan {
            inner: self.inner.clone(),
        }
    }
}

impl Debug for PhysicalPlan {
    #[recursive::recursive]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
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
        // Deserialize to JSON first to avoid backtracking failures in streaming deserializers.
        let value = JsonValue::deserialize(deserializer)?;
        let inner: PhysicalPlanSerde = serde_json::from_value(value).map_err(DeError::custom)?;

        Ok(PhysicalPlan {
            inner: Box::new(inner),
        })
    }
}

impl PhysicalPlan {
    #[allow(private_bounds)]
    pub fn new<T>(inner: T) -> PhysicalPlan
    where PhysicalPlanSerde: From<T> {
        PhysicalPlan {
            inner: Box::new(inner.into()),
        }
    }

    pub fn as_any(&self) -> &dyn Any {
        dispatch_plan_ref!(self, v => v.as_any())
    }

    pub fn get_meta(&self) -> &PhysicalPlanMeta {
        dispatch_plan_ref!(self, v => v.get_meta())
    }

    pub fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        dispatch_plan_mut!(self, v => v.get_meta_mut())
    }

    pub fn get_id(&self) -> u32 {
        dispatch_plan_ref!(self, v => v.get_id())
    }

    pub fn get_name(&self) -> String {
        dispatch_plan_ref!(self, v => v.get_name())
    }

    pub fn adjust_plan_id(&mut self, next_id: &mut u32) {
        dispatch_plan_mut!(self, v => v.adjust_plan_id(next_id))
    }

    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        dispatch_plan_ref!(self, v => v.output_schema())
    }

    pub fn children(&self) -> Box<dyn Iterator<Item = &'_ PhysicalPlan> + '_> {
        dispatch_plan_ref!(self, v => v.children())
    }

    pub fn children_mut(&mut self) -> Box<dyn Iterator<Item = &'_ mut PhysicalPlan> + '_> {
        dispatch_plan_mut!(self, v => v.children_mut())
    }

    pub fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        dispatch_plan_ref!(self, v => v.formatter())
    }

    pub fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        dispatch_plan_ref!(self, v => v.try_find_single_data_source())
    }

    pub fn try_find_mutation_source(&self) -> Option<MutationSource> {
        dispatch_plan_ref!(self, v => v.try_find_mutation_source())
    }

    pub fn get_all_data_source(&self, sources: &mut Vec<(u32, Box<DataSourcePlan>)>) {
        dispatch_plan_ref!(self, v => v.get_all_data_source(sources))
    }

    pub fn set_pruning_stats(&mut self, stats: &mut HashMap<u32, PartStatistics>) {
        dispatch_plan_mut!(self, v => v.set_pruning_stats(stats))
    }

    pub fn is_distributed_plan(&self) -> bool {
        dispatch_plan_ref!(self, v => v.is_distributed_plan())
    }

    pub fn is_warehouse_distributed_plan(&self) -> bool {
        dispatch_plan_ref!(self, v => v.is_warehouse_distributed_plan())
    }

    pub fn display_in_profile(&self) -> bool {
        dispatch_plan_ref!(self, v => v.display_in_profile())
    }

    pub fn get_desc(&self) -> Result<String> {
        dispatch_plan_ref!(self, v => v.get_desc())
    }

    pub fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        dispatch_plan_ref!(self, v => v.get_labels())
    }

    pub fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        dispatch_plan_ref!(self, v => v.derive(children))
    }

    pub fn build_pipeline(&self, builder: &mut PipelineBuilder) -> Result<()> {
        dispatch_plan_ref!(self, v => v.build_pipeline(builder))
    }

    pub fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        dispatch_plan_ref!(self, v => v.build_pipeline2(builder))
    }

    #[recursive::recursive]
    pub fn derive_with(&self, handle: &mut Box<dyn DeriveHandle>) -> PhysicalPlan {
        let mut children = vec![];
        for child in self.children() {
            children.push(child.derive_with(handle));
        }

        match handle.derive(self, children) {
            Ok(v) => v,
            Err(children) => self.derive(children),
        }
    }

    #[recursive::recursive]
    pub fn visit(&self, visitor: &mut Box<dyn PhysicalPlanVisitor>) -> Result<()> {
        for child in self.children() {
            child.visit(visitor)?;
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
            runtime_filter_reports: HashMap::new(),
        };

        self.formatter()?.format(&mut context)
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::backtrace::Backtrace;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use serde::ser::SerializeStruct;
    use serde_json::{self};

    use super::*;

    static STACK_DEPTH: AtomicUsize = AtomicUsize::new(0);
    static STACK_DELTA: AtomicUsize = AtomicUsize::new(0);
    static BASELINE_DEPTH: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone, Debug, serde::Deserialize)]
    pub(crate) struct StackDepthPlan {
        meta: PhysicalPlanMeta,
    }

    impl StackDepthPlan {
        fn new(name: impl Into<String>) -> Self {
            StackDepthPlan {
                meta: PhysicalPlanMeta::new(name),
            }
        }
    }

    impl serde::Serialize for StackDepthPlan {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where S: serde::Serializer {
            let depth = current_stack_depth();
            STACK_DEPTH.store(depth, Ordering::Relaxed);
            let baseline = BASELINE_DEPTH.load(Ordering::Relaxed);
            STACK_DELTA.store(depth.saturating_sub(baseline), Ordering::Relaxed);

            // Serialize in the same shape as a normal plan node.
            let mut state = serializer.serialize_struct("StackDepthPlan", 1)?;
            state.serialize_field("meta", &self.meta)?;
            state.end()
        }
    }

    impl IPhysicalPlan for StackDepthPlan {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get_meta(&self) -> &PhysicalPlanMeta {
            &self.meta
        }

        fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
            &mut self.meta
        }

        fn derive(&self, _children: Vec<PhysicalPlan>) -> PhysicalPlan {
            PhysicalPlan::new(self.clone())
        }

        fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
            let _ = builder;
            Ok(())
        }
    }

    // Used to compare the serialization stack depth of different versions
    #[test]
    fn typetag_serialize_stack_depth_is_measured() {
        STACK_DEPTH.store(0, Ordering::Relaxed);
        STACK_DELTA.store(0, Ordering::Relaxed);
        let baseline = current_stack_depth();
        BASELINE_DEPTH.store(baseline, Ordering::Relaxed);

        let plan = PhysicalPlan::new(StackDepthPlan::new("stack_depth_plan"));
        serde_json::to_vec(&plan).expect("serialize typetag plan");

        let depth = STACK_DEPTH.load(Ordering::Relaxed);
        let delta = STACK_DELTA.load(Ordering::Relaxed);

        assert!(depth > 0, "backtrace depth was not captured");
        assert!(
            delta > 0,
            "delta between typetag serialize and baseline should be > 0"
        );
        eprintln!(
            "typetag serialize stack depth: {}, delta from baseline: {}",
            depth, delta
        );
    }

    fn current_stack_depth() -> usize {
        Backtrace::force_capture()
            .to_string()
            .lines()
            .filter(|line| {
                line.trim_start()
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_digit())
            })
            .count()
    }
}
