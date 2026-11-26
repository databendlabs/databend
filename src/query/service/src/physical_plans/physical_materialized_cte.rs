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
use std::collections::HashSet;

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_sql::Symbol;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::optimizer::ir::PExpr;

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::MaterializeCTERef;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::Sequence;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::MaterializedCTEFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::display_materialized_cte_name;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::MaterializedCteSink;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MaterializedCTE {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
    pub input: PhysicalPlan,
    pub cte_name: String,
    pub cte_output_columns: Option<Vec<Symbol>>,
    pub ref_count: usize,
    pub channel_size: Option<usize>,
    pub meta: PhysicalPlanMeta,
}

#[typetag::serde]
impl IPhysicalPlan for MaterializedCTE {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(MaterializedCTEFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(MaterializedCTE {
            plan_id: self.plan_id,
            stat_info: self.stat_info.clone(),
            input,
            cte_name: self.cte_name.clone(),
            cte_output_columns: self.cte_output_columns.clone(),
            ref_count: self.ref_count,
            channel_size: self.channel_size,
            meta: self.meta.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let input_schema = self.input.output_schema()?;
        if let Some(output_columns) = &self.cte_output_columns {
            let mut projections = Vec::with_capacity(output_columns.len());
            for index in output_columns {
                projections.push(input_schema.index_of(index.to_string().as_str())?);
            }
            let num_input_columns = input_schema.num_fields();
            builder.main_pipeline.add_transformer(|| {
                CompoundBlockOperator::new(
                    vec![BlockOperator::Project {
                        projection: projections.clone(),
                    }],
                    builder.func_ctx.clone(),
                    num_input_columns,
                )
            });
        }

        builder.main_pipeline.try_resize(1)?;
        let tx = builder.ctx.get_materialized_cte_senders(
            &self.cte_name,
            self.ref_count,
            self.channel_size,
        );
        let memory_settings =
            databend_common_pipeline_transforms::MemorySettings::from_materialized_cte_settings(
                &builder.ctx,
                &builder.settings,
            )?;
        builder.main_pipeline.add_sink(|input| {
            MaterializedCteSink::create(
                builder.ctx.clone(),
                input,
                tx.clone(),
                &builder.settings,
                memory_settings.clone(),
            )
        })
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_materialized_cte(
        &mut self,
        s_expr: &PExpr,
        materialized_cte: &databend_common_sql::plans::MaterializedCTE,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let required = self
            .cte_required_columns
            .get(&materialized_cte.cte_name)
            .unwrap()
            .clone();
        let cte_output_columns = Some(required.iter().copied().collect());
        let input = self.build_physical_plan(s_expr.child(0)?, required).await?;
        // Sequence builds the producer before its consumers. Preserve its actual
        // types: rewrites can change nullability without allocating new symbols.
        self.cte_schemas
            .insert(materialized_cte.cte_name.clone(), input.output_schema()?);
        Ok(PhysicalPlan::new(MaterializedCTE {
            plan_id: 0,
            stat_info: Some(stat_info),
            input,
            cte_name: materialized_cte.cte_name.clone(),
            cte_output_columns,
            ref_count: materialized_cte.ref_count,
            channel_size: materialized_cte.channel_size,
            meta: PhysicalPlanMeta::new("MaterializedCTE"),
        }))
    }
}

/// Gives every producer in the plan a name of its own.
///
/// A producer registers its channels under its name and each consumer takes one of them
/// by that name. One bound subquery can occur twice in a plan, e.g. behind a SELECT alias
/// that is referenced again, or as an argument that a SQL function uses twice. Both copies
/// then produce their CTEs under the same names: the later producer replaces the channels
/// of the earlier one, and the consumers of both copies are counted together.
///
/// The consumers of a producer are on the right side of its `Sequence`, so a renamed
/// producer takes the consumers there with it. A later name keeps the display name, which
/// EXPLAIN shows.
pub fn rename_duplicate_materialized_ctes(plan: &mut PhysicalPlan) {
    let mut names = HashSet::new();
    collect_cte_producer_names(plan, &mut names);
    let mut produced = HashSet::new();
    rename_duplicate_producers(plan, &mut names, &mut produced, &mut Vec::new());
}

#[recursive::recursive]
fn collect_cte_producer_names(plan: &PhysicalPlan, names: &mut HashSet<String>) {
    if let Some(producer) = MaterializedCTE::from_physical_plan(plan) {
        names.insert(producer.cte_name.clone());
    }
    for child in plan.children() {
        collect_cte_producer_names(child, names);
    }
}

/// `scopes` maps the name of each enclosing producer to the name it now has.
#[recursive::recursive]
fn rename_duplicate_producers(
    plan: &mut PhysicalPlan,
    names: &mut HashSet<String>,
    produced: &mut HashSet<String>,
    scopes: &mut Vec<(String, String)>,
) {
    if let Some(consumer) = MaterializeCTERef::from_mut_physical_plan(plan) {
        if let Some((_, name)) = scopes
            .iter()
            .rev()
            .find(|(original, _)| *original == consumer.cte_name)
        {
            consumer.cte_name = name.clone();
        }
        return;
    }

    if let Some(sequence) = Sequence::from_mut_physical_plan(plan)
        && let Some(producer) = MaterializedCTE::from_mut_physical_plan(&mut sequence.left)
    {
        rename_duplicate_producers(&mut producer.input, names, produced, scopes);
        let original = producer.cte_name.clone();
        if !produced.insert(original.clone()) {
            producer.cte_name = unique_cte_name(&original, names);
            produced.insert(producer.cte_name.clone());
        }
        scopes.push((original, producer.cte_name.clone()));
        rename_duplicate_producers(&mut sequence.right, names, produced, scopes);
        scopes.pop();
        return;
    }

    for child in plan.children_mut() {
        rename_duplicate_producers(child, names, produced, scopes);
    }
}

fn unique_cte_name(name: &str, names: &mut HashSet<String>) -> String {
    let display_name = display_materialized_cte_name(name).to_string();
    let mut id = 0;
    loop {
        let name = format!("__materialized_cte_{id}_{display_name}");
        if names.insert(name.clone()) {
            return name;
        }
        id += 1;
    }
}

/// Sets each producer's `ref_count` to the number of consumers left in the physical plan.
///
/// The producer opens one channel per counted consumer and sends every block to each of
/// them, while each consumer takes one channel when its pipeline is built. The logical
/// count can be too high: physical building may drop a consumer, e.g. an aggregate with
/// no keys and no used functions becomes a `ConstantTableScan` and its input, a
/// consumer, is never built. Nobody reads that extra channel, so a bounded one fills up
/// and blocks the producer and every other consumer.
pub fn recount_materialized_cte_refs(plan: &mut PhysicalPlan) {
    let mut ref_counts = HashMap::new();
    count_cte_consumers(plan, &mut ref_counts);
    set_cte_ref_counts(plan, &ref_counts);
}

#[recursive::recursive]
fn count_cte_consumers(plan: &PhysicalPlan, ref_counts: &mut HashMap<String, usize>) {
    if let Some(consumer) = MaterializeCTERef::from_physical_plan(plan) {
        *ref_counts.entry(consumer.cte_name.clone()).or_default() += 1;
    }
    for child in plan.children() {
        count_cte_consumers(child, ref_counts);
    }
}

#[recursive::recursive]
fn set_cte_ref_counts(plan: &mut PhysicalPlan, ref_counts: &HashMap<String, usize>) {
    if let Some(producer) = MaterializedCTE::from_mut_physical_plan(plan) {
        producer.ref_count = ref_counts.get(&producer.cte_name).copied().unwrap_or(0);
    }
    for child in plan.children_mut() {
        set_cte_ref_counts(child, ref_counts);
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::DataSchemaRefExt;

    use super::*;

    fn producer(name: &str, input: PhysicalPlan) -> PhysicalPlan {
        PhysicalPlan::new(MaterializedCTE {
            plan_id: 0,
            stat_info: None,
            input,
            cte_name: name.to_string(),
            cte_output_columns: None,
            ref_count: 0,
            channel_size: Some(2),
            meta: PhysicalPlanMeta::new("MaterializedCTE"),
        })
    }

    fn consumer(name: &str) -> PhysicalPlan {
        PhysicalPlan::new(MaterializeCTERef {
            plan_id: 0,
            stat_info: None,
            cte_name: name.to_string(),
            cte_schema: DataSchemaRefExt::create(vec![]),
            meta: PhysicalPlanMeta::new("MaterializeCTERef"),
        })
    }

    fn sequence(left: PhysicalPlan, right: PhysicalPlan) -> PhysicalPlan {
        PhysicalPlan::new(Sequence {
            plan_id: 0,
            stat_info: None,
            left,
            right,
            meta: PhysicalPlanMeta::new("Sequence"),
        })
    }

    fn producers(plan: &PhysicalPlan, out: &mut Vec<(String, usize)>) {
        if let Some(producer) = MaterializedCTE::from_physical_plan(plan) {
            out.push((producer.cte_name.clone(), producer.ref_count));
        }
        for child in plan.children() {
            producers(child, out);
        }
    }

    fn consumers(plan: &PhysicalPlan, out: &mut Vec<String>) {
        if let Some(consumer) = MaterializeCTERef::from_physical_plan(plan) {
            out.push(consumer.cte_name.clone());
        }
        for child in plan.children() {
            consumers(child, out);
        }
    }

    #[test]
    fn renames_duplicate_producers_with_their_consumers() {
        // x and y read the base CTE; a second x, nested in the first one's scope, has a
        // consumer of its own. `__materialized_cte_0_x` is already taken.
        let mut plan = sequence(
            producer("x", consumer("base")),
            sequence(
                producer("__materialized_cte_0_x", consumer("base")),
                sequence(
                    producer("y", consumer("x")),
                    sequence(
                        producer("x", consumer("y")),
                        sequence(producer("x", consumer("x")), consumer("x")),
                    ),
                ),
            ),
        );

        rename_duplicate_materialized_ctes(&mut plan);
        recount_materialized_cte_refs(&mut plan);

        let mut names = vec![];
        producers(&plan, &mut names);
        assert_eq!(names, vec![
            ("x".to_string(), 1),
            ("__materialized_cte_0_x".to_string(), 0),
            ("y".to_string(), 1),
            ("__materialized_cte_1_x".to_string(), 1),
            ("__materialized_cte_2_x".to_string(), 1),
        ]);

        let mut refs = vec![];
        consumers(&plan, &mut refs);
        assert_eq!(refs, vec![
            "base".to_string(),
            "base".to_string(),
            "x".to_string(),
            "y".to_string(),
            "__materialized_cte_1_x".to_string(),
            "__materialized_cte_2_x".to_string(),
        ]);
    }
}
