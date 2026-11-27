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

//! Runtime filters on the producer of a materialized CTE.
//!
//! A filter on the producer changes the rows that every consumer reads, so it
//! must keep every row that some consumer still needs. A consumer is covered
//! by a hash join above it that drops the consumer rows whose key is not among
//! the join's build keys. When every consumer is covered on the same producer
//! column, the producer can keep only the rows whose key is in the union of
//! the covering joins' build keys. Different consumers may be covered by
//! different joins.
//!
//! The producer scans wait for that union, so the build side of a covering
//! join must not depend on the producer. Otherwise the join waits for a
//! consumer that waits for the producer.

use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::conversion::classify_conversion;
use databend_common_expression::types::DataType;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::MetadataRef;
use databend_common_sql::Symbol;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::optimizer::ir::StatContext;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::PhysicalRelOperator;
use databend_common_sql::plans::ScalarExpr;

use super::builder::normalize_probe_target;
use super::builder::supported_join_type_for_runtime_filter;
use super::cte_targets::find_probe_targets;
use super::cte_targets::probe_child_is_safe;
use super::types::resolve_runtime_filter_probe_expr;

/// A probe expression and the scan that applies it.
pub type ScanProbeTarget = (RemoteExpr<String>, usize);

/// Identity of a join node while one physical plan is being built.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct JoinId(usize);

impl JoinId {
    pub fn of(s_expr: &PExpr) -> Self {
        JoinId(std::ptr::from_ref(s_expr) as usize)
    }
}

/// One equi-condition of one join, whose build keys feed a CTE filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CteFilterSource {
    pub join: JoinId,
    pub condition: usize,
}

/// A filter planned for the producer of a materialized CTE.
#[derive(Clone, Debug)]
pub struct PlannedCteFilter {
    pub cte_name: String,
    /// Producer scans that apply the filter.
    pub probe_targets: Vec<ScanProbeTarget>,
    /// Joins whose build keys form the filter. Every consumer is covered by
    /// at least one of them.
    pub sources: Vec<CteFilterSource>,
}

#[derive(Default)]
pub struct CteFilterPlan {
    pub filters: Vec<PlannedCteFilter>,
    /// Subtrees that the producer waits for through `filters`.
    pub producer_waits: Vec<PExpr>,
}

pub struct CteFilterPlanner<'a> {
    pub metadata: &'a MetadataRef,
    pub func_ctx: &'a FunctionContext,
    pub stat_ctx: &'a StatContext,
    /// Producer input of every materialized CTE in the query, by CTE name.
    pub producers: &'a HashMap<String, PExpr>,
    /// Subtrees that the producer of a CTE already waits for through its
    /// own filters, by CTE name.
    pub producer_waits: &'a HashMap<String, Vec<PExpr>>,
}

struct Candidate {
    source: CteFilterSource,
    build_rows: f64,
    waits: Vec<PExpr>,
}

/// The node above a plan node, and which of its children leads to that node.
type Ancestor<'a> = (&'a PExpr, usize);

impl CteFilterPlanner<'_> {
    /// Plans filters for the CTE defined by `sequence`.
    pub fn plan(&self, sequence: &PExpr) -> Result<CteFilterPlan> {
        let cte_expr = sequence.child(0)?;
        let PhysicalRelOperator::MaterializedCTE(cte) = cte_expr.plan() else {
            return Ok(CteFilterPlan::default());
        };
        // Each node of a distributed plan holds only part of a build side.
        if contains_exchange(sequence) {
            return Ok(CteFilterPlan::default());
        }

        let mut consumers = Vec::new();
        collect_consumers(
            sequence.child(1)?,
            &cte.cte_name,
            &mut Vec::new(),
            &mut consumers,
        );
        if consumers.is_empty() || consumers.len() != cte.ref_count {
            return Ok(CteFilterPlan::default());
        }

        let mut coverage = Vec::with_capacity(consumers.len());
        for (consumer, ancestors) in &consumers {
            coverage.push(self.consumer_candidates(&cte.cte_name, consumer, ancestors)?);
        }

        let mut columns = coverage[0]
            .keys()
            .filter(|column| coverage.iter().all(|c| c.contains_key(column)))
            .copied()
            .collect::<Vec<_>>();
        columns.sort();

        let producer = cte_expr.child(0)?;
        let mut plan = CteFilterPlan::default();
        for column in columns {
            let Some(probe_targets) = self.producer_targets(producer, column)? else {
                continue;
            };
            let mut sources = Vec::new();
            for candidate in coverage.iter().map(|c| &c[&column]) {
                if !sources.contains(&candidate.source) {
                    sources.push(candidate.source);
                    plan.producer_waits.extend(candidate.waits.iter().cloned());
                }
            }
            plan.filters.push(PlannedCteFilter {
                cte_name: cte.cte_name.clone(),
                probe_targets,
                sources,
            });
        }
        Ok(plan)
    }

    /// For each producer column, the cheapest join that covers the consumer.
    fn consumer_candidates<'a>(
        &self,
        cte_name: &str,
        consumer: &PExpr,
        ancestors: &[Ancestor<'a>],
    ) -> Result<HashMap<Symbol, Candidate>> {
        let PhysicalRelOperator::MaterializedCTERef(cte_ref) = consumer.plan() else {
            return Ok(HashMap::new());
        };
        // Columns that still hold the value of a producer column.
        let mut columns = cte_ref.column_mapping.clone();
        let mut candidates: HashMap<Symbol, Candidate> = HashMap::new();

        for depth in (0..ancestors.len()).rev() {
            let (parent, child_index) = ancestors[depth];
            if let PhysicalRelOperator::Join(join) = parent.plan() {
                if child_index == 0 && can_build_filter(join) {
                    for (condition, equi) in join.equi_conditions.iter().enumerate() {
                        if equi.is_null_equal {
                            continue;
                        }
                        let Some(producer_column) = probe_key_column(equi.canonical_keys().0)
                            .and_then(|column| columns.get(&column))
                            .copied()
                        else {
                            continue;
                        };
                        let Some(candidate) =
                            self.covering_source(cte_name, parent, condition, &ancestors[..depth])?
                        else {
                            continue;
                        };
                        if candidates
                            .get(&producer_column)
                            .is_none_or(|best| candidate.build_rows < best.build_rows)
                        {
                            candidates.insert(producer_column, candidate);
                        }
                    }
                }
            }
            // A join can drop the consumer rows it would not match only if
            // every operator between them commutes with that filter.
            if !probe_child_is_safe(parent.plan(), child_index) {
                break;
            }
            track_columns(parent.plan(), &mut columns);
            if columns.is_empty() {
                break;
            }
        }
        Ok(candidates)
    }

    /// A source whose build keys contain the build keys of `join`.
    fn covering_source<'a>(
        &self,
        cte_name: &str,
        join: &'a PExpr,
        condition: usize,
        ancestors: &[Ancestor<'a>],
    ) -> Result<Option<Candidate>> {
        if let Some(candidate) = self.independent_source(cte_name, join, condition, ancestors)? {
            return Ok(Some(candidate));
        }
        // The build side depends on the producer. A join below it whose build
        // key is the same column holds every key this join holds, because the
        // operators in between only remove rows or add NULL keys.
        let PhysicalRelOperator::Join(plan) = join.plan() else {
            return Ok(None);
        };
        let Some(column) = connector_column(plan.equi_conditions[condition].canonical_keys().1)
        else {
            return Ok(None);
        };
        let mut ancestors = ancestors.to_vec();
        ancestors.push((join, 1));
        self.superset_source(cte_name, join.build_side_child(), column, &mut ancestors)
    }

    fn superset_source<'a>(
        &self,
        cte_name: &str,
        node: &'a PExpr,
        column: Symbol,
        ancestors: &mut Vec<Ancestor<'a>>,
    ) -> Result<Option<Candidate>> {
        let next = match node.plan() {
            PhysicalRelOperator::Join(join) => {
                if can_build_filter(join) {
                    for (condition, equi) in join.equi_conditions.iter().enumerate() {
                        if !equi.is_null_equal
                            && connector_column(equi.canonical_keys().1) == Some(column)
                        {
                            if let Some(candidate) =
                                self.independent_source(cte_name, node, condition, ancestors)?
                            {
                                return Ok(Some(candidate));
                            }
                        }
                    }
                }
                let mut next = None;
                for (index, child) in node.children().enumerate() {
                    if child
                        .derive_relational_prop()?
                        .output_columns
                        .contains(&column)
                    {
                        next = Some(index);
                        break;
                    }
                }
                next
            }
            PhysicalRelOperator::EvalScalar(eval) => (!eval
                .items
                .iter()
                .any(|item| item.index == column && !is_column(&item.scalar, column)))
            .then_some(0),
            PhysicalRelOperator::Aggregate(aggregate) => aggregate
                .aggregate
                .group_items
                .iter()
                .any(|item| item.index == column && is_column(&item.scalar, column))
                .then_some(0),
            PhysicalRelOperator::Filter(_)
            | PhysicalRelOperator::Sort(_)
            | PhysicalRelOperator::Limit(_)
            | PhysicalRelOperator::TopN(_)
            | PhysicalRelOperator::Window(_)
            | PhysicalRelOperator::WindowGroup(_) => Some(0),
            _ => None,
        };
        let Some(index) = next else {
            return Ok(None);
        };
        ancestors.push((node, index));
        let result = self.superset_source(cte_name, node.child(index)?, column, ancestors);
        ancestors.pop();
        result
    }

    /// Returns a candidate if the join's filter does not depend on the
    /// producer of `cte_name`.
    fn independent_source(
        &self,
        cte_name: &str,
        join: &PExpr,
        condition: usize,
        ancestors: &[Ancestor<'_>],
    ) -> Result<Option<Candidate>> {
        // The join publishes its filter after its build side finishes. Scans
        // in the build side may in turn wait for filters of the joins that
        // have this join on their probe side.
        let mut waits = vec![join.build_side_child().clone()];
        for (ancestor, child_index) in ancestors {
            if *child_index == 0 && matches!(ancestor.plan(), PhysicalRelOperator::Join(_)) {
                waits.push(ancestor.build_side_child().clone());
            }
        }
        let mut visited = HashSet::new();
        if waits
            .iter()
            .any(|wait| self.depends_on(wait, cte_name, &mut visited))
        {
            return Ok(None);
        }
        let build_rows = join
            .build_side_child()
            .derive_cardinality(self.stat_ctx)?
            .cardinality;
        Ok(Some(Candidate {
            source: CteFilterSource {
                join: JoinId::of(join),
                condition,
            },
            build_rows,
            waits,
        }))
    }

    /// Whether running `node` requires output of the producer of `cte_name`.
    fn depends_on(&self, node: &PExpr, cte_name: &str, visited: &mut HashSet<String>) -> bool {
        match node.plan() {
            PhysicalRelOperator::MaterializedCTERef(cte_ref) => {
                if cte_ref.cte_name == cte_name {
                    return true;
                }
                if !visited.insert(cte_ref.cte_name.clone()) {
                    return false;
                }
                let Some(producer) = self.producers.get(&cte_ref.cte_name) else {
                    return true;
                };
                self.depends_on(producer, cte_name, visited)
                    || self
                        .producer_waits
                        .get(&cte_ref.cte_name)
                        .is_some_and(|waits| {
                            waits
                                .iter()
                                .any(|wait| self.depends_on(wait, cte_name, visited))
                        })
            }
            // Their inputs are produced elsewhere in the plan.
            PhysicalRelOperator::CacheScan(_) | PhysicalRelOperator::RecursiveCteScan(_) => true,
            _ => node
                .children()
                .any(|child| self.depends_on(child, cte_name, visited)),
        }
    }

    /// Producer scans that a filter on `column` can reach.
    fn producer_targets(
        &self,
        producer: &PExpr,
        column: Symbol,
    ) -> Result<Option<Vec<ScanProbeTarget>>> {
        let entry = self.metadata.read().column(column).clone();
        let scalar = ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                entry.name(),
                column,
                Box::new(entry.data_type()),
                Visibility::Visible,
            )
            .build(),
        });
        let Some(probe) = resolve_runtime_filter_probe_expr(self.metadata, &scalar)? else {
            return Ok(None);
        };
        let Some(scan_id) = self.metadata.read().base_column_scan_id(column) else {
            return Ok(None);
        };
        let targets = find_probe_targets(
            self.metadata,
            producer,
            probe.as_remote_expr(),
            scan_id,
            column,
            self.func_ctx,
        )?;
        Ok((!targets.is_empty()).then_some(targets))
    }
}

/// Casts producer targets to the key type of the source filters, like the
/// probe keys of a join are cast to the join's key type. Returns `None` if a
/// target cannot be cast without changing which values are equal.
pub fn cast_producer_targets(
    targets: &[ScanProbeTarget],
    key_type: &DataType,
    func_ctx: &FunctionContext,
) -> Result<Option<Vec<ScanProbeTarget>>> {
    let mut cast_targets = Vec::with_capacity(targets.len());
    for (target, scan_id) in targets {
        let Some(target) = normalize_probe_target(target, key_type, func_ctx)? else {
            return Ok(None);
        };
        cast_targets.push((target, *scan_id));
    }
    Ok(Some(cast_targets))
}

fn can_build_filter(join: &Join) -> bool {
    supported_join_type_for_runtime_filter(&join.join_type) && join.single_to_inner.is_none()
}

/// Follows producer columns across an operator that the filter can cross.
fn track_columns(plan: &PhysicalRelOperator, columns: &mut HashMap<Symbol, Symbol>) {
    match plan {
        PhysicalRelOperator::EvalScalar(eval) => {
            for item in &eval.items {
                match connector_column(&item.scalar)
                    .and_then(|c| columns.get(&c))
                    .copied()
                {
                    Some(producer_column) => {
                        columns.insert(item.index, producer_column);
                    }
                    None => {
                        columns.remove(&item.index);
                    }
                }
            }
        }
        PhysicalRelOperator::Aggregate(aggregate) => {
            columns.retain(|column, _| {
                aggregate
                    .aggregate
                    .group_items
                    .iter()
                    .any(|item| item.index == *column && is_column(&item.scalar, *column))
            });
        }
        // Rows of an inner join have equal non-NULL keys on both sides.
        PhysicalRelOperator::Join(join) if join.join_type == JoinType::Inner => {
            for equi in join.equi_conditions.iter().filter(|e| !e.is_null_equal) {
                let (left, right) = equi.canonical_keys();
                let (Some(left), Some(right)) = (connector_column(left), connector_column(right))
                else {
                    continue;
                };
                if let Some(producer_column) = columns.get(&left).copied() {
                    columns.entry(right).or_insert(producer_column);
                } else if let Some(producer_column) = columns.get(&right).copied() {
                    columns.entry(left).or_insert(producer_column);
                }
            }
        }
        _ => {}
    }
}

/// The column behind a key whose value equals the column's value.
fn connector_column(scalar: &ScalarExpr) -> Option<Symbol> {
    match scalar {
        ScalarExpr::BoundColumnRef(column) => Some(column.column.index),
        ScalarExpr::CastExpr(cast) if !cast.is_try => {
            let ScalarExpr::BoundColumnRef(column) = cast.argument.as_ref() else {
                return None;
            };
            (cast.target_type.remove_nullable() == column.column.data_type.remove_nullable())
                .then_some(column.column.index)
        }
        _ => None,
    }
}

/// The column behind a probe key whose value determines the column's value.
/// The producer applies the source keys after the same cast.
fn probe_key_column(scalar: &ScalarExpr) -> Option<Symbol> {
    if let ScalarExpr::CastExpr(cast) = scalar {
        if let ScalarExpr::BoundColumnRef(column) = cast.argument.as_ref() {
            if !cast.is_try
                && classify_conversion(&column.column.data_type, &cast.target_type)
                    .is_lossless_injective()
            {
                return Some(column.column.index);
            }
        }
    }
    connector_column(scalar)
}

fn is_column(scalar: &ScalarExpr, column: Symbol) -> bool {
    matches!(scalar, ScalarExpr::BoundColumnRef(c) if c.column.index == column)
}

fn contains_exchange(s_expr: &PExpr) -> bool {
    matches!(s_expr.plan(), PhysicalRelOperator::Exchange(_))
        || s_expr.children().any(contains_exchange)
}

fn collect_consumers<'a>(
    node: &'a PExpr,
    cte_name: &str,
    ancestors: &mut Vec<Ancestor<'a>>,
    consumers: &mut Vec<(&'a PExpr, Vec<Ancestor<'a>>)>,
) {
    if let PhysicalRelOperator::MaterializedCTERef(cte_ref) = node.plan() {
        if cte_ref.cte_name == cte_name {
            consumers.push((node, ancestors.clone()));
        }
        return;
    }
    for (index, child) in node.children().enumerate() {
        ancestors.push((node, index));
        collect_consumers(child, cte_name, ancestors, consumers);
        ancestors.pop();
    }
}
