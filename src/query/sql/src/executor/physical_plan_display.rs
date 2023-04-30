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

use std::fmt::Display;
use std::fmt::Formatter;

use common_functions::BUILTIN_FUNCTIONS;
use itertools::Itertools;

use super::AggregateExpand;
use super::DistributedInsertSelect;
use super::ProjectSet;
use super::RowFetch;
use crate::executor::AggregateFinal;
use crate::executor::AggregatePartial;
use crate::executor::EvalScalar;
use crate::executor::Exchange;
use crate::executor::ExchangeSink;
use crate::executor::ExchangeSource;
use crate::executor::Filter;
use crate::executor::HashJoin;
use crate::executor::Limit;
use crate::executor::PhysicalPlan;
use crate::executor::Project;
use crate::executor::RuntimeFilterSource;
use crate::executor::Sort;
use crate::executor::TableScan;
use crate::executor::UnionAll;
use crate::executor::Window;
use crate::plans::JoinType;

impl PhysicalPlan {
    pub fn format_indent(&self, indent: usize) -> impl std::fmt::Display + '_ {
        PhysicalPlanIndentFormatDisplay { indent, node: self }
    }
}

pub struct PhysicalPlanIndentFormatDisplay<'a> {
    indent: usize,
    node: &'a PhysicalPlan,
}

impl<'a> Display for PhysicalPlanIndentFormatDisplay<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", "  ".repeat(self.indent))?;

        match self.node {
            PhysicalPlan::TableScan(scan) => write!(f, "{}", scan)?,
            PhysicalPlan::Filter(filter) => write!(f, "{}", filter)?,
            PhysicalPlan::Project(project) => write!(f, "{}", project)?,
            PhysicalPlan::EvalScalar(eval_scalar) => write!(f, "{}", eval_scalar)?,
            PhysicalPlan::AggregateExpand(aggregate) => write!(f, "{}", aggregate)?,
            PhysicalPlan::AggregatePartial(aggregate) => write!(f, "{}", aggregate)?,
            PhysicalPlan::AggregateFinal(aggregate) => write!(f, "{}", aggregate)?,
            PhysicalPlan::Window(window) => write!(f, "{}", window)?,
            PhysicalPlan::Sort(sort) => write!(f, "{}", sort)?,
            PhysicalPlan::Limit(limit) => write!(f, "{}", limit)?,
            PhysicalPlan::RowFetch(row_fetch) => write!(f, "{}", row_fetch)?,
            PhysicalPlan::HashJoin(join) => write!(f, "{}", join)?,
            PhysicalPlan::Exchange(exchange) => write!(f, "{}", exchange)?,
            PhysicalPlan::ExchangeSource(source) => write!(f, "{}", source)?,
            PhysicalPlan::ExchangeSink(sink) => write!(f, "{}", sink)?,
            PhysicalPlan::UnionAll(union_all) => write!(f, "{}", union_all)?,
            PhysicalPlan::DistributedInsertSelect(insert_select) => write!(f, "{}", insert_select)?,
            PhysicalPlan::ProjectSet(unnest) => write!(f, "{}", unnest)?,
            PhysicalPlan::RuntimeFilterSource(plan) => write!(f, "{}", plan)?,
        }

        for node in self.node.children() {
            writeln!(f)?;
            write!(f, "{}", node.format_indent(self.indent + 1))?;
        }

        Ok(())
    }
}

impl Display for TableScan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableScan: [{}]", self.source.source_info.desc())
    }
}

impl Display for Filter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let predicates = self
            .predicates
            .iter()
            .map(|pred| pred.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .join(", ");

        write!(f, "Filter: [{predicates}]")
    }
}

impl Display for Project {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Ok(input_schema) = self.input.output_schema() {
            let project_columns_name = self
                .projections
                .iter()
                .sorted()
                .map(|idx| input_schema.field(*idx).name())
                .cloned()
                .collect::<Vec<String>>();

            return write!(f, "Project: [{}]", project_columns_name.join(", "));
        }

        write!(f, "Project: [{:?}]", self.projections)
    }
}

impl Display for Sort {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let scalars = self
            .order_by
            .iter()
            .map(|item| {
                format!(
                    "{} {}",
                    item.order_by,
                    if item.asc { "ASC" } else { "DESC" }
                )
            })
            .collect::<Vec<String>>();
        let limit = self.limit.as_ref().cloned().unwrap_or(0);
        write!(f, "Sort: [{}], Limit: [{}]", scalars.join(", "), limit)
    }
}

impl Display for EvalScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let scalars = self
            .exprs
            .iter()
            .map(|(expr, _)| expr.as_expr(&BUILTIN_FUNCTIONS).to_string())
            .collect::<Vec<String>>();

        write!(f, "EvalScalar: [{}]", scalars.join(", "))
    }
}

impl Display for AggregateExpand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let sets = self
            .grouping_sets
            .iter()
            .map(|set| {
                set.iter()
                    .map(|index| index.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            })
            .map(|s| format!("[{}]", s))
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "Aggregate(Expand): grouping sets: [{}]", sets)
    }
}

impl Display for AggregateFinal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let group_items = self
            .group_by
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let agg_funcs = self
            .agg_funcs
            .iter()
            .map(|item| {
                format!(
                    "{}({})",
                    item.sig.name,
                    item.args
                        .iter()
                        .map(|index| index.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            })
            .join(", ");

        write!(
            f,
            "Aggregate(Final): group items: [{}], aggregate functions: [{}]",
            group_items, agg_funcs
        )
    }
}

impl Display for AggregatePartial {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let group_items = self
            .group_by
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let agg_funcs = self
            .agg_funcs
            .iter()
            .map(|item| {
                format!(
                    "{}({})",
                    item.sig.name,
                    item.args
                        .iter()
                        .map(|index| index.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )
            })
            .join(", ");

        write!(
            f,
            "Aggregate(Partial): group items: [{}], aggregate functions: [{}]",
            group_items, agg_funcs
        )
    }
}

impl Display for Window {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let window_id = self.plan_id;
        write!(f, "Window: [{}]", window_id)
    }
}

impl Display for Limit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let limit = self.limit.as_ref().cloned().unwrap_or(0);
        write!(f, "Limit: [{}], Offset: [{}]", limit, self.offset)
    }
}

impl Display for RowFetch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RowFetch: [{:?}]", self.cols_to_fetch)
    }
}

impl Display for HashJoin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.join_type {
            JoinType::Cross => {
                write!(f, "CrossJoin")
            }
            _ => {
                let build_keys = self
                    .build_keys
                    .iter()
                    .map(|scalar| scalar.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                    .collect::<Vec<String>>()
                    .join(", ");

                let probe_keys = self
                    .probe_keys
                    .iter()
                    .map(|scalar| scalar.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                    .collect::<Vec<String>>()
                    .join(", ");

                let join_filters = self
                    .non_equi_conditions
                    .iter()
                    .map(|scalar| scalar.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                    .collect::<Vec<String>>()
                    .join(", ");

                write!(
                    f,
                    "HashJoin: {}, build keys: [{}], probe keys: [{}], join filters: [{}]",
                    &self.join_type, build_keys, probe_keys, join_filters,
                )
            }
        }
    }
}

impl Display for Exchange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let keys = self
            .keys
            .iter()
            .map(|key| key.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .join(", ");

        write!(f, "Exchange: [kind: {:?}, keys: {}]", self.kind, keys)
    }
}

impl Display for ExchangeSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Exchange Source: fragment id: [{:?}]",
            self.source_fragment_id
        )
    }
}

impl Display for ExchangeSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Exchange Sink: fragment id: [{:?}]",
            self.destination_fragment_id
        )
    }
}

impl Display for UnionAll {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UnionAll")
    }
}

impl Display for DistributedInsertSelect {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DistributedInsertSelect")
    }
}

impl Display for RuntimeFilterSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RuntimeFilterSource")
    }
}

impl Display for ProjectSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let scalars = self
            .srf_exprs
            .iter()
            .map(|(expr, _)| expr.as_expr(&BUILTIN_FUNCTIONS).to_string())
            .collect::<Vec<String>>();

        write!(
            f,
            "ProjectSet: set-returning functions : {}",
            scalars.join(", ")
        )
    }
}
