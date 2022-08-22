// Copyright 2021 Datafuse Labs.
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

use std::fmt;
use std::fmt::Formatter;

use common_datavalues::DataType;

use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::BroadcastPlan;
use crate::Expression;
use crate::ExpressionPlan;
use crate::LimitPlan;
use crate::PlanNode;
use crate::ProjectionPlan;
use crate::ReadDataSourcePlan;
use crate::RemotePlan;
use crate::SortPlan;
use crate::StagePlan;
use crate::SubQueriesSetPlan;

pub struct PlanNodeIndentFormatDisplay<'a> {
    indent: usize,
    node: &'a PlanNode,
    printed_indent: bool,
}

impl<'a> PlanNodeIndentFormatDisplay<'a> {
    pub fn create(indent: usize, node: &'a PlanNode, printed: bool) -> Self {
        PlanNodeIndentFormatDisplay {
            indent,
            node,
            printed_indent: printed,
        }
    }
}

impl<'a> fmt::Display for PlanNodeIndentFormatDisplay<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if !self.printed_indent {
            write!(f, "{}", str::repeat("  ", self.indent))?;
        }

        match self.node {
            PlanNode::Stage(plan) => Self::format_stage(f, plan),
            PlanNode::Broadcast(plan) => Self::format_broadcast(f, plan),
            PlanNode::Remote(plan) => Self::format_remote(f, plan),
            PlanNode::Projection(plan) => Self::format_projection(f, plan),
            PlanNode::Expression(plan) => Self::format_expression(f, plan),
            PlanNode::AggregatorPartial(plan) => Self::format_aggregator_partial(f, plan),
            PlanNode::AggregatorFinal(plan) => Self::format_aggregator_final(f, plan),
            PlanNode::Filter(plan) => write!(f, "Filter: {:?}", plan.predicate),
            PlanNode::Having(plan) => write!(f, "Having: {:?}", plan.predicate),
            PlanNode::WindowFunc(plan) => {
                write!(f, "WindowFunc: {:?}", plan.window_func)
            }
            PlanNode::Sort(plan) => Self::format_sort(f, plan),
            PlanNode::Limit(plan) => Self::format_limit(f, plan),
            PlanNode::SubQueryExpression(plan) => Self::format_subquery_expr(f, plan),
            PlanNode::ReadSource(plan) => Self::format_read_source(f, plan),
            _ => {
                let mut printed = true;

                for input in self.node.inputs() {
                    if matches!(input.as_ref(), PlanNode::Empty(_)) {
                        continue;
                    }

                    if !printed {
                        writeln!(f)?;
                    }

                    PlanNodeIndentFormatDisplay::create(self.indent, input.as_ref(), printed)
                        .fmt(f)?;
                    printed = true;
                }

                return fmt::Result::Ok(());
            }
        }?;

        let new_indent = self.indent + 1;
        for input in self.node.inputs() {
            if matches!(input.as_ref(), PlanNode::Empty(_)) {
                continue;
            }

            writeln!(f)?;
            PlanNodeIndentFormatDisplay::create(new_indent, &input, false).fmt(f)?;
        }

        fmt::Result::Ok(())
    }
}

impl<'a> PlanNodeIndentFormatDisplay<'a> {
    fn format_stage(f: &mut Formatter, plan: &StagePlan) -> fmt::Result {
        write!(f, "RedistributeStage[expr: {:?}]", plan.scatters_expr)
    }

    fn format_remote(f: &mut Formatter, plan: &RemotePlan) -> fmt::Result {
        match plan {
            RemotePlan::V1(_) => write!(f, "Remote"),
            RemotePlan::V2(v2_plan) => write!(
                f,
                "Remote[receive fragment: {}]",
                v2_plan.receive_fragment_id
            ),
        }
    }

    fn format_broadcast(f: &mut Formatter, _plan: &BroadcastPlan) -> fmt::Result {
        write!(f, "Broadcast in cluster")
    }

    fn format_projection(f: &mut Formatter, plan: &ProjectionPlan) -> fmt::Result {
        write!(f, "Projection: ")?;
        for i in 0..plan.expr.len() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(
                f,
                "{:?}:{}",
                plan.expr[i],
                plan.expr[i]
                    .to_data_type(&plan.input.schema())
                    .unwrap()
                    .name()
            )?;
        }

        fmt::Result::Ok(())
    }

    fn format_expression(f: &mut Formatter, plan: &ExpressionPlan) -> fmt::Result {
        write!(f, "Expression: ")?;
        for i in 0..plan.exprs.len() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(
                f,
                "{:?}:{}",
                plan.exprs[i],
                plan.exprs[i]
                    .to_data_type(&plan.input.schema())
                    .unwrap()
                    .name()
            )?;
        }

        write!(f, " ({})", plan.desc)
    }

    fn format_aggregator_partial(f: &mut Formatter, plan: &AggregatorPartialPlan) -> fmt::Result {
        write!(
            f,
            "AggregatorPartial: groupBy=[{:?}], aggr=[{:?}]",
            plan.group_expr, plan.aggr_expr
        )
    }

    fn format_aggregator_final(f: &mut Formatter, plan: &AggregatorFinalPlan) -> fmt::Result {
        write!(
            f,
            "AggregatorFinal: groupBy=[{:?}], aggr=[{:?}]",
            plan.group_expr, plan.aggr_expr
        )
    }

    fn format_sort(f: &mut Formatter, plan: &SortPlan) -> fmt::Result {
        write!(f, "Sort: ")?;
        for i in 0..plan.order_by.len() {
            if i > 0 {
                write!(f, ", ")?;
            }
            let expr = plan.order_by[i].clone();
            write!(
                f,
                "{:?}:{}",
                expr,
                expr.to_data_type(&plan.schema()).unwrap().name()
            )?;
        }

        fmt::Result::Ok(())
    }

    fn format_limit(f: &mut Formatter, plan: &LimitPlan) -> fmt::Result {
        match (plan.n, plan.offset) {
            (Some(n), 0) => write!(f, "Limit: {}", n),
            (Some(n), offset) => write!(f, "Limit: {}, {}", n, offset),
            (None, offset) => write!(f, "Limit: all, {}", offset),
        }
    }

    fn format_subquery_expr(f: &mut Formatter, plan: &SubQueriesSetPlan) -> fmt::Result {
        let mut names = Vec::with_capacity(plan.expressions.len());
        for expression in &plan.expressions {
            match expression {
                Expression::Subquery { name, .. } => names.push(name.clone()),
                Expression::ScalarSubquery { name, .. } => names.push(name.clone()),
                _ => {}
            };
        }
        write!(f, "Create sub queries sets: [{}]", names.join(", "))
    }

    fn format_read_source(f: &mut Formatter, plan: &ReadDataSourcePlan) -> fmt::Result {
        write!(
            f,
            "ReadDataSource: scan schema: {}, statistics: [read_rows: {:?}, read_bytes: {:?}, partitions_scanned: {:?}, partitions_total: {:?}]",
            PlanNode::display_scan_fields(&plan.scan_fields()),
            plan.statistics.read_rows,
            plan.statistics.read_bytes,
            plan.statistics.partitions_scanned,
            plan.statistics.partitions_total,
        )?;

        if let Some(p) = &plan.push_downs {
            if p.limit.is_some() || p.projection.is_some() {
                write!(f, ", push_downs: [")?;
                let mut comma = false;
                if p.projection.is_some() {
                    write!(f, "projections: {:?}", p.projection.clone().unwrap())?;
                    comma = true;
                }

                if !p.filters.is_empty() {
                    if comma {
                        write!(f, ", ")?;
                    }
                    write!(f, "filters: {:?}", p.filters)?;
                    comma = true;
                }

                if p.limit.is_some() {
                    if comma {
                        write!(f, ", ")?;
                    }

                    write!(f, "limit: {:?}", p.limit.unwrap())?;
                }

                if !p.order_by.is_empty() {
                    if comma {
                        write!(f, ", ")?;
                    }
                    write!(f, "order_by: {:?}", p.order_by)?;
                }

                write!(f, "]")?;
            }
        }
        Ok(())
    }
}
