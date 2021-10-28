// Copyright 2020 Datafuse Labs.
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

use crate::plan_broadcast::BroadcastPlan;
use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::CreateDatabasePlan;
use crate::CreateTablePlan;
use crate::DropDatabasePlan;
use crate::DropTablePlan;
use crate::Expression;
use crate::ExpressionPlan;
use crate::LimitPlan;
use crate::PlanNode;
use crate::ProjectionPlan;
use crate::ReadDataSourcePlan;
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
            PlanNode::Projection(plan) => Self::format_projection(f, plan),
            PlanNode::Expression(plan) => Self::format_expression(f, plan),
            PlanNode::AggregatorPartial(plan) => Self::format_aggregator_partial(f, plan),
            PlanNode::AggregatorFinal(plan) => Self::format_aggregator_final(f, plan),
            PlanNode::Filter(plan) => write!(f, "Filter: {:?}", plan.predicate),
            PlanNode::Having(plan) => write!(f, "Having: {:?}", plan.predicate),
            PlanNode::Sort(plan) => Self::format_sort(f, plan),
            PlanNode::Limit(plan) => Self::format_limit(f, plan),
            PlanNode::SubQueryExpression(plan) => Self::format_subquery_expr(f, plan),
            PlanNode::ReadSource(plan) => Self::format_read_source(f, plan),
            PlanNode::CreateDatabase(plan) => Self::format_create_database(f, plan),
            PlanNode::DropDatabase(plan) => Self::format_drop_database(f, plan),
            PlanNode::CreateTable(plan) => Self::format_create_table(f, plan),
            PlanNode::DropTable(plan) => Self::format_drop_table(f, plan),
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
                "{:?}:{:?}",
                plan.expr[i],
                plan.expr[i].to_data_type(&plan.input.schema()).unwrap()
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
                "{:?}:{:?}",
                plan.exprs[i],
                plan.exprs[i].to_data_type(&plan.input.schema()).unwrap()
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
                "{:?}:{:?}",
                expr,
                expr.to_data_type(&plan.schema()).unwrap()
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
            "ReadDataSource: scan partitions: [{}], scan schema: {}, statistics: [read_rows: {:?}, read_bytes: {:?}]",
            plan.parts.len(),
            PlanNode::display_scan_fields(&plan.scan_fields()),
            plan.statistics.read_rows,
            plan.statistics.read_bytes,
        )?;

        if plan.push_downs.is_some() {
            let extras = plan.push_downs.clone().unwrap();
            write!(f, ", push_downs: [")?;
            if extras.limit.is_some() {
                write!(f, "limit: {}", extras.limit.unwrap())?;
                write!(f, ", order_by: {:?}", extras.order_by)?;
            }
            write!(f, "]")?;
        }
        Ok(())
    }

    fn format_create_database(f: &mut Formatter, plan: &CreateDatabasePlan) -> fmt::Result {
        write!(f, "Create database {:},", plan.db)?;
        write!(f, " if_not_exists:{:},", plan.if_not_exists)?;
        write!(f, " option: {:?}", plan.options)
    }

    fn format_drop_database(f: &mut Formatter, plan: &DropDatabasePlan) -> fmt::Result {
        write!(f, "Drop database {:},", plan.db)?;
        write!(f, " if_exists:{:}", plan.if_exists)
    }

    fn format_create_table(f: &mut Formatter, plan: &CreateTablePlan) -> fmt::Result {
        write!(f, "Create table {:}.{:}", plan.db, plan.table)?;
        write!(f, " {:},", plan.schema())?;
        // need engine to impl Display
        write!(f, " engine: {},", plan.engine().to_string())?;
        write!(f, " if_not_exists:{:},", plan.if_not_exists)?;
        write!(f, " option: {:?}", plan.options())
    }

    fn format_drop_table(f: &mut Formatter, plan: &DropTablePlan) -> fmt::Result {
        write!(f, "Drop table {:}.{:},", plan.db, plan.table)?;
        write!(f, " if_exists:{:}", plan.if_exists)
    }
}
