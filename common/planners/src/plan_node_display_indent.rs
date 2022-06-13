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
use crate::AlterTableClusterKeyPlan;
use crate::BroadcastPlan;
use crate::CallPlan;
use crate::CopyPlan;
use crate::CreateDatabasePlan;
use crate::CreateRolePlan;
use crate::CreateTablePlan;
use crate::DropDatabasePlan;
use crate::DropRolePlan;
use crate::DropTableClusterKeyPlan;
use crate::DropTablePlan;
use crate::Expression;
use crate::ExpressionPlan;
use crate::LimitPlan;
use crate::PlanNode;
use crate::ProjectionPlan;
use crate::ReadDataSourcePlan;
use crate::RenameDatabasePlan;
use crate::RenameTablePlan;
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
            PlanNode::WindowFunc(plan) => {
                write!(f, "WindowFunc: {:?}", plan.window_func)
            }
            PlanNode::Sort(plan) => Self::format_sort(f, plan),
            PlanNode::Limit(plan) => Self::format_limit(f, plan),
            PlanNode::SubQueryExpression(plan) => Self::format_subquery_expr(f, plan),
            PlanNode::ReadSource(plan) => Self::format_read_source(f, plan),
            PlanNode::CreateDatabase(plan) => Self::format_create_database(f, plan),
            PlanNode::DropDatabase(plan) => Self::format_drop_database(f, plan),
            PlanNode::RenameDatabase(plan) => Self::format_rename_database(f, plan),
            PlanNode::CreateTable(plan) => Self::format_create_table(f, plan),
            PlanNode::DropTable(plan) => Self::format_drop_table(f, plan),
            PlanNode::RenameTable(plan) => Self::format_rename_table(f, plan),
            PlanNode::CreateRole(plan) => Self::format_create_role(f, plan),
            PlanNode::DropRole(plan) => Self::format_drop_role(f, plan),
            PlanNode::Copy(plan) => Self::format_copy(f, plan),
            PlanNode::Call(plan) => Self::format_call(f, plan),
            PlanNode::AlterTableClusterKey(plan) => Self::format_alter_table_cluster_key(f, plan),
            PlanNode::DropTableClusterKey(plan) => Self::format_drop_table_cluster_key(f, plan),
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

    fn format_create_database(f: &mut Formatter, plan: &CreateDatabasePlan) -> fmt::Result {
        write!(f, "Create database {:},", plan.database)?;
        write!(f, " if_not_exists:{:},", plan.if_not_exists)?;
        if !plan.meta.engine.is_empty() {
            write!(
                f,
                " engine: {}={:?},",
                plan.meta.engine, plan.meta.engine_options
            )?;
        }
        write!(f, " option: {:?}", plan.meta.options)
    }

    fn format_drop_database(f: &mut Formatter, plan: &DropDatabasePlan) -> fmt::Result {
        write!(f, "Drop database {:},", plan.database)?;
        write!(f, " if_exists:{:}", plan.if_exists)
    }

    fn format_create_role(f: &mut Formatter, plan: &CreateRolePlan) -> fmt::Result {
        write!(f, "Create role {:}", plan.role_name)?;
        write!(f, " if_not_exist:{:}", plan.if_not_exists)
    }

    fn format_drop_role(f: &mut Formatter, plan: &DropRolePlan) -> fmt::Result {
        write!(f, "Drop role {:}", plan.role_name)?;
        write!(f, " if_exists:{:}", plan.if_exists)
    }

    fn format_create_table(f: &mut Formatter, plan: &CreateTablePlan) -> fmt::Result {
        write!(f, "Create table {:}.{:}", plan.database, plan.table)?;
        write!(f, " {:},", plan.schema())?;
        // need engine to impl Display
        write!(f, " engine: {},", plan.engine())?;
        write!(f, " if_not_exists:{:},", plan.if_not_exists)?;
        write!(f, " option: {:?},", plan.options())?;
        write!(f, " as_select: {:?}", plan.as_select())
    }

    fn format_drop_table(f: &mut Formatter, plan: &DropTablePlan) -> fmt::Result {
        write!(f, "Drop table {:}.{:},", plan.database, plan.table)?;
        write!(f, " if_exists:{:}", plan.if_exists)
    }

    fn format_rename_table(f: &mut Formatter, plan: &RenameTablePlan) -> fmt::Result {
        write!(f, "Rename table,")?;
        write!(f, " [")?;
        for (i, entity) in plan.entities.iter().enumerate() {
            write!(
                f,
                "{:}.{:} to {:}.{:}",
                entity.database, entity.table, entity.new_database, entity.new_table
            )?;

            if i + 1 != plan.entities.len() {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }

    fn format_rename_database(f: &mut Formatter, plan: &RenameDatabasePlan) -> fmt::Result {
        write!(f, "Rename database,")?;
        write!(f, " [")?;
        for (i, entity) in plan.entities.iter().enumerate() {
            write!(f, "{:} to {:}", entity.database, entity.new_database,)?;

            if i + 1 != plan.entities.len() {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }

    fn format_copy(f: &mut Formatter, plan: &CopyPlan) -> fmt::Result {
        write!(f, "{:?}", plan)
    }

    fn format_call(f: &mut Formatter, plan: &CallPlan) -> fmt::Result {
        write!(f, "Call {:}", plan.name)?;
        write!(f, " args: {:?}", plan.args)
    }

    fn format_alter_table_cluster_key(
        f: &mut Formatter,
        plan: &AlterTableClusterKeyPlan,
    ) -> fmt::Result {
        write!(f, "Alter table {:}.{:}", plan.database, plan.table)?;
        write!(f, " cluster by {:?}", plan.cluster_keys)
    }

    fn format_drop_table_cluster_key(
        f: &mut Formatter,
        plan: &DropTableClusterKeyPlan,
    ) -> fmt::Result {
        write!(
            f,
            "Alter table {:}.{:} drop cluster key",
            plan.database, plan.table
        )
    }
}
