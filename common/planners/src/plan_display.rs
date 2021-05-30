// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;
use std::fmt::Display;

use common_datavalues::DataSchema;

use crate::PlanNode;

impl PlanNode {
    pub fn display_indent(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a PlanNode);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let mut indent = 0;
                let mut write_indent = |f: &mut fmt::Formatter| -> fmt::Result {
                    if indent > 0 {
                        writeln!(f)?;
                    }
                    for _ in 0..indent {
                        write!(f, "  ")?;
                    }
                    indent += 1;
                    Ok(())
                };

                self.0.walk_preorder(|node| {
                    write_indent(f)?;
                    match node {
                        PlanNode::Stage(plan) => {
                            write!(
                                f,
                                "RedistributeStage[state: {:?}, id: {}]",
                                plan.state, plan.id
                            )?;
                            Ok(true)
                        }
                        PlanNode::Projection(plan) => {
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
                            Ok(true)
                        }
                        PlanNode::Expression(plan) => {
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
                            write!(f, " ({})", plan.desc)?;
                            Ok(true)
                        }
                        PlanNode::AggregatorPartial(plan) => {
                            write!(
                                f,
                                "AggregatorPartial: groupBy=[{:?}], aggr=[{:?}]",
                                plan.group_expr, plan.aggr_expr
                            )?;
                            Ok(true)
                        }
                        PlanNode::AggregatorFinal(plan) => {
                            write!(
                                f,
                                "AggregatorFinal: groupBy=[{:?}], aggr=[{:?}]",
                                plan.group_expr, plan.aggr_expr
                            )?;
                            Ok(true)
                        }
                        PlanNode::Filter(plan) => {
                            write!(f, "Filter: {:?}", plan.predicate)?;
                            Ok(true)
                        }
                        PlanNode::Having(plan) => {
                            write!(f, "Having: {:?}", plan.predicate)?;
                            Ok(true)
                        }
                        PlanNode::Sort(plan) => {
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
                            Ok(true)
                        }
                        PlanNode::Limit(plan) => {
                            write!(f, "Limit: {}", plan.n)?;
                            Ok(true)
                        }
                        PlanNode::ReadSource(plan) => {
                            write!(
                                f,
                                "ReadDataSource: scan partitions: [{}], scan schema: {}, statistics: [read_rows: {:?}, read_bytes: {:?}]",
                                plan.partitions.len(),
                                PlanNode::display_schema(plan.schema.as_ref()),
                                plan.statistics.read_rows,
                                plan.statistics.read_bytes,
                            )?;
                            Ok(false)
                        }
                        PlanNode::Explain(plan) => {
                            write!(f, "{:?}", plan.input)?;
                            Ok(false)
                        }
                        PlanNode::Select(plan) => {
                            write!(f, "{:?}", plan.input)?;
                            Ok(false)
                        }
                        PlanNode::CreateDatabase(plan) => {
                            write!(f, "Create database {:},", plan.db)?;
                            write!(f, " engine: {},", plan.engine.to_string())?;
                            write!(f, " if_not_exists:{:},", plan.if_not_exists)?;
                            write!(f, " option: {:?}", plan.options)?;
                            Ok(false)
                        }
                        PlanNode::DropDatabase(plan) => {
                            write!(f, "Drop database {:},", plan.db)?;
                            write!(f, " if_exists:{:}", plan.if_exists)?;
                            Ok(false)
                        }
                        PlanNode::CreateTable(plan) => {
                            write!(f, "Create table {:}.{:}", plan.db, plan.table)?;
                            write!(f, " {:},", plan.schema)?;
                            // need engine to impl Display
                            write!(f, " engine: {},", plan.engine.to_string())?;
                            write!(f, " if_not_exists:{:},", plan.if_not_exists)?;
                            write!(f, " option: {:?}", plan.options)?;
                            Ok(false)
                        }
                        PlanNode::DropTable(plan) => {
                            write!(f, "Drop table {:}.{:},", plan.db, plan.table)?;
                            write!(f, " if_exists:{:}", plan.if_exists)?;
                            Ok(false)
                        }
                        PlanNode::Join(plan) => {
                            write!(f, "Join\n")?;
                            write!(f, "{:?}\n", &plan.left_input)?;
                            write!(f, "{:?}\n", &plan.right_input)?;
                            Ok(false)
                        }
                        _ => Ok(false),
                    }
                })
            }
        }
        Wrapper(self)
    }

    pub fn display_graphviz(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a PlanNode);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                writeln!(
                    f,
                    "// Begin DataFuse GraphViz Plan (see https://graphviz.org)"
                )?;
                writeln!(f, "digraph {{")?;
                // TODO()
                writeln!(f, "}}")?;
                writeln!(f, "// End DataFuse GraphViz Plan")?;
                Ok(())
            }
        }
        Wrapper(self)
    }

    pub fn display_schema(schema: &DataSchema) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a DataSchema);

        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "[")?;
                for (idx, field) in self.0.fields().iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    let nullable_str = if field.is_nullable() { ";N" } else { "" };
                    write!(
                        f,
                        "{}:{:?}{}",
                        field.name(),
                        field.data_type(),
                        nullable_str
                    )?;
                }
                write!(f, "]")
            }
        }
        Wrapper(schema)
    }
}

impl fmt::Debug for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}
