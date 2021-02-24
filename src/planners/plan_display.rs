// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::fmt;
use std::fmt::Display;

use crate::planners::{walk_preorder, PlanNode};

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

                walk_preorder(self.0, |node| {
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
                                    plan.schema().fields()[i].data_type()
                                )?;
                            }
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
                        PlanNode::Limit(plan) => {
                            write!(f, "Limit: {}", plan.n)?;
                            Ok(true)
                        }
                        PlanNode::ReadSource(plan) => {
                            write!(
                                f,
                                "ReadDataSource: scan parts [{}]{}",
                                plan.partitions.len(),
                                plan.description
                            )?;
                            Ok(false)
                        }
                        PlanNode::Explain(plan) => {
                            write!(f, "{:?}", plan.input())?;
                            Ok(false)
                        }
                        PlanNode::Select(plan) => {
                            write!(f, "{:?}", plan.input())?;
                            Ok(false)
                        }
                        PlanNode::Scan(_) | PlanNode::SetVariable(_) | PlanNode::Empty(_) => {
                            Ok(false)
                        }
                    }
                })
                .map_err(|_| fmt::Error)?;
                Ok(())
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
}

impl fmt::Debug for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}
