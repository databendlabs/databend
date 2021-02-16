// Copyright 2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::fmt;
use std::fmt::Display;

use crate::planners::{GraphvizVisitor, IndentVisitor, PlanNode, PlanVisitor};

impl PlanNode {
    pub fn accept<V>(&self, visitor: &mut V) -> std::result::Result<bool, V::Error>
    where
        V: PlanVisitor,
    {
        if !visitor.pre_visit(self)? {
            return Ok(false);
        }

        let recurse = match self {
            PlanNode::Fragment(plan) => plan.input.accept(visitor)?,
            PlanNode::Projection(plan) => plan.input.accept(visitor)?,
            PlanNode::AggregatorPartial(plan) => plan.input.accept(visitor)?,
            PlanNode::AggregatorFinal(plan) => plan.input.accept(visitor)?,
            PlanNode::Filter(plan) => plan.input.accept(visitor)?,
            PlanNode::Limit(plan) => plan.input.accept(visitor)?,
            PlanNode::Scan(..)
            | PlanNode::Empty(..)
            | PlanNode::Select(..)
            | PlanNode::Explain(..)
            | PlanNode::SetVariable(..)
            | PlanNode::ReadSource(..) => true,
        };
        if !recurse {
            return Ok(false);
        }

        if !visitor.post_visit(self)? {
            return Ok(false);
        }

        Ok(true)
    }

    pub fn display_indent(&self) -> impl fmt::Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a PlanNode);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let with_schema = false;
                let mut visitor = IndentVisitor::new(f, with_schema);
                self.0.accept(&mut visitor).unwrap();
                Ok(())
            }
        }
        Wrapper(self)
    }

    pub fn display_graphviz(&self) -> impl fmt::Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a PlanNode);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                writeln!(
                    f,
                    "// Begin DataFuse GraphViz Plan (see https://graphviz.org)"
                )?;
                writeln!(f, "digraph {{")?;

                let mut visitor = GraphvizVisitor::new(f);

                visitor.set_with_schema(true);
                visitor.pre_visit_plan("LogicalPlan")?;
                self.0.accept(&mut visitor).unwrap();
                visitor.post_visit_plan()?;

                writeln!(f, "}}")?;
                writeln!(f, "// End DataFuse GraphViz Plan")?;
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure with the a human readable
    /// description of this LogicalPlan node per node, not including
    /// children.
    pub fn display(&self) -> impl fmt::Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a PlanNode);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self.0 {
                    PlanNode::Empty(_) => {
                        write!(f, "")
                    }
                    PlanNode::Fragment(v) => {
                        write!(
                            f,
                            "RedistributeStage[state: {:?}, uuid: {}, id: {}]",
                            v.state, v.uuid, v.id
                        )
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
                        Ok(())
                    }
                    PlanNode::AggregatorPartial(plan) => {
                        write!(
                            f,
                            "AggregatorPartial: groupBy=[{:?}], aggr=[{:?}]",
                            plan.group_expr, plan.aggr_expr
                        )
                    }
                    PlanNode::AggregatorFinal(plan) => {
                        write!(
                            f,
                            "AggregatorFinal: groupBy=[{:?}], aggr=[{:?}]",
                            plan.group_expr, plan.aggr_expr
                        )
                    }
                    PlanNode::Filter(plan) => {
                        write!(f, "Filter: {:?}", plan.predicate)
                    }
                    PlanNode::Limit(plan) => {
                        write!(f, "Limit: {}", plan.n)
                    }
                    PlanNode::Scan(_) | PlanNode::SetVariable(_) => {
                        write!(f, "")
                    }
                    PlanNode::ReadSource(plan) => {
                        write!(
                            f,
                            "ReadDataSource: scan parts [{}]{}",
                            plan.partitions.len(),
                            plan.description
                        )
                    }
                    PlanNode::Explain(plan) => {
                        write!(f, "{:?}", plan.input)
                    }
                    PlanNode::Select(plan) => {
                        write!(f, "{:?}", plan.input)
                    }
                }
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
