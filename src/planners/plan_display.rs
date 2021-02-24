// Copyright 2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::fmt;
use std::fmt::Display;

use crate::planners::{walk_preorder, GraphvizVisitor, IndentVisitor, PlanNode, PlanVisitor};

impl PlanNode {
    pub fn accept<V>(&self, visitor: &mut V) -> std::result::Result<bool, V::Error>
    where
        V: PlanVisitor,
    {
        if !visitor.pre_visit(self)? {
            return Ok(false);
        }

        let recurse = match self {
            PlanNode::Stage(plan) => plan.input.accept(visitor)?,
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

    pub fn display(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a PlanNode);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                walk_preorder(self.0, |node| match node {
                    PlanNode::Stage(plan) => {
                        write!(
                            f,
                            "RedistributeStage[state: {:?}, id: {}]",
                            plan.state, plan.id
                        )?;
                        Ok(false)
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
                        Ok(false)
                    }
                    PlanNode::AggregatorPartial(plan) => {
                        write!(
                            f,
                            "AggregatorPartial: groupBy=[{:?}], aggr=[{:?}]",
                            plan.group_expr, plan.aggr_expr
                        )?;
                        Ok(false)
                    }
                    PlanNode::AggregatorFinal(plan) => {
                        write!(
                            f,
                            "AggregatorFinal: groupBy=[{:?}], aggr=[{:?}]",
                            plan.group_expr, plan.aggr_expr
                        )?;
                        Ok(false)
                    }
                    PlanNode::Filter(plan) => {
                        write!(f, "Filter: {:?}", plan.predicate)?;
                        Ok(false)
                    }
                    PlanNode::Limit(plan) => {
                        write!(f, "Limit: {}", plan.n)?;
                        Ok(false)
                    }
                    PlanNode::Scan(_) => {
                        write!(f, "")?;
                        Ok(false)
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
                    PlanNode::SetVariable(_) | PlanNode::Empty(_) => Ok(false),
                })
                .unwrap();
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
