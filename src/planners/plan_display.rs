// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::fmt;

use crate::planners::PlanNode;

/// Formatter settings for PlanStep debug.
struct FormatterSettings {
    pub indent: usize,
    pub indent_char: &'static str,
    pub prefix: &'static str,
}

impl PlanNode {
    fn format(&self, f: &mut fmt::Formatter, setting: &mut FormatterSettings) -> fmt::Result {
        if setting.indent > 0 {
            writeln!(f)?;
            for _ in 0..setting.indent {
                write!(f, "{}", setting.indent_char)?;
            }
        }
        match self {
            PlanNode::Projection(v) => {
                write!(f, "{} Projection: ", setting.prefix)?;
                for i in 0..v.expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", v.expr[i])?;
                }
                write!(f, "")
            }
            PlanNode::Aggregate(v) => {
                write!(f, "{} Aggregate: ", setting.prefix)?;
                for i in 0..v.aggr_expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", v.aggr_expr[i])?;
                }
                for i in 0..v.group_expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", v.group_expr[i])?;
                }
                write!(f, "")
            }
            PlanNode::Filter(v) => write!(f, "{} Filter: {:?}", setting.prefix, v.predicate),
            PlanNode::Limit(v) => write!(f, "{} Limit: {}", setting.prefix, v.n),
            PlanNode::ReadSource(v) => write!(
                f,
                "{} ReadDataSource: scan parts [{}]{}",
                setting.prefix,
                v.partitions.len(),
                v.description
            ),

            // Empty.
            PlanNode::Empty(_) => write!(f, ""),
            PlanNode::Scan(_) => write!(f, ""),
            PlanNode::Select(_) => {
                write!(f, "")
            }
            PlanNode::Explain(_) => write!(f, ""),
        }
    }
}

impl fmt::Debug for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let setting = &mut FormatterSettings {
            indent: 0,
            indent_char: "  ",
            prefix: "└─",
        };

        let mut plans = self.children_to_plans().map_err(|_| std::fmt::Error)?;
        plans.reverse();
        for node in plans.iter() {
            node.format(f, setting)?;
            setting.indent += 1;
        }
        write!(f, "")
    }
}
