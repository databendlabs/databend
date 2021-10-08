use crate::pipelines::processors::processor_dag::ProcessorsDAG;
use petgraph::dot::{Dot, Config};
use std::fmt::{Display, Formatter};
use petgraph::Direction;
use petgraph::prelude::StableGraph;
use std::sync::Arc;
use crate::pipelines::processors::Processor;
use petgraph::visit::{IntoNeighbors, IntoNeighborsDirected};
use petgraph::stable_graph::NodeIndex;

impl ProcessorsDAG {
    pub fn display_indent(&self) -> impl Display + '_ {
        IndentDisplayWrap(self)
    }

    pub fn display_graphviz(&self) -> impl Display + '_ {
        struct GraphvizDisplayWrap<'a>(&'a ProcessorsDAG);

        impl<'a> Display for GraphvizDisplayWrap<'a> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self.0)
            }
        }

        GraphvizDisplayWrap(self)
    }
}

struct IndentDisplayWrap<'a>(&'a ProcessorsDAG);

impl<'a> IndentDisplayWrap<'a> {
    fn write(&self, c: usize, index: &Vec<NodeIndex>, f: &mut Formatter<'_>) -> std::fmt::Result {
        let graph = &self.0.graph;
        match graph.node_weight(index[0]) {
            None => unreachable!("Not found graph node weight."),
            Some(processor) => {
                write!(
                    f,
                    "{}{} × {} {}\n",
                    "  ".repeat(c),
                    processor.name(),
                    index.len(),
                    if index.len() == 1 { "processor" } else { "processors" }
                )?;
            }
        };

        let mut neighbors: Vec<NodeIndex> = Vec::new();
        for node_index in index {
            let iterator = graph.neighbors_directed(*node_index, Direction::Incoming);

            for neighbors_index in iterator {
                neighbors.push(neighbors_index);
            }
        }

        match (index.len(), neighbors.len()) {
            (_, 0) => Ok(()),
            (left, right) if left == right => self.write(c + 1, &neighbors, f),
            (1, _) => self.write_merge(c + 1, f, &index, &neighbors),
            _ => self.write_mixed(c + 1, f, index, &neighbors),
        }
    }

    fn write_mixed(
        &self,
        c: usize,
        f: &mut Formatter,
        index: &Vec<NodeIndex>,
        neighbors: &Vec<NodeIndex>,
    ) -> std::fmt::Result {
        if !neighbors.is_empty() {
            let graph = &self.0.graph;
            let prev_processor = graph.node_weight(neighbors[0]);
            let post_processor = graph.node_weight(index[0]);

            match (prev_processor, post_processor) {
                (Some(prev_processor), Some(post_processor)) => {
                    write!(
                        f,
                        "{}Mixed ({} × {} {}) to ({} × {} {})\n",
                        "  ".repeat(c),
                        prev_processor.name(), neighbors.len(),
                        if neighbors.len() == 1 {
                            "processor"
                        } else {
                            "processors"
                        },
                        post_processor.name(),
                        index.len(),
                        if index.len() == 1 {
                            "processor"
                        } else {
                            "processors"
                        },
                    )?;
                }
                _ => unreachable!("")
            }
        }

        self.write(c + 1, neighbors, f)
    }

    fn write_merge(
        &self,
        c: usize,
        f: &mut Formatter,
        index: &Vec<NodeIndex>,
        neighbors: &Vec<NodeIndex>,
    ) -> std::fmt::Result {
        if !neighbors.is_empty() {
            let graph = &self.0.graph;
            let prev_processor = graph.node_weight(neighbors[0]);
            let post_processor = graph.node_weight(index[0]);

            match (prev_processor, post_processor) {
                (Some(prev_processor), Some(post_processor)) => {
                    write!(
                        f,
                        "{}Merge ({} × {} {}) to ({} × {})\n",
                        "  ".repeat(c),
                        prev_processor.name(), neighbors.len(),
                        if neighbors.len() == 1 {
                            "processor"
                        } else {
                            "processors"
                        },
                        post_processor.name(), "1"
                    )?;
                }
                _ => unreachable!("")
            }
        }

        self.write(c + 1, neighbors, f)
    }
}

impl<'a> Display for IndentDisplayWrap<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let graph = &self.0.graph;
        let outgoing_externals = graph.externals(Direction::Outgoing);
        let externals_nodes: Vec<NodeIndex> = outgoing_externals.collect();
        match externals_nodes.is_empty() {
            true => Ok(()),
            false => self.write(0, &externals_nodes, f)
        }
    }
}
