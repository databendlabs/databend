use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;

use petgraph::stable_graph::NodeIndex;
use petgraph::Direction;

use crate::pipelines::processors::processor_dag::ProcessorsDAG;

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
    fn write(&self, c: usize, index: &[NodeIndex], f: &mut Formatter<'_>) -> std::fmt::Result {
        let graph = &self.0.graph;
        match graph.node_weight(index[0]) {
            None => unreachable!("Not found graph node weight."),
            Some(processor) => {
                writeln!(
                    f,
                    "{}{} × {} {}",
                    "  ".repeat(c),
                    processor.name(),
                    index.len(),
                    if index.len() == 1 {
                        "processor"
                    } else {
                        "processors"
                    }
                )?;
            }
        };

        let mut neighbors_set = HashSet::new();
        for node_index in index {
            for neighbors_index in graph.neighbors_directed(*node_index, Direction::Incoming) {
                neighbors_set.insert(neighbors_index);
            }
        }

        let neighbors = neighbors_set.iter().cloned().collect::<Vec<_>>();

        match (index.len(), neighbors_set.len()) {
            (_, 0) => Ok(()),
            (left, right) if left == right => self.write(c + 1, &neighbors, f),
            (1, _) => self.write_merge(c + 1, f, index, &neighbors),
            _ => self.write_mixed(c + 1, f, index, &neighbors),
        }
    }

    fn write_mixed(
        &self,
        c: usize,
        f: &mut Formatter,
        index: &[NodeIndex],
        neighbors: &[NodeIndex],
    ) -> std::fmt::Result {
        if !neighbors.is_empty() {
            let graph = &self.0.graph;
            let prev_processor = graph.node_weight(neighbors[0]);
            let post_processor = graph.node_weight(index[0]);

            match (prev_processor, post_processor) {
                (Some(prev_processor), Some(post_processor)) => {
                    writeln!(
                        f,
                        "{}Mixed ({} × {} {}) to ({} × {} {})",
                        "  ".repeat(c),
                        prev_processor.name(),
                        neighbors.len(),
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
                _ => unreachable!(""),
            }
        }

        self.write(c + 1, neighbors, f)
    }

    fn write_merge(
        &self,
        c: usize,
        f: &mut Formatter,
        index: &[NodeIndex],
        neighbors: &[NodeIndex],
    ) -> std::fmt::Result {
        if !neighbors.is_empty() {
            let graph = &self.0.graph;
            let prev_processor = graph.node_weight(neighbors[0]);
            let post_processor = graph.node_weight(index[0]);

            match (prev_processor, post_processor) {
                (Some(prev_processor), Some(post_processor)) => {
                    writeln!(
                        f,
                        "{}Merge ({} × {} {}) to ({} × 1)",
                        "  ".repeat(c),
                        prev_processor.name(),
                        neighbors.len(),
                        if neighbors.len() == 1 {
                            "processor"
                        } else {
                            "processors"
                        },
                        post_processor.name(),
                    )?;
                }
                _ => unreachable!(""),
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
            false => self.write(0, &externals_nodes, f),
        }
    }
}
