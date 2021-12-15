// running graph

use std::collections::HashMap;
use common_infallible::{Mutex, RwLock};
use crate::pipelines::processors::Processor;
use common_exception::Result;

enum RunningState {
    Idle,
    Preparing,
    Sync,
    Async,
    Finished,
}

struct RunningProcessor {
    state: Mutex<RunningState>,
    inputs: Vec<usize>,
    outputs: Vec<usize>,
}

impl RunningProcessor {
    pub fn create(processor: &dyn Processor) -> RunningProcessor {
        RunningProcessor {
            state: Mutex::new(RunningState::Idle),
            inputs: vec![],
            outputs: vec![],
        }
    }
}

struct RunningGraphState {
    nodes: Vec<RunningProcessor>,
    raw_processors: Vec<dyn Processor>,
}

impl RunningGraphState {
    pub fn create(processors: Vec<dyn Processor>, edges: HashMap<usize, Vec<usize>>) -> RunningGraphState {
        let mut nodes = Vec::with_capacity(processors.len());

        for processor in &processors {
            nodes.push(RunningProcessor::create(processor));
        }

        for (from, to) in edges {
            {
                for index in &to {
                    let to_node = &mut nodes[*index];
                    to_node.inputs.push(from);
                }
            }

            {
                let from_node = &mut nodes[from];
                for index in &to {
                    from_node.outputs.push(*index);
                }
            }
        }

        RunningGraphState { nodes, raw_processors: processors }
    }

    pub fn schedule_next(state: &RwLock<RunningGraphState>) -> Result<()> {
        let graph = state.upgradable_read();
        // TODO:

        unimplemented!()
    }
}

pub struct RunningGraph(RwLock<RunningGraphState>);

impl RunningGraph {
    pub fn create(processors: Vec<dyn Processor>, edges: HashMap<usize, Vec<usize>>) -> RunningGraph {
        RunningGraph(RwLock::new(RunningGraphState::create(processors, edges)))
    }
}

// Syntactic sugar for RunningGraph
impl RunningGraph {
    pub fn schedule_next(&self) -> Result<()> {
        RunningGraphState::schedule_next(&self.0)
    }
}
