// running graph

use std::collections::HashMap;
use std::sync::Arc;
use common_infallible::{Mutex, RwLock};
use common_exception::{ErrorCode, Result};
use crate::pipelines::new::processors::{create_port, PortReactor, Processor, Processors};

pub enum RunningState {
    Idle,
    Preparing,
    Processing,
    Finished,
}

pub struct RunningProcessor {
    state: Mutex<RunningState>,
    inputs: Vec<usize>,
    outputs: Vec<usize>,
}

impl RunningProcessor {
    pub fn create(processor: &Box<dyn Processor>) -> Arc<RunningProcessor> {
        Arc::new(RunningProcessor {
            state: Mutex::new(RunningState::Idle),
            inputs: vec![],
            outputs: vec![],
        })
    }
}

struct RunningGraphState {
    nodes: Vec<Arc<RunningProcessor>>,
    raw_processors: Processors,
}

impl RunningGraphState {
    pub fn create(mut processors: Processors, edges: Vec<(usize, usize)>) -> Result<RunningGraphState> {
        let mut nodes = Vec::with_capacity(processors.len());

        for processor in &processors {
            nodes.push(RunningProcessor::create(processor));
        }

        for (input, output) in edges {
            if input == output {
                return Err(ErrorCode::IllegalPipelineState(""));
            }

            let (input_port, output_port) = create_port(&nodes, input, output);
            processors[input].connect_input(input_port)?;
            processors[output].connect_output(output_port)?;
        }

        Ok(RunningGraphState { nodes, raw_processors: processors })
    }

    pub fn initialize_executor(state: &RwLock<RunningGraphState>) -> Result<()> {
        {
            let graph = state.read();
            // TODO: init executor
        }

        RunningGraphState::schedule_next(state)
    }

    pub fn schedule_next(state: &RwLock<RunningGraphState>) -> Result<()> {
        let graph = state.upgradable_read();
        // TODO:

        unimplemented!()
    }
}

pub struct RunningGraph(RwLock<RunningGraphState>);

impl RunningGraph {
    pub fn create(nodes: Processors, edges: Vec<(usize, usize)>) -> Result<RunningGraph> {
        Ok(RunningGraph(RwLock::new(RunningGraphState::create(nodes, edges)?)))
    }
}

// Syntactic sugar for RunningGraph
impl RunningGraph {
    pub fn schedule_next(&self) -> Result<()> {
        RunningGraphState::schedule_next(&self.0)
    }

    pub fn initialize_executor(&self) -> Result<()> {
        RunningGraphState::initialize_executor(&self.0)
    }
}

impl PortReactor<usize> for RunningProcessor {
    fn on_push(&self, push_to: usize) {
        //
        unimplemented!("")
    }

    fn on_pull(&self, pull_from: usize) {
        //
        unimplemented!("")
    }
}
