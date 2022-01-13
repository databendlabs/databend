use std::cell::UnsafeCell;
use std::ops::Deref;
use std::sync::Arc;
use common_exception::Result;
use petgraph::prelude::{NodeIndex, StableGraph};
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::{UpdateTrigger, UpdateList};

pub struct Edge(Arc<InputPort>, Arc<OutputPort>);

pub struct NewPipe {
    pub processor: ProcessorPtr,
    pub inputs_port: Vec<InputPort>,
    pub outputs_port: Vec<OutputPort>,
}

impl NewPipe {
    pub fn create_source(processor: ProcessorPtr, output: OutputPort) -> NewPipe {
        NewPipe {
            processor,
            inputs_port: vec![],
            outputs_port: vec![output],
        }
    }
}

pub struct NewPipeline {
    pub pipes: Vec<Vec<NewPipe>>,
}

impl NewPipeline {
    pub fn create(graph: StableGraph<ProcessorPtr, Edge>) -> NewPipeline {
        NewPipeline { pipes: Vec::new() }
    }

    pub fn add_simple_transform() -> NewPipeline {

    }
}

impl Deref for NewPipeline {
    type Target = StableGraph<ProcessorPtr, Edge>;

    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}
