use std::cell::UnsafeCell;
use std::ops::Deref;
use std::sync::Arc;
use petgraph::prelude::{NodeIndex, StableGraph};
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::{PortTrigger, UpdateList};

pub struct Edge(Arc<InputPort>, Arc<OutputPort>);

pub struct NewPipeline {
    graph: StableGraph<ProcessorPtr, Edge>,

}

impl NewPipeline {
    pub fn create(graph: StableGraph<ProcessorPtr, Edge>) -> NewPipeline {
        NewPipeline { graph }
    }
}

impl Deref for NewPipeline {
    type Target = StableGraph<ProcessorPtr, Edge>;

    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

impl Edge {
    pub fn set_input_trigger(&self, pid: usize, update_list: &Arc<UpdateList>) {
        self.0.set_trigger(PortTrigger::create(pid, update_list.clone()))
    }

    pub fn set_output_trigger(&self, pid: usize, update_list: &Arc<UpdateList>) {
        self.1.set_trigger(PortTrigger::create(pid, update_list.clone()))
    }
}
