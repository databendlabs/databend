use std::cell::UnsafeCell;
use std::ops::Deref;
use std::sync::Arc;
use common_exception::Result;
use petgraph::prelude::{NodeIndex, StableGraph};
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::{PortTrigger, UpdateList};

pub struct Edge(Arc<InputPort>, Arc<OutputPort>);

pub struct NewPipe {
    processor: ProcessorPtr,
    updated_inputs_list: UpdateList,
    updated_outputs_list: UpdateList,
}

pub struct NewPipeline {
    pipes: Vec<Vec<NewPipe>>,
}

impl NewPipeline {
    pub fn create(graph: StableGraph<ProcessorPtr, Edge>) -> NewPipeline {
        NewPipeline { pipes: Vec::new() }
    }

    pub fn add_source<F>(&mut self, parallel: usize, f: F) -> Result<()>
        where F: Fn(usize, OutputPort) -> Result<ProcessorPtr>
    {
        let mut pipe = Vec::with_capacity(parallel);

        for index in 0..parallel {
            let updated_inputs_list = UpdateList::create();
            let updated_outputs_list = UpdateList::create();
            let processor = f(index, OutputPort::create(updated_outputs_list.clone()))?;
            pipe.push(NewPipe { processor, updated_inputs_list, updated_outputs_list });
        }

        self.pipes.push(pipe);
        Ok(())
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
        let trigger = self.0.get_trigger();

        self.0.get_trigger(PortTrigger::create(pid, update_list.clone()))
    }

    pub fn set_output_trigger(&self, pid: usize, update_list: &Arc<UpdateList>) {
        self.1.get_trigger(PortTrigger::create(pid, update_list.clone()))
    }
}
