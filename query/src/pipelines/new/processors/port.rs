use std::sync::Arc;
use std::sync::atomic::AtomicPtr;
use futures::future::Shared;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_infallible::Mutex;
use crate::pipelines::new::executor::RunningProcessor;

struct SharedStatus {
    // TODO: Very bad
    data: Mutex<Option<Result<DataBlock>>>,
}

impl SharedStatus {
    pub fn create() -> Arc<SharedStatus> {
        Arc::new(SharedStatus { data: Mutex::new(None) })
    }
}

pub trait PortReactor<Data> {
    fn on_push(&self, push_to: Data);

    fn on_pull(&self, pull_from: Data);
}

pub struct ReactiveInputPort<Data: Copy, T: PortReactor<Data> + Sized> {
    shared: Arc<SharedStatus>,

    reactor: Arc<T>,
    reactor_data: Data,
}

impl<Data: Copy, T: PortReactor<Data> + Sized> ReactiveInputPort<Data, T> {
    pub fn create(shared: Arc<SharedStatus>, reactor: Arc<T>, data: Data) -> Self {
        Self { reactor, reactor_data: data, shared }
    }

    pub fn pull_data(&self) -> Option<Result<DataBlock>> {
        self.reactor.on_pull(self.reactor_data);
        self.shared.data.lock().take()
    }
}

pub struct ReactiveOutputPort<Data: Copy, T: PortReactor<Data> + Sized> {
    shared: Arc<SharedStatus>,

    reactor: Arc<T>,
    reactor_data: Data,
}

impl<Data: Copy, T: PortReactor<Data> + Sized> ReactiveOutputPort<Data, T> {
    pub fn create(shared: Arc<SharedStatus>, reactor: Arc<T>, data: Data) -> Self {
        Self { reactor, reactor_data: data, shared }
    }

    pub fn push_data(&self, data: Result<DataBlock>) {
        self.reactor.on_push(self.reactor_data);
        *self.shared.data.lock() = Some(data);
    }
}

pub type InputPort = ReactiveInputPort<usize, RunningProcessor>;
pub type OutputPort = ReactiveOutputPort<usize, RunningProcessor>;

pub fn create_port(processors: &[Arc<RunningProcessor>], input: usize, output: usize) -> (InputPort, OutputPort) {
    let input_reactor = processors[input].clone();
    let output_reactor = processors[output].clone();

    let state = SharedStatus::create();
    (InputPort::create(state.clone(), input_reactor, output), OutputPort::create(state, output_reactor, input))
}

