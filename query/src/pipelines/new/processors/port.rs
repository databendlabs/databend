use std::cell::UnsafeCell;
use std::intrinsics::unreachable;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use futures::future::Shared;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_infallible::Mutex;
use crate::pipelines::new::executor::RunningProcessor;

const HAS_DATA: usize = 1;

const FLAGS_MASK: usize = 0b111;
const UNSET_FLAGS_MASK: usize = !FLAGS_MASK;

#[repr(align(8))]
struct SharedData(pub Result<DataBlock>);

pub struct SharedStatus {
    data: AtomicPtr<SharedData>,
}

impl SharedStatus {
    pub fn create() -> Arc<SharedStatus> {
        Arc::new(SharedStatus { data: AtomicPtr::new(std::ptr::null_mut()) })
    }

    pub fn swap(&self, data: Option<Result<DataBlock>>, flags: usize) -> Option<Result<DataBlock>> {
        let mut expected = std::ptr::null_mut();
        let desired = match data {
            None => std::ptr::null_mut(),
            Some(data) => {
                let new_data = Box::into_raw(Box::new(SharedData(data)));
                (new_data as usize | flags) as *mut SharedData
            }
        };

        loop {
            match self.data.compare_exchange_weak(expected, desired, Ordering::SeqCst, Ordering::Relaxed) {
                Err(new_expected) => { expected = new_expected; }
                Ok(old_value) => {
                    let old_value_ptr = old_value as usize;

                    return match old_value_ptr & FLAGS_MASK {
                        HAS_DATA => unsafe {
                            let raw_ptr = (old_value_ptr & UNSET_FLAGS_MASK) as *mut SharedData;
                            Some((*Box::from_raw(raw_ptr)).0)
                        }
                        _ => None,
                    };
                }
            }
        }
    }

    pub fn set_flags(&self, flags: usize) {
        // let mut expected = std::ptr::null_mut();
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
        self.shared.swap(None, 0)
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
        if let Some(_) = self.shared.swap(Some(data), HAS_DATA) {
            // It shouldn't have happened
            unreachable!("Cannot push data to port which already has data.");
        }
    }

    pub fn finish(&self) {
        unimplemented!()
    }

    pub fn is_finished(&self) -> bool {
        unimplemented!()
    }

    pub fn can_push(&self) -> bool {
        unimplemented!()
    }
}

unsafe impl<Data: Copy, T: PortReactor<Data> + Sized> Send for ReactiveInputPort<Data, T> {}
unsafe impl<Data: Copy, T: PortReactor<Data> + Sized> Send for ReactiveOutputPort<Data, T> {}

pub type InputPort = ReactiveInputPort<usize, RunningProcessor>;
pub type OutputPort = ReactiveOutputPort<usize, RunningProcessor>;

pub fn create_port(processors: &[Arc<RunningProcessor>], input: usize, output: usize) -> (InputPort, OutputPort) {
    let input_reactor = processors[input].clone();
    let output_reactor = processors[output].clone();

    let state = SharedStatus::create();
    (InputPort::create(state.clone(), input_reactor, output), OutputPort::create(state, output_reactor, input))
}

