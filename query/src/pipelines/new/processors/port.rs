use std::cell::UnsafeCell;
use std::intrinsics::unreachable;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::Result;
use common_infallible::Mutex;
use futures::future::Shared;

use crate::pipelines::new::executor::RunningProcessor;
use crate::pipelines::new::processors::PortTrigger;

const HAS_DATA: usize = 1;

const FLAGS_MASK: usize = 0b111;
const UNSET_FLAGS_MASK: usize = !FLAGS_MASK;

#[repr(align(8))]
pub struct SharedData(pub Result<DataBlock>);

pub struct SharedStatus {
    data: UnsafeCell<Arc<AtomicPtr<SharedData>>>,
}

unsafe impl Send for SharedStatus {}

impl SharedStatus {
    pub fn create() -> SharedStatus {
        SharedStatus {
            data: UnsafeCell::new(Arc::new(AtomicPtr::new(std::ptr::null_mut()))),
        }
    }

    pub unsafe fn set_data_ptr(&self, data: &Arc<AtomicPtr<SharedData>>) {
        (*self.data.get()) = data.clone()
    }

    pub unsafe fn get_data_ptr(&self) -> &Arc<AtomicPtr<SharedData>> {
        &(*self.data.get())
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
            unsafe {
                match self.get_data_ptr().compare_exchange_weak(
                    expected,
                    desired,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Err(new_expected) => {
                        expected = new_expected;
                    }
                    Ok(old_value) => {
                        let old_value_ptr = old_value as usize;

                        return match old_value_ptr & FLAGS_MASK {
                            HAS_DATA => {
                                let raw_ptr = (old_value_ptr & UNSET_FLAGS_MASK) as *mut SharedData;
                                Some((*Box::from_raw(raw_ptr)).0)
                            }
                            _ => None,
                        };
                    }
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

pub struct InputPort {
    shared: SharedStatus,
}

unsafe impl Send for InputPort {}

unsafe impl Sync for InputPort {}

impl InputPort {
    pub fn create() -> Arc<InputPort> {
        Arc::new(InputPort {
            shared: SharedStatus::create()
        })
    }

    pub fn set_trigger(&self, trigger: Arc<PortTrigger>) {
        unimplemented!()
    }

    pub fn pull_data(&self) -> Option<Result<DataBlock>> {
        self.shared.swap(None, 0)
    }
}

pub struct OutputPort {
    shared: SharedStatus,
}

/// Safely:
unsafe impl Send for OutputPort {}

unsafe impl Sync for OutputPort {}

impl OutputPort {
    pub fn create() -> Arc<OutputPort> {
        Arc::new(OutputPort {
            shared: SharedStatus::create()
        })
    }

    pub fn set_trigger(&self, trigger: Arc<PortTrigger>) {
        unimplemented!()
    }

    pub fn push_data(&self, data: Result<DataBlock>) {
        // self.reactor.on_push(self.reactor_data);
        if let Some(value) = self.shared.swap(Some(data), HAS_DATA) {
            // It shouldn't have happened
            unreachable!("Cannot push data to port which already has data. old value {:?}", value);
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

pub unsafe fn connect(input: Arc<InputPort>, output: Arc<OutputPort>) {
    output.shared.set_data_ptr(input.shared.get_data_ptr());
}
