use std::cell::UnsafeCell;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::Result;

use crate::pipelines::new::processors::{UpdateTrigger};

const HAS_DATA: usize = 0b1;
const NEED_DATA: usize = 0b10;
const IS_FINISHED: usize = 0b100;

const FLAGS_MASK: usize = 0b111;
const UNSET_FLAGS_MASK: usize = !FLAGS_MASK;

#[repr(align(8))]
pub struct SharedData(pub Result<DataBlock>);

pub struct SharedStatus {
    data: AtomicPtr<SharedData>,
}

unsafe impl Send for SharedStatus {}

impl SharedStatus {
    pub fn create() -> Arc<SharedStatus> {
        Arc::new(SharedStatus { data: AtomicPtr::new(std::ptr::null_mut()) })
    }

    pub fn swap(&self, data: *mut SharedData, set_flags: usize, unset_flags: usize) -> *mut SharedData {
        let mut expected = std::ptr::null_mut();
        let mut desired = (data as usize | set_flags) as *mut SharedData;

        loop {
            unsafe {
                match self.data.compare_exchange_weak(
                    expected,
                    desired,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Err(new_expected) => {
                        expected = new_expected;
                        let address = expected as usize;
                        let desired_data = desired as usize & UNSET_FLAGS_MASK;
                        let desired_flags = (address & FLAGS_MASK & !unset_flags) | set_flags;
                        desired = (desired_data | desired_flags) as *mut SharedData;
                    }
                    Ok(old_value) => {
                        let old_value_ptr = old_value as usize;
                        return (old_value_ptr & UNSET_FLAGS_MASK) as *mut SharedData;
                    }
                }
            }
        }
    }

    pub fn set_flags(&self, set_flags: usize, unset_flags: usize) {
        let mut expected = std::ptr::null_mut();
        let mut desired = set_flags as *mut SharedData;

        unsafe {
            while let Err(new_expected) = self.data.compare_exchange_weak(
                expected,
                desired,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                expected = new_expected;
                let address = desired as usize;
                let desired_data = address & UNSET_FLAGS_MASK;
                let desired_flags = (address & FLAGS_MASK & !unset_flags) | set_flags;
                desired = (desired_data | desired_flags) as *mut SharedData;
            }
        }
    }

    pub fn get_flags(&self) -> usize {
        self.data.load(Ordering::Relaxed) as usize & FLAGS_MASK
    }
}

struct InputPortShared {
    pub base: Arc<SharedStatus>,
    pub update_trigger: *mut UpdateTrigger,
}

impl InputPortShared {
    pub fn create() -> InputPortShared {
        InputPortShared {
            base: SharedStatus::create(),
            update_trigger: std::ptr::null_mut(),
        }
    }

    pub fn get_flags(&self) -> usize {
        self.base.get_flags()
    }

    pub fn pull_data(&self) -> Option<Result<DataBlock>> {
        unsafe {
            UpdateTrigger::update(self.update_trigger);
            let unset_flags = HAS_DATA | NEED_DATA;
            match (&*self.base).swap(std::ptr::null_mut(), 0, unset_flags) {
                address if address.is_null() => None,
                address => Some((*Box::from_raw(address)).0),
            }
        }
    }
}

impl Drop for InputPortShared {
    fn drop(&mut self) {
        unsafe {
            if !self.update_trigger.is_null() {
                Box::from_raw(self.update_trigger);
            }
        }
    }
}

pub struct InputPort {
    shared: Arc<UnsafeCell<InputPortShared>>,
}

unsafe impl Send for InputPort {}

unsafe impl Sync for InputPort {}

impl InputPort {
    pub fn create() -> InputPort {
        InputPort {
            shared: Arc::new(UnsafeCell::new(InputPortShared::create())),
        }
    }

    pub fn has_data(&self) -> bool {
        (self.get_shared().get_flags() & HAS_DATA) != 0
    }

    pub fn pull_data(&self) -> Option<Result<DataBlock>> {
        self.get_shared().pull_data()
    }

    pub unsafe fn set_shared(&self, shared: Arc<SharedStatus>) {
        self.get_mut_shared().base = shared
    }

    pub unsafe fn set_trigger(&self, update_trigger: *mut UpdateTrigger) {
        unimplemented!()
    }

    fn get_shared(&self) -> &InputPortShared {
        unsafe { &*self.shared.get() }
    }

    fn get_mut_shared(&self) -> &mut InputPortShared {
        unsafe { &mut *self.shared.get() }
    }
}

struct OutputPortShared {
    pub base: Arc<SharedStatus>,
    pub update_trigger: *mut UpdateTrigger,
}

impl OutputPortShared {
    pub fn create() -> OutputPortShared {
        OutputPortShared {
            base: SharedStatus::create(),
            update_trigger: std::ptr::null_mut(),
        }
    }

    pub fn push_data(&self, data: Result<DataBlock>) {
        UpdateTrigger::update(self.update_trigger);

        let data = Box::into_raw(Box::new(SharedData(data)));
        self.base.swap(data, HAS_DATA, HAS_DATA);
    }

    pub fn get_flags(&self) -> usize {
        self.base.get_flags()
    }
}

impl Drop for OutputPortShared {
    fn drop(&mut self) {
        unsafe {
            if !self.update_trigger.is_null() {
                Box::from_raw(self.update_trigger);
            }
        }
    }
}

#[derive(Clone)]
pub struct OutputPort {
    shared: Arc<UnsafeCell<OutputPortShared>>,
}

unsafe impl Send for OutputPort {}

unsafe impl Sync for OutputPort {}

impl OutputPort {
    pub fn create() -> OutputPort {
        OutputPort { shared: Arc::new(UnsafeCell::new(OutputPortShared::create())) }
    }

    pub fn push_data(&self, data: Result<DataBlock>) {
        self.get_shared().push_data(data);
    }

    pub fn finish(&self) {
        unimplemented!()
    }

    pub fn is_finished(&self) -> bool {
        (self.get_shared().get_flags() & IS_FINISHED) != 0
    }

    pub fn can_push(&self) -> bool {
        let flags = self.get_shared().get_flags();
        ((flags & NEED_DATA) != 1) && ((flags & HAS_DATA) == 0)
    }

    pub unsafe fn set_shared(&self, shared: Arc<SharedStatus>) {
        self.get_mut_shared().base = shared;
    }

    pub unsafe fn set_trigger(&self, update_trigger: *mut UpdateTrigger) {
        unimplemented!()
    }

    fn get_shared(&self) -> &OutputPortShared {
        unsafe { &*self.shared.get() }
    }

    fn get_mut_shared(&self) -> &mut OutputPortShared {
        unsafe { &mut *self.shared.get() }
    }
}

pub unsafe fn connect(input: &InputPort, output: &OutputPort) {
    let shared_status = SharedStatus::create();

    input.set_shared(shared_status.clone());
    output.set_shared(shared_status);
}
