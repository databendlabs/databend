use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::Result;

use crate::pipelines::new::processors::UpdateTrigger;
use crate::pipelines::new::unsafe_cell_wrap::UnSafeCellWrap;

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

pub struct InputPort {
    shared: UnSafeCellWrap<Arc<SharedStatus>>,
    update_trigger: UnSafeCellWrap<*mut UpdateTrigger>,
}

impl InputPort {
    pub fn create() -> Arc<InputPort> {
        Arc::new(InputPort {
            shared: UnSafeCellWrap::create(SharedStatus::create()),
            update_trigger: UnSafeCellWrap::create(std::ptr::null_mut()),
        })
    }

    pub fn finish(&self) {
        unsafe {
            UpdateTrigger::update_input(&self.update_trigger);

            self.shared.set_flags(IS_FINISHED, IS_FINISHED);
        }
    }

    pub fn is_finished(&self) -> bool {
        (self.shared.get_flags() & IS_FINISHED) != 0
    }

    pub fn set_need_data(&self) {
        unsafe {
            UpdateTrigger::update_input(&self.update_trigger);
            self.shared.set_flags(NEED_DATA, NEED_DATA);
        }
    }

    pub fn set_not_need_data(&self) {
        self.shared.set_flags(0, NEED_DATA);
    }

    pub fn has_data(&self) -> bool {
        (self.shared.get_flags() & HAS_DATA) != 0
    }

    pub fn pull_data(&self) -> Option<Result<DataBlock>> {
        unsafe {
            UpdateTrigger::update_input(&self.update_trigger);
            let unset_flags = HAS_DATA | NEED_DATA;
            match self.shared.swap(std::ptr::null_mut(), 0, unset_flags) {
                address if address.is_null() => None,
                address => Some((*Box::from_raw(address)).0),
            }
        }
    }

    pub unsafe fn set_shared(&self, shared: Arc<SharedStatus>) {
        self.shared.set_value(shared);
    }

    pub unsafe fn set_trigger(&self, update_trigger: *mut UpdateTrigger) {
        self.update_trigger.set_value(update_trigger)
    }
}

pub struct OutputPort {
    shared: UnSafeCellWrap<Arc<SharedStatus>>,
    update_trigger: UnSafeCellWrap<*mut UpdateTrigger>,
}

impl OutputPort {
    pub fn create() -> Arc<OutputPort> {
        Arc::new(OutputPort {
            shared: UnSafeCellWrap::create(SharedStatus::create()),
            update_trigger: UnSafeCellWrap::create(std::ptr::null_mut()),
        })
    }

    pub fn push_data(&self, data: Result<DataBlock>) {
        unsafe {
            UpdateTrigger::update_output(&self.update_trigger);

            let data = Box::into_raw(Box::new(SharedData(data)));
            self.shared.swap(data, HAS_DATA, HAS_DATA);
        }
    }

    pub fn finish(&self) {
        unsafe {
            UpdateTrigger::update_output(&self.update_trigger);

            self.shared.set_flags(IS_FINISHED, IS_FINISHED);
        }
    }

    pub fn is_finished(&self) -> bool {
        (self.shared.get_flags() & IS_FINISHED) != 0
    }

    pub fn can_push(&self) -> bool {
        let flags = self.shared.get_flags();
        ((flags & NEED_DATA) != 1) && ((flags & HAS_DATA) == 0)
    }

    pub unsafe fn set_shared(&self, shared: Arc<SharedStatus>) {
        self.shared.set_value(shared);
    }

    pub unsafe fn set_trigger(&self, update_trigger: *mut UpdateTrigger) {
        self.update_trigger.set_value(update_trigger)
    }
}

pub unsafe fn connect(input: &InputPort, output: &OutputPort) {
    let shared_status = SharedStatus::create();

    input.set_shared(shared_status.clone());
    output.set_shared(shared_status);
}
