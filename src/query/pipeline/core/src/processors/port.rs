// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use crate::processors::Profile;
use crate::processors::ProfileStatisticsName;
use crate::processors::UpdateTrigger;
use crate::unsafe_cell_wrap::UnSafeCellWrap;

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

impl Drop for SharedStatus {
    fn drop(&mut self) {
        unsafe {
            let address = self.swap(std::ptr::null_mut(), 0, HAS_DATA);

            if !address.is_null() {
                drop(Box::from_raw(address));
            }
        }
    }
}

impl SharedStatus {
    pub fn create() -> Arc<SharedStatus> {
        Arc::new(SharedStatus {
            data: AtomicPtr::new(std::ptr::null_mut()),
        })
    }

    #[inline(always)]
    pub fn swap(
        &self,
        data: *mut SharedData,
        set_flags: usize,
        unset_flags: usize,
    ) -> *mut SharedData {
        let mut expected = std::ptr::null_mut();
        let mut desired = (data as usize | set_flags) as *mut SharedData;

        loop {
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

    #[inline(always)]
    pub fn set_flags(&self, set_flags: usize, unset_flags: usize) -> usize {
        let mut expected = std::ptr::null_mut();
        let mut desired = set_flags as *mut SharedData;
        loop {
            match self.data.compare_exchange_weak(
                expected,
                desired,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(old_value) => {
                    return old_value as usize & FLAGS_MASK;
                }
                Err(new_expected) => {
                    expected = new_expected;
                    let address = expected as usize;
                    let desired_data = address & UNSET_FLAGS_MASK;
                    let desired_flags = (address & FLAGS_MASK & !unset_flags) | set_flags;
                    desired = (desired_data | desired_flags) as *mut SharedData;
                }
            }
        }
    }

    #[inline(always)]
    pub fn get_flags(&self) -> usize {
        self.data.load(Ordering::SeqCst) as usize & FLAGS_MASK
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

    #[inline(always)]
    pub fn finish(&self) {
        unsafe {
            let flags = self.shared.set_flags(IS_FINISHED, IS_FINISHED);

            if flags & IS_FINISHED == 0 {
                UpdateTrigger::update_input(&self.update_trigger);
            }
        }
    }

    pub fn get_flags(&self) -> usize {
        self.shared.get_flags()
    }

    #[inline(always)]
    pub fn is_finished(&self) -> bool {
        let flags = self.shared.get_flags();
        ((flags & IS_FINISHED) == IS_FINISHED) && ((flags & HAS_DATA) == 0)
    }

    pub fn is_need_data(&self) -> bool {
        self.shared.get_flags() & NEED_DATA != 0
    }

    #[inline(always)]
    pub fn set_need_data(&self) {
        unsafe {
            let flags = self.shared.set_flags(NEED_DATA, NEED_DATA);
            if flags & NEED_DATA == 0 {
                UpdateTrigger::update_input(&self.update_trigger);
            }
        }
    }

    #[inline(always)]
    pub fn set_not_need_data(&self) {
        self.shared.set_flags(0, NEED_DATA);
    }

    #[inline(always)]
    pub fn has_data(&self) -> bool {
        (self.shared.get_flags() & HAS_DATA) != 0
    }

    #[inline(always)]
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

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn set_shared(&self, shared: Arc<SharedStatus>) {
        self.shared.set_value(shared);
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn set_trigger(&self, update_trigger: *mut UpdateTrigger) {
        self.update_trigger.set_value(update_trigger)
    }
}

pub struct OutputPort {
    record_profile: UnSafeCellWrap<bool>,
    shared: UnSafeCellWrap<Arc<SharedStatus>>,
    update_trigger: UnSafeCellWrap<*mut UpdateTrigger>,
}

impl OutputPort {
    pub fn create() -> Arc<OutputPort> {
        Arc::new(OutputPort {
            record_profile: UnSafeCellWrap::create(false),
            shared: UnSafeCellWrap::create(SharedStatus::create()),
            update_trigger: UnSafeCellWrap::create(std::ptr::null_mut()),
        })
    }

    #[inline(always)]
    pub fn push_data(&self, data: Result<DataBlock>) {
        unsafe {
            UpdateTrigger::update_output(&self.update_trigger);

            if *self.record_profile {
                if let Ok(data_block) = &data {
                    Profile::record_usize_profile(
                        ProfileStatisticsName::OutputRows,
                        data_block.num_rows(),
                    );
                    Profile::record_usize_profile(
                        ProfileStatisticsName::OutputBytes,
                        data_block.memory_size(),
                    );
                }
            }

            let data = Box::into_raw(Box::new(SharedData(data)));
            self.shared.swap(data, HAS_DATA, HAS_DATA);
        }
    }

    #[inline(always)]
    pub fn finish(&self) {
        unsafe {
            let flags = self.shared.set_flags(IS_FINISHED, IS_FINISHED);

            if flags & IS_FINISHED == 0 {
                UpdateTrigger::update_output(&self.update_trigger);
            }
        }
    }

    #[inline(always)]
    pub fn is_finished(&self) -> bool {
        (self.shared.get_flags() & IS_FINISHED) != 0
    }

    pub fn has_data(&self) -> bool {
        (self.shared.get_flags() & HAS_DATA) != 0
    }

    pub fn is_need_data(&self) -> bool {
        (self.shared.get_flags() & NEED_DATA) != 0
    }

    #[inline(always)]
    pub fn can_push(&self) -> bool {
        let flags = self.shared.get_flags();
        ((flags & NEED_DATA) == NEED_DATA) && ((flags & HAS_DATA) == 0)
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn set_shared(&self, shared: Arc<SharedStatus>) {
        self.shared.set_value(shared);
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn set_trigger(&self, update_trigger: *mut UpdateTrigger) {
        self.update_trigger.set_value(update_trigger)
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn record_profile(&self) {
        self.record_profile.set_value(true);
    }
}

/// Connect input and output ports.
///
/// # Safety
pub unsafe fn connect(input: &InputPort, output: &OutputPort) {
    let shared_status = SharedStatus::create();

    input.set_shared(shared_status.clone());
    output.set_shared(shared_status);
}
