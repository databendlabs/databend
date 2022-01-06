use std::cell::UnsafeCell;
use std::sync::Arc;

pub struct UpdateList {
    update_list: UnsafeCell<Vec<usize>>,
}

unsafe impl Send for UpdateList {}

impl UpdateList {
    pub fn create() -> Arc<UpdateList> {
        Arc::new(UpdateList { update_list: UnsafeCell::new(vec![]) })
    }

    #[inline(always)]
    pub fn update(&self, pid: usize) {
        unsafe {
            // TODO: version?
            (&mut *self.update_list.get()).push(pid)
        }
    }
}

pub struct PortTrigger {
    pid: usize,
    update_list: Arc<UpdateList>,
}

unsafe impl Send for PortTrigger {}

impl PortTrigger {
    pub fn create(pid: usize, update_list: Arc<UpdateList>) -> Arc<PortTrigger> {
        Arc::new(PortTrigger { pid, update_list })
    }

    #[inline(always)]
    pub fn trigger(&self) {
        self.update_list.update(self.pid)
    }
}

