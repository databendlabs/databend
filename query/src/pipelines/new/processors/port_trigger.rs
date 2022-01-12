use std::cell::UnsafeCell;
use std::sync::Arc;
use petgraph::prelude::EdgeIndex;

#[derive(Clone)]
pub struct UpdateList {
    pub inner: Arc<UnsafeCell<UpdateListMutable>>,
}

unsafe impl Send for UpdateList {}

struct UpdateListMutable {
    list: Vec<EdgeIndex>,
    version: usize,
}

impl UpdateList {
    pub fn create() -> UpdateList {
        UpdateList {
            inner: Arc::new(UnsafeCell::new(UpdateListMutable {
                list: vec![],
                version: 0,
            }))
        }
    }

    #[inline(always)]
    pub fn update(&self, index: EdgeIndex) {
        unsafe {
            // TODO: version?
            (&mut *self.inner.get()).push(index)
        }
    }

    pub unsafe fn clear(&self) {
        (&mut *self.inner.get()).clear()
    }
}


pub struct PortTrigger {
    pid: usize,
    update_list: Option<Arc<UpdateList>>,
}

unsafe impl Send for PortTrigger {}

impl PortTrigger {
    pub fn create() -> PortTrigger {
        PortTrigger {
            pid: 0,
            update_list: None,
        }
    }

    #[inline(always)]
    pub fn update(&self) {
        // self.update_list
        // self.update_list.update(self.pid)
    }
}

