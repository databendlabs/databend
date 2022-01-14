use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::sync::Arc;
use petgraph::prelude::EdgeIndex;

#[derive(Clone)]
pub struct UpdateList {
    inner: Arc<UnsafeCell<UpdateListMutable>>,
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
    pub unsafe fn update(&self, index: EdgeIndex) {
        let inner = &mut *self.inner.get();
        // TODO: version
        inner.list.push(index);
    }

    pub unsafe fn trigger(&self, queue: &mut VecDeque<EdgeIndex>) {
        let inner = &mut *self.inner.get();

        inner.version += 1;
        while let Some(index) = inner.list.pop() {
            queue.push_front(index);
        }
    }
}


pub struct UpdateTrigger {
    index: EdgeIndex,
    update_list: UpdateList,
}

unsafe impl Send for UpdateTrigger {}

impl UpdateTrigger {
    pub fn create(index: EdgeIndex, update_list: UpdateList) -> *mut UpdateTrigger {
        Box::into_raw(Box::new(UpdateTrigger { index, update_list }))
    }

    #[inline(always)]
    pub fn update(self_: *mut UpdateTrigger) {
        unsafe {
            if !self_.is_null() {
                let self_ = &mut *self_;
                self_.update_list.update(self_.index);
            }
        }
    }
}

