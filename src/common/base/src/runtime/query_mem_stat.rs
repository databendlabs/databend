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

use std::cell::RefCell;
use std::sync::Arc;

use crate::runtime::MemStat;

thread_local! {
    static QUERY_MEM_STATE: RefCell<Option<Arc<MemStat>>> = const { RefCell::new(None) };
}

pub struct QueryMemState {}

impl QueryMemState {
    pub fn attach(mem_stat: Arc<MemStat>) -> QueryMemStatGuard {
        QUERY_MEM_STATE.with(|v: &RefCell<Option<Arc<MemStat>>>| {
            let mut borrow_mut = v.borrow_mut();
            let old = borrow_mut.clone();
            *borrow_mut = Some(mem_stat);
            QueryMemStatGuard { save: old }
        })
    }

    pub fn current() -> Option<Arc<MemStat>> {
        QUERY_MEM_STATE.with(|v: &RefCell<Option<Arc<MemStat>>>| v.borrow_mut().clone())
    }
}

pub struct QueryMemStatGuard {
    save: Option<Arc<MemStat>>,
}

impl Drop for QueryMemStatGuard {
    fn drop(&mut self) {
        let saved = self.save.clone();
        QUERY_MEM_STATE.with(|v: &RefCell<Option<Arc<MemStat>>>| {
            let mut borrow_mut = v.borrow_mut();
            *borrow_mut = saved;
        })
    }
}
