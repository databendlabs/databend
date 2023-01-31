// Copyright 2021 Datafuse Labs.
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

use std::alloc::Layout;
use std::collections::VecDeque;
use std::ptr::NonNull;
use std::sync::Arc;

use bumpalo::Bump;
use parking_lot::RwLock;

pub struct Area {
    bump: Bump,
}

impl Area {
    pub fn create() -> Area {
        Area { bump: Bump::new() }
    }

    pub fn alloc_layout(&mut self, layout: Layout) -> NonNull<u8> {
        self.bump.alloc_layout(layout)
    }
}

unsafe impl Send for Area {}

#[derive(Clone)]
pub struct ArenaHolder {
    values: Arc<RwLock<VecDeque<Area>>>,
}

impl ArenaHolder {
    pub fn create() -> ArenaHolder {
        ArenaHolder {
            values: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    pub fn put_area(&self, area: Option<Area>) {
        if let Some(area) = area {
            let mut values = self.values.write();
            values.push_back(area);
            tracing::info!("Putting arena into holder, current size: {}", values.len());
        }
    }
}

unsafe impl Send for ArenaHolder {}
