// Copyright 2020 Datafuse Labs.
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

use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

static GLOBAL_SEQ: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub struct Seq(usize);

impl Default for Seq {
    fn default() -> Self {
        Seq(GLOBAL_SEQ.fetch_add(1, Ordering::SeqCst))
    }
}

impl Deref for Seq {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
