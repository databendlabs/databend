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

use std::marker::PhantomData;
use std::sync::Arc;

use crate::ops::Read;
use crate::ops::ReadBuilder;
use crate::ops::Write;
use crate::ops::WriteBuilder;

pub struct DataAccessor<'d, S> {
    s: Arc<S>,
    phantom: PhantomData<&'d ()>,
}

impl<'d, S> DataAccessor<'d, S> {
    pub fn new(s: S) -> DataAccessor<'d, S> {
        DataAccessor {
            s: Arc::new(s),
            phantom: PhantomData::default(),
        }
    }
}

impl<'d, S> DataAccessor<'d, S>
where S: Read<S>
{
    pub fn read(&self, path: &'d str) -> ReadBuilder<S> {
        ReadBuilder::new(self.s.clone(), path)
    }
}

impl<'d, S> DataAccessor<'d, S>
where S: Write<S>
{
    pub fn write(&self, path: &'d str, size: usize) -> WriteBuilder<S> {
        WriteBuilder::new(self.s.clone(), path, size)
    }
}
