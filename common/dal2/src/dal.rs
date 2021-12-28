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

use common_exception::Result;

use crate::ops::*;

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
    pub fn write(&self, path: &'d str, size: u64) -> WriteBuilder<S> {
        WriteBuilder::new(self.s.clone(), path, size)
    }
}

impl<'d, S> DataAccessor<'d, S>
where S: Stat<S>
{
    pub async fn stat(&self, path: &'d str) -> Result<Object> {
        self.s.stat(path).await
    }
}

impl<'d, S> DataAccessor<'d, S>
where S: Delete<S>
{
    pub async fn delete(&self, path: &'d str) -> Result<()> {
        self.s.delete(path).await
    }
}
