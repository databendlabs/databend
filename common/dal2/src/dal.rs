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

use std::sync::Arc;

use crate::error::Result;
use crate::ops::Delete;
use crate::ops::Object;
use crate::ops::Read;
use crate::ops::ReadBuilder;
use crate::ops::Stat;
use crate::ops::Write;
use crate::ops::WriteBuilder;

pub struct DataAccessor<S> {
    s: Arc<S>,
}

impl<S> DataAccessor<S> {
    pub fn new(s: S) -> DataAccessor<S> {
        DataAccessor { s: Arc::new(s) }
    }
}

impl<S> DataAccessor<S>
where S: Read<S>
{
    pub fn read(&self, path: &str) -> ReadBuilder<S> {
        ReadBuilder::new(self.s.clone(), path)
    }
}

impl<S> DataAccessor<S>
where S: Write<S>
{
    pub fn write(&self, path: &str, size: u64) -> WriteBuilder<S> {
        WriteBuilder::new(self.s.clone(), path, size)
    }
}

impl<S> DataAccessor<S>
where S: Stat<S>
{
    pub async fn stat(&self, path: &str) -> Result<Object> {
        self.s.stat(path).await
    }
}

impl<S> DataAccessor<S>
where S: Delete<S>
{
    pub async fn delete(&self, path: &str) -> Result<()> {
        self.s.delete(path).await
    }
}

impl<S> Clone for DataAccessor<S> {
    fn clone(&self) -> Self {
        DataAccessor { s: self.s.clone() }
    }
}
