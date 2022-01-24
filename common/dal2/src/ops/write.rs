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

use async_trait::async_trait;

use super::io::Reader;
use crate::error::Result;

/// `Write` will write data to the underlying storage.
#[async_trait]
pub trait Write<S: Send + Sync>: Send + Sync {
    async fn write(&self, r: Reader, args: &WriteBuilder<S>) -> Result<usize> {
        let (_, _) = (r, args);
        unimplemented!()
    }
}

pub struct WriteBuilder<S> {
    s: Arc<S>,

    pub path: String,
    pub size: u64,
}

impl<S> WriteBuilder<S> {
    pub fn new(s: Arc<S>, path: &str, size: u64) -> Self {
        Self {
            s,
            path: path.to_string(),
            size,
        }
    }
}

impl<S: Write<S>> WriteBuilder<S> {
    pub async fn run(&mut self, r: Reader) -> Result<usize> {
        self.s.write(r, self).await
    }
}
