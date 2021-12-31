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

/// `Read` will read the data from the underlying storage.
#[async_trait]
pub trait Read<S: Send + Sync>: Send + Sync {
    async fn read(&self, args: &ReadBuilder<S>) -> Result<Reader> {
        let _ = args;
        unimplemented!()
    }
}

pub struct ReadBuilder<'p, S> {
    s: Arc<S>,

    pub path: &'p str,
    pub offset: Option<u64>,
    pub size: Option<u64>,
}

impl<'p, S> ReadBuilder<'p, S> {
    pub fn new(s: Arc<S>, path: &'p str) -> Self {
        Self {
            s,
            path,
            offset: None,
            size: None,
        }
    }

    pub fn offset(&mut self, offset: u64) -> &mut Self {
        self.offset = Some(offset);

        self
    }

    pub fn size(&mut self, size: u64) -> &mut Self {
        self.size = Some(size);

        self
    }
}

impl<'p, S: Read<S>> ReadBuilder<'p, S> {
    pub async fn run(&mut self) -> Result<Reader> {
        self.s.read(self).await
    }
}
