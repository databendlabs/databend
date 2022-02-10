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

use crate::error::Result;
use crate::Operator;
use crate::Reader;

pub struct OpRead {
    op: Operator,

    pub path: String,
    pub offset: Option<u64>,
    pub size: Option<u64>,
}

impl OpRead {
    pub fn new(op: Operator, path: &str) -> Self {
        Self {
            op,
            path: path.to_string(),
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

    pub async fn run(&mut self) -> Result<Reader> {
        self.op.inner().read(self).await
    }
}
