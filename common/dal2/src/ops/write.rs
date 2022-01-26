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

pub struct OpWrite {
    op: Operator,

    pub path: String,
    pub size: u64,
}

impl OpWrite {
    pub fn new(op: Operator, path: &str, size: u64) -> Self {
        Self {
            op,
            path: path.to_string(),
            size,
        }
    }

    pub async fn run(&mut self, r: Reader) -> Result<usize> {
        self.op.inner().write(r, self).await
    }
}
