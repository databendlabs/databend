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

use std::io;

#[derive(Debug, Clone, thiserror::Error)]
#[error("IncompleteStream: expect: {expect} items, got: {got} items{context}")]
pub struct IncompleteStream {
    pub expect: u64,
    pub got: u64,
    pub context: String,
}

impl IncompleteStream {
    pub fn new(expect: u64, got: u64) -> Self {
        Self {
            expect,
            got,
            context: "".to_string(),
        }
    }
    pub fn context(mut self, context: impl ToString) -> Self {
        self.context = context.to_string();
        self
    }
}

impl From<IncompleteStream> for io::Error {
    fn from(value: IncompleteStream) -> Self {
        io::Error::new(io::ErrorKind::UnexpectedEof, value)
    }
}
