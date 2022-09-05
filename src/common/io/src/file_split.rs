// Copyright 2022 Datafuse Labs.
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

use std::borrow::Cow;

#[derive(Debug)]
pub struct FileSplitCow<'a> {
    pub path: Option<String>,
    pub start_offset: usize,
    pub start_row: usize,
    pub buf: Cow<'a, [u8]>,
}

#[derive(Debug)]
pub struct FileSplit {
    pub path: Option<String>,
    pub start_offset: usize,
    pub start_row: usize,
    pub buf: Vec<u8>,
}

impl FileSplit {
    pub fn to_cow(self) -> FileSplitCow<'static> {
        FileSplitCow {
            path: self.path,
            start_offset: self.start_offset,
            start_row: self.start_row,
            buf: Cow::from(self.buf),
        }
    }

    pub fn from_cow(data: FileSplitCow<'_>) -> Self {
        Self {
            path: data.path,
            start_offset: data.start_offset,
            start_row: data.start_row,
            buf: data.buf.into_owned(),
        }
    }
}
