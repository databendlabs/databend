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

mod indent_format;
mod pretty_format;

use std::fmt::Display;

#[derive(Clone)]
pub struct FormatTreeNode<T: Display + Clone = String> {
    pub payload: T,
    pub children: Vec<Self>,
}

impl<T> FormatTreeNode<T>
where T: Display + Clone
{
    pub fn new(payload: T) -> Self {
        Self {
            payload,
            children: vec![],
        }
    }

    pub fn with_children(payload: T, children: Vec<Self>) -> Self {
        Self { payload, children }
    }
}
