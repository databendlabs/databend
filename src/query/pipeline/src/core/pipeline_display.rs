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

use std::fmt::Display;
use std::fmt::Formatter;

use petgraph::dot::Dot;

use crate::core::Pipeline;

impl Pipeline {
    pub fn display_indent(&self) -> impl std::fmt::Display + '_ {
        PipelineIndentDisplayWrapper { pipeline: self }
    }
}

struct PipelineIndentDisplayWrapper<'a> {
    pipeline: &'a Pipeline,
}

impl Display for PipelineIndentDisplayWrapper<'_> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:?}",
            Dot::with_attr_getters(
                &self.pipeline.graph,
                &[],
                &|_, _| String::new(),
                &|_, (_, _)| String::new(),
            )
        )
    }
}
