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

use crate::pipelines::processors::Processor;

#[derive(Clone)]
pub struct Pipe {
    processors: Vec<Arc<dyn Processor>>,
}

impl Pipe {
    pub fn create() -> Self {
        Pipe { processors: vec![] }
    }

    pub fn nums(&self) -> usize {
        self.processors.len()
    }

    pub fn name(&self) -> &str {
        self.processors[0].name()
    }

    pub fn processors(&self) -> Vec<Arc<dyn Processor>> {
        self.processors.clone()
    }

    pub fn processor_by_index(&self, index: usize) -> Arc<dyn Processor> {
        self.processors[index].clone()
    }

    /// The first processor of the pipe.
    pub fn first(&self) -> Arc<dyn Processor> {
        self.processors[0].clone()
    }

    pub fn add(&mut self, processor: Arc<dyn Processor>) {
        self.processors.push(processor);
    }
}
