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

use crate::ops::OpDelete;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::Layer;

#[derive(Clone)]
pub struct Operator {
    accessor: Arc<dyn Accessor>,
}

impl Operator {
    pub fn new(accessor: Arc<dyn Accessor>) -> Self {
        Self { accessor }
    }

    #[must_use]
    pub fn layer(self, layer: impl Layer) -> Self {
        Operator {
            accessor: layer.layer(self.accessor.clone()),
        }
    }

    pub fn inner(&self) -> Arc<dyn Accessor> {
        self.accessor.clone()
    }

    pub fn read(&self, path: &str) -> OpRead {
        OpRead::new(self.clone(), path)
    }

    pub fn write(&self, path: &str, size: u64) -> OpWrite {
        OpWrite::new(self.clone(), path, size)
    }

    pub fn stat(&self, path: &str) -> OpStat {
        OpStat::new(self.clone(), path)
    }

    pub fn delete(&self, path: &str) -> OpDelete {
        OpDelete::new(self.clone(), path)
    }
}
