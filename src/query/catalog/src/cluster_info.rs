//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_meta_types::NodeInfo;

pub struct Cluster {
    pub local_id: String,
    pub nodes: Vec<Arc<NodeInfo>>,
}

impl Cluster {
    /// If this cluster is empty?
    ///
    /// # TODO
    ///
    /// From @Xuanwo
    ///
    /// Ideally, we should implement a cluster trait to replace `ClusterHelper`
    /// defined in `databend-query`.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}
