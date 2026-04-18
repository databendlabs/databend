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

use std::sync::Arc;

use databend_common_exception::Result;

use crate::cluster_info::Cluster;

#[async_trait::async_trait]
pub trait TableContextCluster: Send + Sync {
    fn get_cluster(&self) -> Arc<Cluster>;

    fn set_cluster(&self, cluster: Arc<Cluster>);

    async fn get_warehouse_cluster(&self) -> Result<Arc<Cluster>>;
}
