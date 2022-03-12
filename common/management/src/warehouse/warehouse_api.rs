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

use common_exception::Result;
use common_meta_types::SeqV;
use common_meta_types::WarehouseInfo;

#[async_trait::async_trait]
pub trait WarehouseApi: Sync + Send {
    async fn create_warehouse(&self, warehouse_info: WarehouseInfo) -> Result<u64>;

    async fn get_warehouse(&self, name: &str, seq: Option<u64>) -> Result<SeqV<WarehouseInfo>>;

    async fn get_warehouses(&self) -> Result<Vec<SeqV<WarehouseInfo>>>;

    async fn update_warehouse_size(&self, name: &str, size: &str, seq: Option<u64>) -> Result<u64>;

    async fn drop_warehouse(&self, name: &str, seq: Option<u64>) -> Result<()>;
}
