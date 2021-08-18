// Copyright 2020 Datafuse Labs.
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
//

use std::sync::Arc;

use common_exception::Result;
use common_flights::MetaApi;
use common_flights::StorageApi;
use common_flights::StoreClient;

#[async_trait::async_trait]
pub trait TryGetStoreClient {
    async fn try_get_client(&self) -> Result<StoreClient>;
}

pub type StoreClientProvider = Arc<dyn TryGetStoreClient + Send + Sync>;

pub trait StoreApis: MetaApi + StorageApi + Send {}

impl StoreApis for StoreClient {}

#[async_trait::async_trait]
pub trait GetStoreApiClient<T>
where T: StoreApis
{
    async fn try_get_store_apis(&self) -> Result<T>;
}

#[async_trait::async_trait]
impl GetStoreApiClient<StoreClient> for StoreClientProvider {
    async fn try_get_store_apis(&self) -> Result<StoreClient> {
        self.try_get_client().await
    }
}

pub type StoreApisProvider<T> = Arc<dyn GetStoreApiClient<T> + Send + Sync>;
