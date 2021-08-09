// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
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
