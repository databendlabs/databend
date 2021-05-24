// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::sync::Arc;

use common_exception::Result;
use common_flights::StoreClient;

#[async_trait::async_trait]
pub trait IStoreClientProvider {
    async fn try_get_client(&self) -> Result<StoreClient>;
}

pub type StoreClientProvider = Arc<dyn IStoreClientProvider + Send + Sync>;
