// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_flights::StoreClient;

use crate::configs::Config;
use crate::datasources::remote::store_client_provider::StoreClientProvider;
use crate::datasources::remote::store_client_provider::TryGetStoreClient;
use crate::datasources::remote::RemoteDatabase;
use crate::datasources::Database;

pub struct RemoteFactory {
    store_client_provider: StoreClientProvider,
}

impl RemoteFactory {
    pub fn new(conf: &Config) -> Self {
        RemoteFactory {
            store_client_provider: Arc::new(ClientProvider::new(conf)),
        }
    }

    pub fn load_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        // Load databases from remote.
        let databases: Vec<Arc<dyn Database>> = vec![Arc::new(RemoteDatabase::create(
            self.store_client_provider.clone(),
            "for_test".to_string(),
        ))];
        Ok(databases)
    }

    pub fn store_client_provider(&self) -> StoreClientProvider {
        self.store_client_provider.clone()
    }
}
struct ClientProvider {
    conf: Config,
}

impl ClientProvider {
    pub fn new(conf: &Config) -> Self {
        ClientProvider { conf: conf.clone() }
    }
}

#[async_trait::async_trait]
impl TryGetStoreClient for ClientProvider {
    async fn try_get_client(&self) -> Result<StoreClient> {
        let client = StoreClient::try_create(
            &self.conf.store_api_address,
            &self.conf.store_api_username.as_ref(),
            &self.conf.store_api_password.as_ref(),
        )
        .await
        .map_err(ErrorCode::from)?;
        Ok(client)
    }
}
