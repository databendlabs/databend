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

pub struct RemoteFactory {
    store_client_provider: StoreClientProvider,
}

impl RemoteFactory {
    pub fn new(conf: &Config) -> Self {
        RemoteFactory {
            store_client_provider: Arc::new(ClientProvider::new(conf)),
        }
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
        let tls_conf = if self.conf.tls_store_cli_enabled() {
            Some(self.conf.tls_store_client_conf())
        } else {
            None
        };

        let client = StoreClient::with_tls_conf(
            &self.conf.store_api_address,
            self.conf.store_api_username.as_ref(),
            self.conf.store_api_password.as_ref(),
            tls_conf,
        )
        .await
        .map_err(ErrorCode::from)?;
        Ok(client)
    }
}
