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

use common_exception::ErrorCode;
use common_exception::Result;
use common_flights::StoreClient;

use crate::configs::Config;
use crate::datasources::remote::StoreClientProvider;
use crate::datasources::remote::TryGetStoreClient;

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
            &self.conf.store.store_address,
            self.conf.store.store_username.as_ref(),
            self.conf.store.store_password.as_ref(),
            tls_conf,
        )
        .await
        .map_err(ErrorCode::from)?;
        Ok(client)
    }
}
