//  Copyright 2021 Datafuse Labs.
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
//

use std::sync::Arc;

use common_exception::Result;
use common_store_api::MetaApi;

use crate::store_client_conf::StoreClientConf;
use crate::KVApi;
use crate::StorageApi;
use crate::StoreClient;

#[derive(Clone)]
pub struct StoreApiProvider {
    conf: StoreClientConf,
}

impl StoreApiProvider {
    pub fn new(conf: impl Into<StoreClientConf>) -> Self {
        StoreApiProvider { conf: conf.into() }
    }

    pub async fn try_get_meta_client(&self) -> Result<Arc<dyn MetaApi>> {
        let client = StoreClient::try_new(&self.conf).await?;
        Ok(Arc::new(client))
    }

    pub async fn try_get_kv_client(&self) -> Result<Arc<dyn KVApi>> {
        let client = StoreClient::try_new(&self.conf).await?;
        Ok(Arc::new(client))
    }

    pub async fn try_get_storage_client(&self) -> Result<Arc<dyn StorageApi>> {
        let client = StoreClient::try_new(&self.conf).await?;
        Ok(Arc::new(client))
    }
}
