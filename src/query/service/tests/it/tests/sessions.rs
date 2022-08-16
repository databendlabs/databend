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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use once_cell::sync::OnceCell;

use common_base::base::tokio::runtime::Runtime;
use common_base::base::{GlobalIORuntime, Thread};
use common_catalog::catalog::CatalogManager;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_storage::StorageOperator;
use common_tracing::QueryLogger;
use common_users::{RoleCacheManager, UserApiProvider};
use databend_query::api::DataExchangeManager;
use databend_query::catalogs::CatalogManagerHelper;
use databend_query::clusters::ClusterDiscovery;
use databend_query::sessions::SessionManager;
use databend_query::{Config, GlobalServices};
use databend_query::interpreters::AsyncInsertManager;
use databend_query::servers::http::v1::HttpQueryManager;

pub struct TestGlobalServices;

static INITIALIZED: AtomicUsize = AtomicUsize::new(0);

impl TestGlobalServices {
    pub async fn setup(config: Config) -> Result<TestGlobalServices> {
        if INITIALIZED.fetch_add(1, Ordering::Relaxed) != 0 {
            return Ok(TestGlobalServices);
        }

        GlobalServices::init(config.clone()).await?;
        ClusterDiscovery::instance().register_to_metastore(&config).await?;
        Ok(TestGlobalServices)
    }
}

impl Drop for TestGlobalServices {
    fn drop(&mut self) {
        if INITIALIZED.fetch_sub(1, Ordering::Relaxed) == 1 {
            GlobalServices::destroy();
        }
    }
}
