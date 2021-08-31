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

use std::sync::Arc;

use common_exception::Result;
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;

use crate::catalogs::Database;
use crate::catalogs::DatabaseEngine;
use crate::configs::Config;
use crate::datasources::remote::MetaStoreClient;
use crate::datasources::remote::RemoteFactory;
use crate::datasources::remote::RemoteMetaBackend;

pub struct RemoteDatabases {
    meta_store_client: Arc<dyn RemoteMetaBackend>,
}

impl RemoteDatabases {
    pub fn create(conf: Config) -> Self {
        // TODO(bohu): meta URI check, local or fuse?
        let meta_store_client = Arc::new(MetaStoreClient::create(Arc::new(
            RemoteFactory::new(&conf).store_client_provider(),
        )));
        RemoteDatabases { meta_store_client }
    }
}

impl DatabaseEngine for RemoteDatabases {
    fn engine_name(&self) -> &str {
        "remote"
    }

    fn get_database(&self, db_name: &str) -> Result<Option<Arc<dyn Database>>> {
        self.meta_store_client.get_database(db_name)
    }

    fn exists_database(&self, db_name: &str) -> Result<bool> {
        self.meta_store_client.exists_database(db_name)
    }

    fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        self.meta_store_client.get_databases()
    }

    fn create_database(&self, plan: CreateDatabasePlan) -> Result<()> {
        self.meta_store_client.create_database(plan)
    }

    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        self.meta_store_client.drop_database(plan)
    }
}
