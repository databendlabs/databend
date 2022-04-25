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

use common_exception::Result;
use common_meta_api::KVApi;
use common_planners::CreateDatabasePlan;
use common_protos::protobuf;

use crate::common::MetaClientProvider;
use crate::configs::Config;

#[derive(Clone)]
pub struct KvBackend {
    #[allow(unused)]
    client: Arc<dyn KVApi>,
}

impl KvBackend {
    pub async fn create(conf: Config) -> Result<Arc<KvBackend>> {
        let client = MetaClientProvider::new(conf.meta.to_grpc_client_config())
            .try_get_kv_client()
            .await?;

        Ok(Arc::new(KvBackend { client }))
    }

    pub async fn create_database(&self, tenant: &str, req: CreateDatabasePlan) -> Result<()> {
        // 1. Check database exists or not
        // 2. If not exists get a new database id
        // 3. Write:
        //   - write('__fd_databases/<tenant_id>/by_name/<db_name>', <db_id>)
        //   - write('__fd_databases/<tenant_id>/by_id/<db_id>', <db_meta>)

        let _pb = protobuf::Database {
            tenant: tenant.to_string(),
            id: 0,
            name: req.db_name,
            options: req.meta.options,
            engine: "".to_string(),
            engine_options: Default::default(),
            created_on: "".to_string(),
            updated_on: "".to_string(),
            comment: "".to_string(),
        };
        Ok(())
    }
}
