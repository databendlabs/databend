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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use common_base::Progress;
use common_base::Runtime;
use common_dal_context::DalContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;
use common_infallible::RwLock;
use common_meta_types::UserInfo;
use common_planners::PlanNode;
use futures::future::AbortHandle;
use uuid::Uuid;

use crate::catalogs::Catalog;
use crate::catalogs::DatabaseCatalog;
use crate::clusters::Cluster;
use crate::configs::Config;
use crate::servers::http::v1::HttpQueryHandle;
use crate::sessions::Session;
use crate::sessions::Settings;
use crate::storages::Table;

type DatabaseAndTable = (String, String);

/// Data that needs to be shared in a query context.
/// This is very useful, for example, for queries:
///     USE database_1;
///     SELECT
///         (SELECT scalar FROM table_name_1) AS scalar_1,
///         (SELECT scalar FROM table_name_2) AS scalar_2,
///         (SELECT scalar FROM table_name_3) AS scalar_3
///     FROM table_name_4;
/// For each subquery, they will share a runtime, session, progress, init_query_id
pub struct QueryContextShared {
    pub conf: Config,
    pub(in crate::sessions) scan_progress: Arc<Progress>,
    pub(in crate::sessions) result_progress: Arc<Progress>,
    pub(in crate::sessions) session: Arc<Session>,
    pub(in crate::sessions) runtime: Arc<RwLock<Option<Arc<Runtime>>>>,
    pub(in crate::sessions) init_query_id: Arc<RwLock<String>>,
    pub(in crate::sessions) cluster_cache: Arc<Cluster>,
    pub(in crate::sessions) sources_abort_handle: Arc<RwLock<Vec<AbortHandle>>>,
    pub(in crate::sessions) ref_count: Arc<AtomicUsize>,
    pub(in crate::sessions) subquery_index: Arc<AtomicUsize>,
    pub(in crate::sessions) running_query: Arc<RwLock<Option<String>>>,
    pub(in crate::sessions) http_query: Arc<RwLock<Option<HttpQueryHandle>>>,
    pub(in crate::sessions) running_plan: Arc<RwLock<Option<PlanNode>>>,
    pub(in crate::sessions) tables_refs: Arc<Mutex<HashMap<DatabaseAndTable, Arc<dyn Table>>>>,
    pub(in crate::sessions) dal_ctx: Arc<DalContext>,
}

impl QueryContextShared {
    pub fn try_create(
        conf: Config,
        session: Arc<Session>,
        cluster_cache: Arc<Cluster>,
    ) -> Result<Arc<QueryContextShared>> {
        Ok(Arc::new(QueryContextShared {
            conf,
            session,
            cluster_cache,
            init_query_id: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            scan_progress: Arc::new(Progress::create()),
            result_progress: Arc::new(Progress::create()),
            runtime: Arc::new(RwLock::new(None)),
            sources_abort_handle: Arc::new(RwLock::new(Vec::new())),
            ref_count: Arc::new(AtomicUsize::new(0)),
            subquery_index: Arc::new(AtomicUsize::new(1)),
            running_query: Arc::new(RwLock::new(None)),
            http_query: Arc::new(RwLock::new(None)),
            running_plan: Arc::new(RwLock::new(None)),
            tables_refs: Arc::new(Mutex::new(HashMap::new())),
            dal_ctx: Arc::new(Default::default()),
        }))
    }

    pub fn kill(&self) {
        let mut sources_abort_handle = self.sources_abort_handle.write();

        while let Some(source_abort_handle) = sources_abort_handle.pop() {
            source_abort_handle.abort();
        }

        let http_query = self.http_query.read();
        if let Some(handle) = &*http_query {
            handle.abort();
        }

        // TODO: Wait for the query to be processed (write out the last error)
    }

    pub fn get_cluster(&self) -> Arc<Cluster> {
        self.cluster_cache.clone()
    }

    pub fn get_current_database(&self) -> String {
        self.session.get_current_database()
    }

    pub fn set_current_database(&self, new_database_name: String) {
        self.session.set_current_database(new_database_name);
    }

    pub fn set_current_tenant(&self, tenant: String) {
        self.session.set_current_tenant(tenant);
    }

    pub fn get_current_user(&self) -> Result<UserInfo> {
        self.session.get_current_user()
    }

    pub fn get_tenant(&self) -> String {
        self.session.get_current_tenant()
    }

    pub fn get_settings(&self) -> Arc<Settings> {
        self.session.get_settings()
    }

    pub fn get_catalog(&self) -> Arc<DatabaseCatalog> {
        self.session.get_catalog()
    }

    pub async fn get_table(&self, database: &str, table: &str) -> Result<Arc<dyn Table>> {
        // Always get same table metadata in the same query
        let table_meta_key = (database.to_string(), table.to_string());

        let already_in_cache = { self.tables_refs.lock().contains_key(&table_meta_key) };
        match already_in_cache {
            false => self.get_table_to_cache(database, table).await,
            true => Ok(self
                .tables_refs
                .lock()
                .get(&table_meta_key)
                .ok_or_else(|| ErrorCode::LogicalError("Logical error, it's a bug."))?
                .clone()),
        }
    }

    async fn get_table_to_cache(&self, database: &str, table: &str) -> Result<Arc<dyn Table>> {
        let tenant = self.get_tenant();
        let catalog = self.get_catalog();
        let cache_table = catalog.get_table(tenant.as_str(), database, table).await?;

        let table_meta_key = (database.to_string(), table.to_string());
        let mut tables_refs = self.tables_refs.lock();

        match tables_refs.entry(table_meta_key) {
            Entry::Occupied(v) => Ok(v.get().clone()),
            Entry::Vacant(v) => Ok(v.insert(cache_table).clone()),
        }
    }

    /// Init runtime when first get
    pub fn try_get_runtime(&self) -> Result<Arc<Runtime>> {
        let mut query_runtime = self.runtime.write();

        match &*query_runtime {
            Some(query_runtime) => Ok(query_runtime.clone()),
            None => {
                let settings = self.get_settings();
                let max_threads = settings.get_max_threads()? as usize;
                let runtime = Arc::new(Runtime::with_worker_threads(max_threads)?);
                *query_runtime = Some(runtime.clone());
                Ok(runtime)
            }
        }
    }

    pub fn attach_http_query_handle(&self, handle: HttpQueryHandle) {
        let mut http_query = self.http_query.write();
        *http_query = Some(handle);
    }

    pub fn attach_query_str(&self, query: &str) {
        let mut running_query = self.running_query.write();
        *running_query = Some(query.to_string());
    }

    pub fn get_query_str(&self) -> String {
        let running_query = self.running_query.read();
        running_query.as_ref().unwrap_or(&"".to_string()).clone()
    }

    pub fn attach_query_plan(&self, plan: &PlanNode) {
        let mut running_plan = self.running_plan.write();
        *running_plan = Some(plan.clone());
    }

    pub fn add_source_abort_handle(&self, handle: AbortHandle) {
        let mut sources_abort_handle = self.sources_abort_handle.write();
        sources_abort_handle.push(handle);
    }
}

impl Session {
    pub(in crate::sessions) fn destroy_context_shared(&self) {
        self.session_ctx.take_query_context_shared();
    }
}
