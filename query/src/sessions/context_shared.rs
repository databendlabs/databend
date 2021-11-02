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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use common_base::BlockingWait;
use common_base::Progress;
use common_base::Runtime;
use common_exception::Result;
use common_infallible::Mutex;
use common_infallible::RwLock;
use common_planners::PlanNode;
use futures::future::AbortHandle;
use uuid::Uuid;

use crate::catalogs::impls::DatabaseCatalog;
use crate::catalogs::Catalog;
use crate::catalogs::Table;
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::sessions::Session;
use crate::sessions::Settings;

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
pub struct DatabendQueryContextShared {
    pub(in crate::sessions) conf: Config,
    pub(in crate::sessions) progress: Arc<Progress>,
    pub(in crate::sessions) session: Arc<Session>,
    pub(in crate::sessions) runtime: Arc<RwLock<Option<Arc<Runtime>>>>,
    pub(in crate::sessions) init_query_id: Arc<RwLock<String>>,
    pub(in crate::sessions) cluster_cache: ClusterRef,
    pub(in crate::sessions) sources_abort_handle: Arc<RwLock<Vec<AbortHandle>>>,
    pub(in crate::sessions) ref_count: Arc<AtomicUsize>,
    pub(in crate::sessions) subquery_index: Arc<AtomicUsize>,
    pub(in crate::sessions) running_query: Arc<RwLock<Option<String>>>,
    pub(in crate::sessions) running_plan: Arc<RwLock<Option<PlanNode>>>,
    pub(in crate::sessions) tables_refs: Arc<Mutex<HashMap<DatabaseAndTable, Arc<dyn Table>>>>,
}

impl DatabendQueryContextShared {
    pub fn try_create(
        conf: Config,
        session: Arc<Session>,
        cluster_cache: ClusterRef,
    ) -> Arc<DatabendQueryContextShared> {
        Arc::new(DatabendQueryContextShared {
            conf,
            init_query_id: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            progress: Arc::new(Progress::create()),
            session,
            cluster_cache,
            runtime: Arc::new(RwLock::new(None)),
            sources_abort_handle: Arc::new(RwLock::new(Vec::new())),
            ref_count: Arc::new(AtomicUsize::new(0)),
            subquery_index: Arc::new(AtomicUsize::new(1)),
            running_query: Arc::new(RwLock::new(None)),
            running_plan: Arc::new(RwLock::new(None)),
            tables_refs: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn kill(&self) {
        let mut sources_abort_handle = self.sources_abort_handle.write();

        while let Some(source_abort_handle) = sources_abort_handle.pop() {
            source_abort_handle.abort();
        }

        // TODO: Wait for the query to be processed (write out the last error)
    }

    pub fn get_cluster(&self) -> ClusterRef {
        self.cluster_cache.clone()
    }

    pub fn get_current_database(&self) -> String {
        self.session.get_current_database()
    }

    pub fn set_current_database(&self, new_database_name: String) {
        self.session.set_current_database(new_database_name);
    }

    pub fn get_settings(&self) -> Arc<Settings> {
        self.session.get_settings()
    }

    pub fn get_catalog(&self) -> Arc<DatabaseCatalog> {
        self.session.get_catalog()
    }

    pub fn get_table(&self, database: &str, table: &str) -> Result<Arc<dyn Table>> {
        // Always get same table metadata in the same query
        let table_meta_key = (database.to_string(), table.to_string());

        let mut tables_refs = self.tables_refs.lock();

        let ent = match tables_refs.entry(table_meta_key) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let catalog = self.get_catalog();
                let rt = self.try_get_runtime()?;
                let database = database.to_string();
                let table = table.to_string();
                let t = (async move { catalog.get_table(&database, &table).await })
                    .wait_in(&rt, Some(Duration::from_millis(5000)))??;

                entry.insert(t).clone()
            }
        };

        Ok(ent)
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

    pub fn attach_query_str(&self, query: &str) {
        let mut running_query = self.running_query.write();
        *running_query = Some(query.to_string());
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
        let mut mutable_state = self.mutable_state.lock();
        mutable_state.context_shared.take();
    }
}
