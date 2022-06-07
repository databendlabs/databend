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

use chrono_tz::Tz;
use common_base::base::Progress;
use common_base::base::Runtime;
use common_base::infallible::Mutex;
use common_base::infallible::RwLock;
use common_contexts::DalContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use common_meta_types::UserInfo;
use common_planners::PlanNode;
use futures::future::AbortHandle;
use uuid::Uuid;

use crate::catalogs::CatalogManager;
use crate::clusters::Cluster;
use crate::servers::http::v1::HttpQueryHandle;
use crate::sessions::Session;
use crate::sessions::Settings;
use crate::sql::SQLCommon;
use crate::storages::Table;
use crate::users::auth::auth_mgr::AuthMgr;
use crate::users::RoleCacheMgr;
use crate::users::UserApiProvider;
use crate::Config;

type DatabaseAndTable = (String, String, String);

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
    /// scan_progress for scan metrics of datablocks (uncompressed)
    pub(in crate::sessions) scan_progress: Arc<Progress>,
    /// write_progress for write/commit metrics of datablocks (uncompressed)
    pub(in crate::sessions) write_progress: Arc<Progress>,
    /// result_progress for metrics of result datablocks (uncompressed)
    pub(in crate::sessions) result_progress: Arc<Progress>,
    pub(in crate::sessions) error: Arc<Mutex<Option<ErrorCode>>>,
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
    pub(in crate::sessions) user_manager: Arc<UserApiProvider>,
    pub(in crate::sessions) auth_manager: Arc<AuthMgr>,
}

impl QueryContextShared {
    pub async fn try_create(
        session: Arc<Session>,
        cluster_cache: Arc<Cluster>,
    ) -> Result<Arc<QueryContextShared>> {
        let conf = session.get_config();

        let user_manager = session.session_mgr.get_user_api_provider();

        Ok(Arc::new(QueryContextShared {
            session,
            cluster_cache,
            init_query_id: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            scan_progress: Arc::new(Progress::create()),
            result_progress: Arc::new(Progress::create()),
            write_progress: Arc::new(Progress::create()),
            error: Arc::new(Mutex::new(None)),
            runtime: Arc::new(RwLock::new(None)),
            sources_abort_handle: Arc::new(RwLock::new(Vec::new())),
            ref_count: Arc::new(AtomicUsize::new(0)),
            subquery_index: Arc::new(AtomicUsize::new(1)),
            running_query: Arc::new(RwLock::new(None)),
            http_query: Arc::new(RwLock::new(None)),
            running_plan: Arc::new(RwLock::new(None)),
            tables_refs: Arc::new(Mutex::new(HashMap::new())),
            dal_ctx: Arc::new(Default::default()),
            user_manager: user_manager.clone(),
            auth_manager: Arc::new(AuthMgr::create(conf, user_manager.clone()).await?),
        }))
    }

    pub fn set_error(&self, err: ErrorCode) {
        let mut guard = self.error.lock();
        *guard = Some(err);
    }

    pub fn kill(&self) {
        self.set_error(ErrorCode::AbortedQuery(
            "Aborted query, because the server is shutting down or the query was killed",
        ));

        let mut sources_abort_handle = self.sources_abort_handle.write();

        while let Some(source_abort_handle) = sources_abort_handle.pop() {
            source_abort_handle.abort();
        }

        // TODO: Wait for the query to be processed (write out the last error)
    }

    pub fn get_cluster(&self) -> Arc<Cluster> {
        self.cluster_cache.clone()
    }

    pub fn get_current_catalog(&self) -> String {
        self.session.get_current_catalog()
    }

    pub fn get_current_database(&self) -> String {
        self.session.get_current_database()
    }

    pub fn set_current_database(&self, new_database_name: String) {
        self.session.set_current_database(new_database_name);
    }

    pub fn get_current_user(&self) -> Result<UserInfo> {
        self.session.get_current_user()
    }

    pub fn set_current_tenant(&self, tenant: String) {
        self.session.set_current_tenant(tenant);
    }

    pub fn get_tenant(&self) -> String {
        self.session.get_current_tenant()
    }

    pub fn get_user_manager(&self) -> Arc<UserApiProvider> {
        self.user_manager.clone()
    }

    pub fn get_auth_manager(&self) -> Arc<AuthMgr> {
        self.auth_manager.clone()
    }

    pub fn get_role_cache_manager(&self) -> Arc<RoleCacheMgr> {
        self.session.get_role_cache_manager()
    }

    pub fn get_settings(&self) -> Arc<Settings> {
        self.session.get_settings()
    }

    pub fn get_changed_settings(&self) -> Arc<Settings> {
        self.session.get_changed_settings()
    }

    pub fn apply_changed_settings(&self, changed_settings: Arc<Settings>) -> Result<()> {
        self.session.apply_changed_settings(changed_settings)
    }

    pub fn get_catalogs(&self) -> Arc<CatalogManager> {
        self.session.get_catalogs()
    }

    pub async fn get_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        // Always get same table metadata in the same query

        let table_meta_key = (catalog.to_string(), database.to_string(), table.to_string());

        let already_in_cache = { self.tables_refs.lock().contains_key(&table_meta_key) };
        match already_in_cache {
            false => self.get_table_to_cache(catalog, database, table).await,
            true => Ok(self
                .tables_refs
                .lock()
                .get(&table_meta_key)
                .ok_or_else(|| ErrorCode::LogicalError("Logical error, it's a bug."))?
                .clone()),
        }
    }

    async fn get_table_to_cache(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        let tenant = self.get_tenant();
        let table_meta_key = (catalog.to_string(), database.to_string(), table.to_string());
        let catalog = self.get_catalogs().get_catalog(catalog)?;
        let cache_table = catalog.get_table(tenant.as_str(), database, table).await?;

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
                let runtime = Arc::new(Runtime::with_worker_threads(
                    max_threads,
                    Some("query-ctx".to_string()),
                )?);
                *query_runtime = Some(runtime.clone());
                Ok(runtime)
            }
        }
    }

    pub fn attach_http_query_handle(&self, handle: HttpQueryHandle) {
        let mut http_query = self.http_query.write();
        *http_query = Some(handle);
    }
    pub fn get_http_query(&self) -> Option<HttpQueryHandle> {
        self.http_query.read().clone()
    }

    pub fn attach_query_str(&self, query: &str) {
        let mut running_query = self.running_query.write();
        *running_query = Some(SQLCommon::short_sql(query));
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

    pub fn get_config(&self) -> Config {
        self.session.get_config()
    }

    pub fn get_format_settings(&self) -> Result<FormatSettings> {
        let settings = self.get_settings();
        let mut format = FormatSettings::default();
        {
            format.record_delimiter = settings.get_record_delimiter()?;
            format.field_delimiter = settings.get_field_delimiter()?;
            format.empty_as_default = settings.get_empty_as_default()? > 0;
            format.skip_header = settings.get_skip_header()? > 0;

            let tz = String::from_utf8(settings.get_timezone()?).map_err(|_| {
                ErrorCode::LogicalError("Timezone has been checked and should be valid.")
            })?;
            format.timezone = tz.parse::<Tz>().map_err(|_| {
                ErrorCode::InvalidTimezone("Timezone has been checked and should be valid")
            })?;

            let compress = String::from_utf8(settings.get_compression()?).map_err(|_| {
                ErrorCode::UnknownCompressionType("Compress type must be valid utf-8")
            })?;
            format.compression = compress.parse()?
        }
        Ok(format)
    }

    pub fn get_connection_id(&self) -> String {
        self.session.get_id()
    }

    pub async fn reload_config(&self) -> Result<()> {
        self.session.session_mgr.reload_config().await
    }
}

impl Session {
    pub(in crate::sessions) fn destroy_context_shared(&self) {
        self.session_ctx.take_query_context_shared();
    }
}
