// Copyright 2021 Datafuse Labs
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

use super::*;

#[async_trait::async_trait]
impl TableContext for QueryContext {
    fn broadcast_registry(&self) -> &BroadcastRegistry {
        &self.shared.broadcast_registry
    }

    fn copy_state(&self) -> &CopyState {
        &self.shared.copy_state
    }

    fn fragment_id(&self) -> &FragmentId {
        &self.fragment_id
    }

    fn mutation_state(&self) -> &MutationState {
        &self.shared.mutation_state
    }

    fn read_block_thresholds(&self) -> &ReadBlockThresholdsState {
        &self.read_block_thresholds
    }

    fn result_cache_state(&self) -> &ResultCacheState {
        &self.shared.result_cache_state
    }

    fn written_segment_locations(&self) -> &SegmentLocationsState {
        &self.written_segment_locs
    }

    fn selected_segment_locations(&self) -> &SegmentLocationsState {
        &self.shared.selected_segment_locs
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait::async_trait]
impl TableContextAuthorization for QueryContext {
    fn get_current_user(&self) -> Result<UserInfo> {
        self.shared.get_current_user()
    }

    fn get_current_role(&self) -> Option<RoleInfo> {
        self.shared.get_current_role()
    }

    fn get_secondary_roles(&self) -> Option<Vec<String>> {
        self.shared.get_secondary_roles()
    }

    async fn get_all_available_roles(&self) -> Result<Vec<RoleInfo>> {
        self.get_current_session().get_all_available_roles().await
    }

    async fn get_all_effective_roles(&self) -> Result<Vec<RoleInfo>> {
        self.get_current_session().get_all_effective_roles().await
    }

    async fn validate_privilege(
        &self,
        object: &GrantObject,
        privilege: UserPrivilegeType,
        check_current_role_only: bool,
    ) -> Result<()> {
        self.get_current_session()
            .validate_privilege(object, privilege, check_current_role_only)
            .await
    }

    async fn get_visibility_checker(
        &self,
        ignore_ownership: bool,
        object: Object,
    ) -> Result<GrantObjectVisibilityChecker> {
        self.shared
            .session
            .get_visibility_checker(ignore_ownership, object)
            .await
    }

    async fn get_db_table_grant_checker(&self) -> Result<GrantObjectVisibilityChecker> {
        self.shared.session.get_db_table_grant_checker().await
    }
}

#[async_trait::async_trait]
impl TableContextCluster for QueryContext {
    fn get_cluster(&self) -> Arc<Cluster> {
        self.shared.get_cluster()
    }

    fn set_cluster(&self, cluster: Arc<Cluster>) {
        self.shared.set_cluster(cluster)
    }

    async fn get_warehouse_cluster(&self) -> Result<Arc<Cluster>> {
        self.shared.get_warehouse_clusters().await
    }
}

impl TableContextQueryInfo for QueryContext {
    fn get_fuse_version(&self) -> String {
        let session = self.get_current_session();
        match session.get_type() {
            SessionType::MySQL => self.mysql_version.clone(),
            _ => self.version.clone(),
        }
    }

    fn get_version(&self) -> BuildInfoRef {
        self.shared.version
    }

    fn get_input_format_settings(&self) -> Result<InputFormatSettings> {
        self.get_settings().get_input_format_settings()
    }

    fn get_output_format_settings(&self) -> Result<OutputFormatSettings> {
        self.get_settings().get_output_format_settings()
    }

    fn get_query_kind(&self) -> QueryKind {
        self.shared.get_query_kind()
    }
}

impl TableContextProgress for QueryContext {
    fn incr_total_scan_value(&self, value: ProgressValues) {
        self.shared.total_scan_values.as_ref().incr(&value);
    }

    fn get_total_scan_value(&self) -> ProgressValues {
        self.shared.total_scan_values.as_ref().get_values()
    }

    fn get_scan_progress(&self) -> Arc<Progress> {
        self.shared.scan_progress.clone()
    }

    fn get_scan_progress_value(&self) -> ProgressValues {
        self.shared.scan_progress.as_ref().get_values()
    }

    fn get_write_progress(&self) -> Arc<Progress> {
        self.shared.write_progress.clone()
    }

    fn get_write_progress_value(&self) -> ProgressValues {
        self.shared.write_progress.as_ref().get_values()
    }

    fn get_result_progress(&self) -> Arc<Progress> {
        self.shared.result_progress.clone()
    }

    fn get_result_progress_value(&self) -> ProgressValues {
        self.shared.result_progress.as_ref().get_values()
    }
}

impl TableContextTelemetry for QueryContext {
    fn get_processes_info(&self) -> Vec<ProcessInfo> {
        SessionManager::instance().processes_info()
    }

    fn get_queued_queries(&self) -> Vec<ProcessInfo> {
        let queries = QueriesQueueManager::instance()
            .list()
            .iter()
            .map(|x| x.query_id.clone())
            .collect::<HashSet<_>>();

        SessionManager::instance()
            .processes_info()
            .into_iter()
            .filter(|x| match &x.current_query_id {
                None => false,
                Some(query_id) => queries.contains(query_id),
            })
            .collect::<Vec<_>>()
    }

    fn get_status_info(&self) -> String {
        let status = self.shared.status.read();
        status.clone()
    }

    fn set_status_info(&self, info: &str) {
        info!("Status update: {}", info);
        let mut status = self.shared.status.write();
        *status = info.to_string();
    }

    fn get_data_cache_metrics(&self) -> &DataCacheMetrics {
        self.shared.get_query_cache_metrics()
    }

    fn get_query_queued_duration(&self) -> std::time::Duration {
        *self.shared.query_queued_duration.read()
    }

    fn set_query_queued_duration(&self, queued_duration: std::time::Duration) {
        *self.shared.query_queued_duration.write() = queued_duration;
    }
}

impl TableContextQueryState for QueryContext {
    fn check_aborting(&self) -> Result<(), ContextError> {
        self.shared.check_aborting()
    }

    fn get_abort_notify(&self) -> Arc<WatchNotify> {
        self.shared.abort_notify.clone()
    }

    fn get_error(&self) -> Option<ErrorCode<ContextError>> {
        self.shared.get_error()
    }

    fn push_warning(&self, warn: String) {
        self.shared.push_warning(warn)
    }
}

impl TableContextSession for QueryContext {
    fn get_connection_id(&self) -> String {
        self.shared.get_connection_id()
    }

    fn get_current_session_id(&self) -> String {
        self.get_current_session().get_id()
    }

    fn get_current_client_session_id(&self) -> Option<String> {
        self.get_current_session().get_client_session_id()
    }

    fn txn_mgr(&self) -> TxnManagerRef {
        self.shared.session.session_ctx.txn_mgr()
    }

    fn session_state(&self) -> Result<SessionState> {
        self.shared.session.session_ctx.session_state()
    }

    fn get_session_type(&self) -> SessionType {
        self.shared.session.get_type()
    }
}

impl TableContextSettings for QueryContext {
    fn get_function_context(&self) -> Result<FunctionContext> {
        let settings = self.get_settings();

        let tz_string = settings.get_timezone()?;
        let tz = TimeZone::get(&tz_string).map_err(|e| {
            ErrorCode::InvalidTimezone(format!("Timezone validation failed: {}", e))
        })?;
        let now = Zoned::now().with_time_zone(TimeZone::UTC);
        let numeric_cast_option = settings.get_numeric_cast_option()?;
        let rounding_mode = numeric_cast_option.as_str() == "rounding";
        let disable_variant_check = settings.get_disable_variant_check()?;
        let geometry_output_format = settings.get_geometry_output_format()?;
        let binary_input_format = settings.get_binary_input_format()?;
        let binary_output_format = settings.get_binary_output_format()?;
        let parse_datetime_ignore_remainder = settings.get_parse_datetime_ignore_remainder()?;
        let enable_strict_datetime_parser = settings.get_enable_strict_datetime_parser()?;
        let enable_auto_detect_datetime_format =
            settings.get_enable_auto_detect_datetime_format()?;
        let week_start = settings.get_week_start()? as u8;
        let date_format_style = settings.get_date_format_style()?;
        let random_function_seed = settings.get_random_function_seed()?;

        Ok(FunctionContext {
            now,
            tz,
            rounding_mode,
            disable_variant_check,
            enable_selector_executor: settings.get_enable_selector_executor()?,
            geometry_output_format,
            binary_input_format,
            binary_output_format,
            parse_datetime_ignore_remainder,
            enable_strict_datetime_parser,
            enable_auto_detect_datetime_format,
            random_function_seed,
            week_start,
            date_format_style,
        })
    }

    fn get_settings(&self) -> Arc<Settings> {
        if self.shared.query_settings.is_changed()
            && self.shared.query_settings.query_level_change()
        {
            let shared_settings = self.shared.query_settings.changes();
            if self.get_session_settings().is_changed() {
                for r in self.get_session_settings().changes().iter() {
                    if !self.shared.query_settings.changes().contains_key(r.key()) {
                        shared_settings.insert(r.key().clone(), r.value().clone());
                    }
                }
                unsafe {
                    self.query_settings.unchecked_apply_changes(shared_settings);
                }
            } else {
                unsafe {
                    self.query_settings.unchecked_apply_changes(shared_settings);
                }
            }
        } else {
            unsafe {
                self.query_settings
                    .unchecked_apply_changes(self.get_session_settings().changes())
            }
        }

        self.query_settings.clone()
    }

    fn get_session_settings(&self) -> Arc<Settings> {
        self.shared.get_settings()
    }

    fn get_shared_settings(&self) -> Arc<Settings> {
        self.shared.query_settings.clone()
    }
}

impl TableContextLicense for QueryContext {
    fn get_license_key(&self) -> String {
        self.get_settings()
            .get_enterprise_license(self.get_version())
    }
}
