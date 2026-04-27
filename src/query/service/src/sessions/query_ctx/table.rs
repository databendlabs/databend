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

impl TableContextTableFactory for QueryContext {
    fn build_table_from_source_plan(&self, plan: &DataSourcePlan) -> Result<Arc<dyn Table>> {
        match &plan.source_info {
            DataSourceInfo::TableSource(table_info) => {
                self.build_table_by_table_info(table_info, plan.tbl_args.clone())
            }
            DataSourceInfo::StageSource(stage_info) => {
                self.build_external_by_table_info(stage_info, plan.tbl_args.clone())
            }
            DataSourceInfo::ParquetSource(table_info) => ParquetTable::from_info(table_info),
            DataSourceInfo::ResultScanSource(table_info) => ResultScan::from_info(table_info),
            DataSourceInfo::ORCSource(table_info) => OrcTable::from_info(table_info),
        }
    }
}

#[async_trait::async_trait]
impl TableContextTableAccess for QueryContext {
    #[async_backtrace::framed]
    async fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>> {
        self.shared
            .catalog_manager
            .get_catalog(
                self.get_tenant().tenant_name(),
                catalog_name.as_ref(),
                self.session_state()?,
            )
            .await
    }

    fn get_default_catalog(&self) -> Result<Arc<dyn Catalog>> {
        self.shared
            .catalog_manager
            .get_default_catalog(self.session_state()?)
    }

    fn get_current_catalog(&self) -> String {
        self.shared.get_current_catalog()
    }

    fn get_current_database(&self) -> String {
        self.shared.get_current_database()
    }

    fn get_tenant(&self) -> Tenant {
        self.shared.get_tenant()
    }

    fn get_application_level_data_operator(&self) -> Result<DataOperator> {
        Ok(self.shared.data_operator.clone())
    }

    async fn get_table_with_branch(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        branch: Option<&str>,
    ) -> Result<Arc<dyn Table>> {
        if database.eq_ignore_ascii_case("system_history")
            && ThreadTracker::capture_log_settings().is_none()
        {
            LicenseManagerSwitch::instance()
                .check_enterprise_enabled(self.get_license_key(), Feature::SystemHistory)?;

            if GlobalConfig::instance().log.history.is_invisible(table) {
                return Err(ErrorCode::InvalidArgument(format!(
                    "history table `{}` is configured as invisible",
                    table
                )));
            }
        }

        self.get_table_from_shared(catalog, database, table, branch, None)
            .await
    }

    #[async_backtrace::framed]
    async fn resolve_data_source(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        branch: Option<&str>,
        max_batch_size: Option<u64>,
    ) -> Result<Arc<dyn Table>> {
        let final_batch_size = match max_batch_size {
            Some(v) => Some(v),
            None => {
                if let Some(v) = self.get_settings().get_stream_consume_batch_size_hint()? {
                    info!("Overriding stream max_batch_size with setting value: {}", v);
                    Some(v)
                } else {
                    None
                }
            }
        };

        let table = self
            .get_table_from_shared(catalog, database, table, branch, final_batch_size)
            .await?;

        if table.is_stream() {
            let stream = StreamTable::try_from_table(table.as_ref())?;
            let actual_batch_limit = stream.max_batch_size();
            if actual_batch_limit != final_batch_size {
                return Err(ErrorCode::StorageUnsupported(format!(
                    "Stream batch size must be consistent within transaction: actual={:?}, requested={:?}",
                    actual_batch_limit, final_batch_size
                )));
            }
        } else if max_batch_size.is_some() {
            return Err(ErrorCode::StorageUnsupported(
                "MAX_BATCH_SIZE parameter only supported for STREAM tables",
            ));
        }
        Ok(table)
    }

    async fn acquire_table_lock(
        self: Arc<Self>,
        catalog_name: &str,
        db_name: &str,
        tbl_name: &str,
        branch: Option<&str>,
        lock_opt: &LockTableOption,
    ) -> Result<Option<Arc<LockGuard>>> {
        let enabled_table_lock = self.get_settings().get_enable_table_lock().unwrap_or(false);
        if !enabled_table_lock {
            return Ok(None);
        }

        let catalog = self.get_catalog(catalog_name).await?;
        let tbl = catalog
            .get_table_with_branch(&self.get_tenant(), db_name, tbl_name, branch)
            .await?;
        if tbl.engine() != "FUSE" || tbl.is_read_only() || tbl.is_temp() {
            return Ok(None);
        }

        let table_lock = LockManager::create_table_lock(tbl.get_table_info().clone())?;
        let lock_guard = match lock_opt {
            LockTableOption::LockNoRetry => table_lock.try_lock(self.clone(), false).await?,
            LockTableOption::LockWithRetry => table_lock.try_lock(self.clone(), true).await?,
            LockTableOption::NoLock => None,
        };
        if lock_guard.is_some() {
            self.evict_table_from_cache(catalog_name, db_name, tbl_name, branch)?;
        }
        Ok(lock_guard)
    }

    fn get_temp_table_prefix(&self) -> Result<String> {
        self.shared.session.get_temp_table_prefix()
    }

    fn is_temp_table(&self, catalog_name: &str, database_name: &str, table_name: &str) -> bool {
        catalog_name == CATALOG_DEFAULT
            && self
                .shared
                .session
                .session_ctx
                .temp_tbl_mgr()
                .lock()
                .is_temp_table(database_name, table_name)
    }
}

#[async_trait::async_trait]
impl TableContextCte for QueryContext {
    fn add_m_cte_temp_table(&self, database_name: &str, table_name: &str) {
        let entry = (database_name.to_string(), table_name.to_string());
        let mut tables = self.m_cte_temp_table.write();
        if !tables.contains(&entry) {
            tables.push(entry);
        }
    }

    async fn drop_m_cte_temp_table(&self) -> Result<()> {
        let m_cte_temp_table = self.m_cte_temp_table.read().clone();
        self.drop_cte_temp_tables(&m_cte_temp_table).await?;
        self.m_cte_temp_table.write().clear();
        Ok(())
    }

    fn add_recursive_cte_temp_table(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
    ) {
        let entry = (
            catalog_name.to_string(),
            database_name.to_string(),
            table_name.to_string(),
        );
        let mut tables = self.shared.recursive_cte_temp_tables.write();
        if !tables.contains(&entry) {
            tables.push(entry);
        }
    }

    async fn drop_recursive_cte_temp_table(&self) -> Result<()> {
        let recursive_cte_temp_tables = self.shared.recursive_cte_temp_tables.read().clone();
        self.drop_registered_cte_temp_tables(&recursive_cte_temp_tables)
            .await?;
        self.shared.recursive_cte_temp_tables.write().clear();
        Ok(())
    }
}

#[async_trait::async_trait]
impl TableContextCopy for QueryContext {
    #[async_backtrace::framed]
    async fn filter_out_copied_files(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        branch_name: Option<&str>,
        files: &[StageFileInfo],
        path_prefix: Option<String>,
        max_files: Option<usize>,
    ) -> Result<FilteredCopyFiles> {
        if files.is_empty() {
            info!("No files to filter for copy operation");
            return Ok(FilteredCopyFiles::default());
        }

        let collect_duplicated_files = self
            .get_settings()
            .get_enable_purge_duplicated_files_in_copy()?;

        let tenant = self.get_tenant();
        let catalog = self.get_catalog(catalog_name).await?;
        let table = catalog
            .get_table_with_branch(&tenant, database_name, table_name, branch_name)
            .await?;
        let table_id = table.get_id();

        let mut result_size: usize = 0;
        let max_files = max_files.unwrap_or(usize::MAX);
        let batch_size = min(COPIED_FILES_FILTER_BATCH_SIZE, max_files);

        let mut files_to_copy = Vec::with_capacity(files.len());
        let mut duplicated_files = Vec::with_capacity(files.len());

        for chunk in files.chunks(batch_size) {
            let files = chunk
                .iter()
                .map(|v| {
                    if let Some(p) = &path_prefix {
                        format!("{}{}", p, v.path)
                    } else {
                        v.path.clone()
                    }
                })
                .collect::<Vec<_>>();
            let req = GetTableCopiedFileReq {
                table_id,
                files: files.clone(),
            };
            let start_request = Instant::now();
            let copied_files = catalog
                .get_table_copied_file_info(&tenant, database_name, req)
                .await?
                .file_info;

            metrics_inc_copy_filter_out_copied_files_request_milliseconds(
                Instant::now().duration_since(start_request).as_millis() as u64,
            );
            for (file, key) in chunk.iter().zip(files.iter()) {
                if !copied_files.contains_key(key) {
                    files_to_copy.push(file.clone());
                    result_size += 1;
                    if result_size == max_files {
                        return Ok(FilteredCopyFiles {
                            files_to_copy,
                            duplicated_files,
                        });
                    }
                    if result_size > COPY_MAX_FILES_PER_COMMIT {
                        return Err(ErrorCode::Internal(format!(
                            "{}",
                            COPY_MAX_FILES_COMMIT_MSG
                        )));
                    }
                } else if collect_duplicated_files && duplicated_files.len() < max_files {
                    duplicated_files.push(file.path.clone());
                }
            }
        }
        Ok(FilteredCopyFiles {
            files_to_copy,
            duplicated_files,
        })
    }

    fn add_file_status(&self, file_path: &str, file_status: FileStatus) -> Result<()> {
        if matches!(self.get_query_kind(), QueryKind::CopyIntoTable) {
            self.shared
                .copy_state
                .add_file_status(file_path, file_status);
        }
        Ok(())
    }
}

impl TableContextQueryProfile for QueryContext {
    fn get_queries_profile(&self) -> HashMap<String, Vec<PlanProfile>> {
        SessionManager::instance().get_queries_profiles()
    }

    fn add_query_profiles(&self, profiles: &HashMap<u32, PlanProfile>) {
        self.shared.add_query_profiles(profiles)
    }

    fn get_query_profiles(&self) -> Vec<PlanProfile> {
        self.shared.get_query_profiles()
    }
}

#[async_trait::async_trait]
impl TableContextStage for QueryContext {
    fn get_stage_attachment(&self) -> Option<StageAttachment> {
        self.shared.get_stage_attachment()
    }

    #[async_backtrace::framed]
    async fn get_file_format(&self, name: &str) -> Result<FileFormatParams> {
        match StageFileFormatType::from_str(name) {
            Ok(typ) => FileFormatParams::default_by_type(typ),
            Err(_) => {
                let user_mgr = UserApiProvider::instance();
                let tenant = self.get_tenant();
                Ok(user_mgr
                    .get_file_format(&tenant, name)
                    .await?
                    .file_format_params)
            }
        }
    }

    async fn get_connection(&self, name: &str) -> Result<UserDefinedConnection> {
        if self
            .get_settings()
            .get_enable_experimental_connection_privilege_check()?
        {
            let visibility_checker = self
                .get_visibility_checker(false, Object::Connection)
                .await?;
            if !visibility_checker.check_connection_visibility(name) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege AccessConnection is required on connection {name} for user {}",
                    &self.get_current_user()?.identity().display(),
                )));
            }
        }
        self.shared.get_connection(name).await
    }
}

#[async_trait::async_trait]
impl TableContextTableManagement for QueryContext {
    fn evict_table_from_cache(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        branch: Option<&str>,
    ) -> Result<()> {
        self.shared
            .evict_table_from_cache(catalog, database, table, branch)
    }

    fn get_table_meta_timestamps(
        &self,
        table: &dyn Table,
        previous_snapshot: Option<Arc<TableSnapshot>>,
    ) -> Result<TableMetaTimestamps> {
        let table_id = table.get_id();

        let cached_table_timestamps = {
            self.shared
                .table_meta_timestamps
                .lock()
                .get(&table_id)
                .copied()
        };

        if let Some(ts) = cached_table_timestamps {
            return Ok(ts);
        }

        let fuse_table = FuseTable::try_from_table(table)?;
        let is_transient = fuse_table.is_transient();
        let delta = {
            let duration = if is_transient {
                Duration::from_secs(0)
            } else {
                let settings = self.get_settings();
                let max_exec_time_secs = settings.get_max_execute_time_in_seconds()?;
                if max_exec_time_secs != 0 {
                    Duration::from_secs(max_exec_time_secs)
                } else {
                    match fuse_table.get_table_retention_period() {
                        None => Duration::from_days(settings.get_data_retention_time_in_days()?),
                        Some(v) => v,
                    }
                }
            };

            chrono::Duration::from_std(duration).map_err(|e| {
                ErrorCode::Internal(format!(
                    "Unable to construct delta duration of table meta timestamp: {e}",
                ))
            })?
        };

        let validation_context = SnapshotTimestampValidationContext {
            table_id,
            is_transient,
        };

        let table_meta_timestamps = TableMetaTimestamps::with_snapshot_timestamp_validation_context(
            previous_snapshot,
            delta,
            Some(validation_context),
        );

        {
            let txn_mgr_ref = self.txn_mgr();
            let mut txn_mgr = txn_mgr_ref.lock();

            if txn_mgr.is_active() {
                let existing_timestamp = txn_mgr.get_table_txn_begin_timestamp(table_id);

                if let Some(existing_ts) = existing_timestamp {
                    if table_meta_timestamps.segment_block_timestamp < existing_ts {
                        return Err(ErrorCode::Internal(format!(
                            "Transaction timestamp violation: table_id = {}, new segment timestamp {:?} is lesser than existing transaction timestamp {:?}",
                            table_id, table_meta_timestamps.segment_block_timestamp, existing_ts
                        )));
                    }
                } else {
                    txn_mgr.set_table_txn_begin_timestamp(
                        table_id,
                        table_meta_timestamps.segment_block_timestamp,
                    );
                }
            }
        }

        {
            let mut cache = self.shared.table_meta_timestamps.lock();
            cache.insert(table_id, table_meta_timestamps);
        }

        Ok(table_meta_timestamps)
    }

    #[async_backtrace::framed]
    async fn load_datalake_schema(
        &self,
        kind: &str,
        sp: &StorageParams,
    ) -> Result<(TableSchema, String)> {
        match kind {
            "delta" => {
                let table = DeltaTable::load(sp).await?;
                DeltaTable::get_meta(&table).await
            }
            _ => Err(ErrorCode::Internal(
                "Unsupported datalake type for schema loading",
            )),
        }
    }

    #[cfg(feature = "storage-stage")]
    async fn create_stage_table(
        &self,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        files_to_copy: Option<Vec<StageFileInfo>>,
        max_column_position: usize,
        on_error_mode: Option<OnErrorMode>,
    ) -> Result<Arc<dyn Table>> {
        let copy_options = CopyIntoTableOptions {
            on_error: on_error_mode.unwrap_or_default(),
            ..Default::default()
        };
        let operator = init_stage_operator(&stage_info)?;
        let info = operator.info();
        let stage_root = format!("{}{}", info.name(), info.root());
        let stage_root = if stage_root.ends_with('/') {
            stage_root
        } else {
            format!("{}/", stage_root)
        };
        match &stage_info.file_format_params {
            FileFormatParams::Parquet(fmt) => {
                if max_column_position > 1 {
                    Err(ErrorCode::SemanticError(
                        "Query from parquet file only support $1 as column position",
                    ))
                } else if max_column_position == 0 {
                    let settings = self.get_settings();
                    let mut read_options = ParquetReadOptions::default();

                    if !settings.get_enable_parquet_page_index()? {
                        read_options = read_options.with_prune_pages(false);
                    }

                    if !settings.get_enable_parquet_rowgroup_pruning()? {
                        read_options = read_options.with_prune_row_groups(false);
                    }

                    if !settings.get_enable_parquet_prewhere()? {
                        read_options = read_options.with_do_prewhere(false);
                    }
                    ParquetTable::create(
                        self,
                        stage_info.clone(),
                        files_info,
                        read_options,
                        files_to_copy,
                        self.get_settings(),
                        self.get_query_kind(),
                        fmt,
                    )
                    .await
                } else {
                    let schema = Arc::new(TableSchema::new(vec![TableField::new(
                        "_$1",
                        TableDataType::Variant,
                    )]));
                    let info = StageTableInfo {
                        schema,
                        stage_info,
                        files_info,
                        files_to_copy,
                        duplicated_files_detected: vec![],
                        is_select: true,
                        default_exprs: None,
                        copy_into_table_options: copy_options.clone(),
                        stage_root,
                        is_variant: true,
                        parquet_metas: None,
                    };
                    StageTable::try_create(info)
                }
            }
            FileFormatParams::Orc(..) => {
                let is_variant = match max_column_position {
                    0 => false,
                    1 => true,
                    _ => {
                        return Err(ErrorCode::SemanticError(
                            "Query from ORC file only support $1 as column position",
                        ));
                    }
                };
                let schema = Arc::new(TableSchema::empty());
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy,
                    stage_root,
                    is_variant,
                    is_select: true,
                    copy_into_table_options: copy_options.clone(),
                    ..Default::default()
                };
                OrcTable::try_create(self, info).await
            }
            FileFormatParams::NdJson(..) | FileFormatParams::Avro(..) => {
                let schema = Arc::new(TableSchema::new(vec![TableField::new(
                    "_$1",
                    TableDataType::Variant,
                )]));
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy,
                    is_select: true,
                    is_variant: true,
                    stage_root,
                    copy_into_table_options: copy_options.clone(),
                    ..Default::default()
                };
                StageTable::try_create(info)
            }
            FileFormatParams::Csv(..) | FileFormatParams::Text(..) => {
                if max_column_position == 0 {
                    let file_type = match stage_info.file_format_params {
                        FileFormatParams::Csv(..) => "CSV",
                        FileFormatParams::Text(..) => "TEXT",
                        _ => unreachable!(),
                    };

                    return Err(ErrorCode::SemanticError(format!(
                        "Query from {} file lacks column positions. Specify as $1, $2, etc.",
                        file_type
                    )));
                }
                if let FileFormatParams::Text(fmt) = &stage_info.file_format_params {
                    if fmt.field_delimiter.is_empty() && max_column_position > 1 {
                        return Err(ErrorCode::SemanticError(
                            "Query from TEXT line mode only supports $1 as column position",
                        ));
                    }
                }

                let mut fields = vec![];
                for i in 1..(max_column_position + 1) {
                    fields.push(TableField::new(
                        &format!("_${}", i),
                        TableDataType::Nullable(Box::new(TableDataType::String)),
                    ));
                }

                let schema = Arc::new(TableSchema::new(fields));
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy,
                    is_select: true,
                    stage_root,
                    copy_into_table_options: copy_options.clone(),
                    ..Default::default()
                };
                StageTable::try_create(info)
            }
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "Unsupported file format in query stage. Supported formats: Parquet, NDJson, AVRO, CSV, TEXT. Provided: '{}'",
                    stage_info.file_format_params
                )));
            }
        }
    }

    #[cfg(not(feature = "storage-stage"))]
    async fn create_stage_table(
        &self,
        _stage_info: StageInfo,
        _files_info: StageFilesInfo,
        _files_to_copy: Option<Vec<StageFileInfo>>,
        _max_column_position: usize,
        _on_error_mode: Option<OnErrorMode>,
    ) -> Result<Arc<dyn Table>> {
        Err(ErrorCode::Unimplemented(
            "Stage table support is disabled, rebuild with cargo feature 'storage-stage'",
        ))
    }
}
