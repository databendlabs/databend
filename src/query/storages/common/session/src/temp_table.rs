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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::TableEngineMismatch;
use databend_common_meta_app::schema::CommitTableMetaReply;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::ListTableCopiedFileReply;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SwapTableReq;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TruncateTableReply;
use databend_common_meta_app::schema::UpdateTempTableReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_storage::DataOperator;
use databend_common_storage::EndpointPolicyScope;
use databend_common_storage::init_operator_with_policy_scope;
use databend_meta_client::types::SeqV;
use databend_storages_common_blocks::memory::IN_MEMORY_DATA;
use databend_storages_common_blocks::memory::InMemoryDataKey;
use databend_storages_common_table_meta::meta::parse_storage_prefix;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table_id_ranges::TEMP_TBL_ID_BEGIN;
use databend_storages_common_table_meta::table_id_ranges::is_temp_table_id;
use log::info;
use opendal::Operator;
use parking_lot::Mutex;

// Staged CTAS replacements share name_to_id with user-created temporary tables, so reserve a
// namespace that users cannot enter through CREATE or RENAME.
const TEMP_ORPHAN_TABLE_PREFIX: &str = "__tmp_orphan@";

#[derive(Debug, Clone)]
pub struct TempTblMgr {
    name_to_id: HashMap<String, u64>,
    id_to_table: HashMap<u64, TempTable>,
    next_id: u64,
}

#[derive(Debug, Clone)]
pub struct TempTable {
    pub db_name: String,
    pub table_name: String,
    pub meta: TableMeta,
    pub copied_files: BTreeMap<String, TableCopiedFileInfo>,
}

impl TempTblMgr {
    fn temp_table_desc(db_name: &str, table_name: &str) -> String {
        format!("'{}'.'{}'", db_name, table_name)
    }

    fn is_orphan_table_name(table_name: &str) -> bool {
        table_name.starts_with(TEMP_ORPHAN_TABLE_PREFIX)
    }

    fn ensure_user_table_name(table_name: &str) -> Result<()> {
        if Self::is_orphan_table_name(table_name) {
            return Err(ErrorCode::BadArguments(format!(
                "Temporary table names starting with '{TEMP_ORPHAN_TABLE_PREFIX}' are reserved"
            )));
        }
        Ok(())
    }

    pub fn init() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(TempTblMgr {
            name_to_id: HashMap::new(),
            id_to_table: HashMap::new(),
            next_id: TEMP_TBL_ID_BEGIN,
        }))
    }

    fn inc_next_id(&mut self) {
        self.next_id += 1;
        if !is_temp_table_id(self.next_id) {
            panic!("Temp table id used up");
        }
    }

    pub fn is_empty(&mut self) -> bool {
        self.id_to_table.is_empty()
    }

    pub fn create_table(
        &mut self,
        req: CreateTableReq,
        prefix: String,
    ) -> Result<CreateTableReply> {
        let CreateTableReq {
            create_option,
            name_ident,
            table_meta,
            as_dropped,
            ..
        } = req;
        Self::ensure_user_table_name(&name_ident.table_name)?;
        let Some(db_id) = table_meta.options.get(OPT_KEY_DATABASE_ID) else {
            return Err(ErrorCode::Internal(format!(
                "Database id not set in table options"
            )));
        };
        let db_id = db_id.parse::<u64>()?;

        let desc = Self::temp_table_desc(&name_ident.db_name, &name_ident.table_name);
        let existing_id = self.name_to_id.get(&desc).copied();
        let engine = table_meta.engine.to_string();
        let table_id = self.next_id;
        // Keep a two-phase CTAS replacement hidden under its unique temporary table ID until
        // commit_table_meta publishes it under the requested name.
        let orphan_table_name = as_dropped.then(|| format!("{TEMP_ORPHAN_TABLE_PREFIX}{table_id}"));
        let new_table = match (existing_id, create_option) {
            (Some(_), CreateOption::Create) => {
                return Err(ErrorCode::TableAlreadyExists(format!(
                    "Temporary table {} already exists",
                    desc
                )));
            }
            (Some(_), CreateOption::CreateIfNotExists) => false,
            (existing_id, _) => {
                if let Some(existing_id) = existing_id {
                    let existing_table = self.id_to_table.get(&existing_id).ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "Got temporary table id {existing_id}, but its metadata was not found"
                        ))
                    })?;
                    TableEngineMismatch::ensure(
                        &name_ident.table_name,
                        &existing_table.meta.engine,
                        &table_meta.engine,
                    )
                    .map_err(|e| ErrorCode::from(AppError::from(e)))?;
                }

                let desc = orphan_table_name
                    .as_ref()
                    .map(|o| Self::temp_table_desc(&name_ident.db_name, o))
                    .unwrap_or(desc);
                let old_id = self.name_to_id.insert(desc.clone(), table_id);
                if let Some(old_id) = old_id {
                    self.id_to_table.remove(&old_id);
                }
                self.id_to_table.insert(table_id, TempTable {
                    db_name: name_ident.db_name,
                    table_name: orphan_table_name.clone().unwrap_or(name_ident.table_name),
                    meta: table_meta,
                    copied_files: BTreeMap::new(),
                });
                self.inc_next_id();
                info!(
                    "[TEMP TABLE] session={prefix} created {} table {desc}, id = {db_id}.{table_id}.",
                    engine
                );
                true
            }
        };

        Ok(CreateTableReply {
            table_id,
            table_id_seq: Some(0),
            db_id,
            new_table,
            // The commit guard must compare against the visible table observed during prepare.
            // Direct, single-phase creates do not need this value.
            prev_table_id: as_dropped.then_some(existing_id).flatten(),
            orphan_table_name,
        })
    }

    pub fn commit_table_meta(&mut self, req: &CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        let orphan_desc = Self::temp_table_desc(
            &req.name_ident.db_name,
            req.orphan_table_name.as_ref().unwrap(),
        );
        let desc = Self::temp_table_desc(&req.name_ident.db_name, &req.name_ident.table_name);
        // Validate the staged mapping before mutating it: concurrent CTAS requests may have the
        // same visible target, but each commit must publish only its own replacement.
        let orphan_id = self
            .name_to_id
            .get(&orphan_desc)
            .copied()
            .ok_or_else(|| ErrorCode::UnknownTable(orphan_desc.clone()))?;
        if orphan_id != req.table_id {
            return Err(ErrorCode::TableVersionMismatched(format!(
                "Temporary table {desc} replacement changed while CTAS was running"
            )));
        }

        // Do not overwrite a visible table that changed while the CTAS pipeline was running.
        let current_id = self.name_to_id.get(&desc).copied();
        if current_id != req.prev_table_id {
            return Err(ErrorCode::TableVersionMismatched(format!(
                "Temporary table {desc} changed while CTAS was running"
            )));
        }

        // Both publication guards passed, so removing the orphan mapping is now safe.
        self.name_to_id.remove(&orphan_desc);
        if let Some(old_id) = self.name_to_id.insert(desc, orphan_id) {
            self.id_to_table.remove(&old_id);
        }
        let table = self.id_to_table.get_mut(&orphan_id).unwrap();
        table.db_name = req.name_ident.db_name.clone();
        table.table_name = req.name_ident.table_name.clone();

        Ok(CommitTableMetaReply {})
    }

    pub fn rename_table(&mut self, req: &RenameTableReq) -> Result<Option<RenameTableReply>> {
        let RenameTableReq {
            if_exists: _,
            name_ident,
            new_db_name,
            new_table_name,
        } = req;
        let desc = Self::temp_table_desc(&name_ident.db_name, &name_ident.table_name);
        if Self::is_orphan_table_name(&name_ident.table_name) {
            // Reject access to an active staged replacement. Only names not owned by this manager
            // may fall through to the underlying catalog.
            if self.name_to_id.contains_key(&desc) {
                Self::ensure_user_table_name(&name_ident.table_name)?;
            }
            return Ok(None);
        }
        // Keep the source mapping intact until all destination checks pass.
        match self.name_to_id.get(&desc).copied() {
            Some(id) => {
                Self::ensure_user_table_name(new_table_name)?;
                let new_desc = Self::temp_table_desc(new_db_name, new_table_name);
                // A no-op rename finds the source itself and is not a destination collision.
                if new_desc != desc && self.name_to_id.contains_key(&new_desc) {
                    return Err(ErrorCode::TableAlreadyExists(format!(
                        "Temporary table {} already exists",
                        new_desc
                    )));
                }
                self.name_to_id.remove(&desc);
                self.name_to_id.insert(new_desc, id);
                let table = self.id_to_table.get_mut(&id).unwrap();
                table.db_name = new_db_name.clone();
                table.table_name = new_table_name.clone();
                Ok(Some(RenameTableReply { table_id: 0 }))
            }
            None => Ok(None),
        }
    }

    pub fn swap_table(&mut self, _req: &SwapTableReq) -> Result<Option<SwapTableReq>> {
        Err(ErrorCode::Unimplemented("Cannot swap tmp table"))
    }

    pub fn get_table_meta_by_id(&self, id: u64) -> Result<Option<SeqV<TableMeta>>> {
        Ok(self
            .id_to_table
            .get(&id)
            .map(|t| SeqV::new(0, t.meta.clone())))
    }

    pub fn get_table_name_by_id(&self, id: u64) -> Option<String> {
        self.id_to_table.get(&id).map(|t| t.table_name.clone())
    }

    pub fn is_temp_table(&self, database_name: &str, table_name: &str) -> bool {
        let desc = Self::temp_table_desc(database_name, table_name);
        self.name_to_id.contains_key(&desc)
    }

    pub fn get_table(&self, database_name: &str, table_name: &str) -> Result<Option<TableInfo>> {
        let desc = Self::temp_table_desc(database_name, table_name);
        let id = self.name_to_id.get(&desc);
        let Some(id) = id else {
            return Ok(None);
        };
        let Some(table) = self.id_to_table.get(id) else {
            return Err(ErrorCode::Internal(format!(
                "Got table id {:?} but not found meta in temp table manager {:?}",
                id, self
            )));
        };
        let ident = TableIdent {
            table_id: *id,
            ..Default::default()
        };
        let table_info = TableInfo::new(database_name, table_name, ident, table.meta.clone());
        Ok(Some(table_info))
    }

    pub fn list_tables(&self) -> Result<Vec<TableInfo>> {
        Ok(self
            .id_to_table
            .iter()
            .map(|(id, t)| {
                TableInfo::new(
                    &t.db_name,
                    &t.table_name,
                    TableIdent::new(*id, 0),
                    t.meta.clone(),
                )
            })
            .collect())
    }

    pub fn update_multi_table_meta(&mut self, req: Vec<UpdateTempTableReq>) {
        for r in req {
            let UpdateTempTableReq {
                table_id,
                new_table_meta,
                copied_files,
                ..
            } = r;
            let table = self.id_to_table.get_mut(&table_id).unwrap();
            table.meta = new_table_meta;
            table.copied_files.extend(copied_files);
        }
    }

    pub fn upsert_table_option(
        &mut self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        let UpsertTableOptionReq {
            table_id, options, ..
        } = req;
        let table = self.id_to_table.get_mut(&table_id);
        let Some(table) = table else {
            return Err(ErrorCode::UnknownTable(format!(
                "Temporary table id {} not found",
                table_id
            )));
        };
        for (k, v) in options {
            if let Some(v) = v {
                table.meta.options.insert(k, v);
            } else {
                table.meta.options.remove(&k);
            }
        }
        Ok(UpsertTableOptionReply {})
    }

    pub fn truncate_table(&mut self, id: u64) -> Result<TruncateTableReply> {
        let table = self.id_to_table.get_mut(&id);
        let Some(table) = table else {
            return Err(ErrorCode::UnknownTable(format!(
                "Temporary table id {} not found",
                id
            )));
        };
        table.copied_files.clear();
        Ok(TruncateTableReply {})
    }

    pub fn get_table_copied_file_info(
        &self,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        let Some(table) = self.id_to_table.get(&req.table_id) else {
            return Err(ErrorCode::UnknownTable(format!(
                "Temporary table id {} not found",
                req.table_id
            )));
        };
        let mut file_info = BTreeMap::new();
        for name in req.files {
            if let Some(info) = table.copied_files.get(&name) {
                file_info.insert(name, info.clone());
            }
        }
        Ok(GetTableCopiedFileReply { file_info })
    }

    pub fn list_table_copied_file_info(&self, table_id: u64) -> Result<ListTableCopiedFileReply> {
        let Some(table) = self.id_to_table.get(&table_id) else {
            return Err(ErrorCode::UnknownTable(format!(
                "Temporary table id {} not found",
                table_id
            )));
        };
        let file_info = table.copied_files.clone();
        Ok(ListTableCopiedFileReply { file_info })
    }
}

/// Get the appropriate operator for a table based on its storage configuration for vacuum dropped table operations.
/// Note that this operator is NOT storage class setting aware, DO NOT use it for put object operations
fn get_table_operator_for_drop_operation(table_meta: &TableMeta) -> Result<Operator> {
    // Check if the table has custom storage parameters
    if let Some(storage_params) = &table_meta.storage_params {
        // Use the custom storage parameters to create an operator
        init_operator_with_policy_scope(storage_params, EndpointPolicyScope::External)
            .map_err(|e| ErrorCode::StorageUnavailable(format!("Failed to init operator: {}", e)))
    } else {
        // Use the default operator
        Ok(DataOperator::instance().operator())
    }
}

pub async fn drop_table_by_id(
    mgr: TempTblMgrRef,
    req: DropTableByIdReq,
) -> Result<Option<DropTableReply>> {
    let DropTableByIdReq { tb_id, engine, .. } = &req;
    info!(
        "[TEMP TABLE] session={} dropping {} table {tb_id}.",
        req.temp_prefix,
        engine.as_str()
    );
    match engine.as_str() {
        "FUSE" => {
            let (dir, table_meta) = {
                let mut guard = mgr.lock();
                let entry = guard.id_to_table.entry(*tb_id);
                match entry {
                    Entry::Occupied(e) => {
                        let dir = parse_storage_prefix(&e.get().meta.options, *tb_id)?;
                        let table = e.remove();
                        let table_meta = table.meta.clone();
                        let desc = TempTblMgr::temp_table_desc(&table.db_name, &table.table_name);
                        guard.name_to_id.remove(&desc).ok_or_else(|| {
                            ErrorCode::Internal(format!(
                                "Table not found in temp table manager {:?}, drop table request: {:?}",
                                guard, req
                            ))
                        })?;
                        (dir, table_meta)
                    }
                    Entry::Vacant(_) => {
                        return Ok(None);
                    }
                }
            };
            let op = get_table_operator_for_drop_operation(&table_meta)?;
            op.remove_all(&dir).await?;
        }
        "MEMORY" => {
            let mut guard = mgr.lock();
            let entry = guard.id_to_table.entry(*tb_id);
            match entry {
                Entry::Occupied(e) => {
                    let table = e.remove();
                    let desc = TempTblMgr::temp_table_desc(&table.db_name, &table.table_name);
                    guard.name_to_id.remove(&desc).ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "Table not found in temp table manager {:?}, drop table request: {:?}",
                            guard, req
                        ))
                    })?;
                }
                Entry::Vacant(_) => {
                    return Ok(None);
                }
            }
            let key = InMemoryDataKey {
                temp_prefix: Some(req.temp_prefix.clone()),
                table_id: *tb_id,
            };
            let mut in_mem_data = IN_MEMORY_DATA.write();
            in_mem_data.remove(&key).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Table not found in memory data {:?}, drop table request: {:?}",
                    in_mem_data, req
                ))
            })?;
        }
        _ => return Ok(None),
    };

    Ok(Some(DropTableReply {}))
}

pub async fn drop_all_temp_tables(
    user_name_session_id: &str,
    mgr: TempTblMgrRef,
    reason: &str,
) -> Result<()> {
    let (fuse_table_data, mem_tbl_ids) = {
        let mut guard = mgr.lock();
        let mut fuse_table_data = Vec::new(); // (dir, table_meta)
        let mut mem_tbl_ids = Vec::new();
        for (id, table) in &guard.id_to_table {
            let engine = table.meta.engine.as_str();
            if engine == "FUSE" {
                // Parse the storage prefix to get the directory path for each table
                match parse_storage_prefix(&table.meta.options, *id) {
                    Ok(dir) => fuse_table_data.push((dir, table.meta.clone())),
                    Err(e) => {
                        // Log the error but continue with other tables
                        log::warn!(
                            "[TEMP TABLE] Failed to parse storage prefix for table {}: {}",
                            id,
                            e
                        );
                    }
                }
            } else if engine == "MEMORY" {
                mem_tbl_ids.push(*id);
            }
        }
        guard.id_to_table.clear();
        guard.name_to_id.clear();
        (fuse_table_data, mem_tbl_ids)
    };

    let num_fuse_table = fuse_table_data.len();
    let num_mem_table = mem_tbl_ids.len();

    info!(
        "[TEMP TABLE] session={user_name_session_id} starting cleanup, reason = {reason}, {} fuse table, {} mem table.",
        num_fuse_table, num_mem_table
    );

    // Clean up each fuse table directory individually with the correct operator
    for (dir, table_meta) in fuse_table_data {
        // Get the operator for this specific table's storage location
        match get_table_operator_for_drop_operation(&table_meta) {
            Ok(op) => {
                if let Err(e) = op.remove_all(&dir).await {
                    // Log the error but continue with other tables
                    log::warn!(
                        "[TEMP TABLE] Failed to clean up temp table directory '{}': {}",
                        dir,
                        e
                    );
                }
            }
            Err(e) => {
                log::warn!(
                    "[TEMP TABLE] Failed to get operator for temp table directory '{}': {}",
                    dir,
                    e
                );
            }
        }
    }

    if !mem_tbl_ids.is_empty() {
        let mut in_mem_data = IN_MEMORY_DATA.write();
        for id in mem_tbl_ids {
            let key = InMemoryDataKey {
                temp_prefix: Some(user_name_session_id.to_string()),
                table_id: id,
            };
            in_mem_data.remove(&key);
        }
    }

    Ok(())
}

pub type TempTblMgrRef = Arc<Mutex<TempTblMgr>>;

#[cfg(test)]
mod tests {
    use databend_common_meta_app::schema::TableNameIdent;
    use databend_common_meta_app::tenant::Tenant;

    use super::*;

    fn new_temp_table_mgr() -> TempTblMgr {
        TempTblMgr {
            name_to_id: HashMap::new(),
            id_to_table: HashMap::new(),
            next_id: TEMP_TBL_ID_BEGIN,
        }
    }

    fn table_name_ident(table_name: &str) -> TableNameIdent {
        TableNameIdent {
            tenant: Tenant::new_literal("tenant"),
            db_name: "db".to_string(),
            table_name: table_name.to_string(),
        }
    }

    fn create_table_req(
        table_name: &str,
        engine: &str,
        create_option: CreateOption,
        as_dropped: bool,
    ) -> CreateTableReq {
        let mut table_meta = TableMeta {
            engine: engine.to_string(),
            ..Default::default()
        };
        table_meta
            .options
            .insert(OPT_KEY_DATABASE_ID.to_string(), "1".to_string());

        CreateTableReq {
            create_option,
            catalog_name: None,
            name_ident: table_name_ident(table_name),
            table_meta,
            as_dropped,
            materialized_view: None,
            table_properties: None,
            table_partition: None,
        }
    }

    fn rename_table_req(table_name: &str, new_table_name: &str) -> RenameTableReq {
        RenameTableReq {
            if_exists: false,
            name_ident: table_name_ident(table_name),
            new_db_name: "db".to_string(),
            new_table_name: new_table_name.to_string(),
        }
    }

    fn commit_table_req(table_name: &str, reply: &CreateTableReply) -> CommitTableMetaReq {
        CommitTableMetaReq {
            name_ident: table_name_ident(table_name),
            db_id: reply.db_id,
            table_id: reply.table_id,
            prev_table_id: reply.prev_table_id,
            orphan_table_name: reply.orphan_table_name.clone(),
        }
    }

    #[test]
    fn test_ctas_commit_preserves_temporary_tables_when_visible_table_changed() {
        let mut mgr = new_temp_table_mgr();
        let table_name = "t";
        let desc = TempTblMgr::temp_table_desc("db", table_name);

        let visible = mgr
            .create_table(
                create_table_req(table_name, "FUSE", CreateOption::Create, false),
                "session".to_string(),
            )
            .unwrap();
        let replacement = mgr
            .create_table(
                create_table_req(table_name, "FUSE", CreateOption::CreateOrReplace, true),
                "session".to_string(),
            )
            .unwrap();
        assert_eq!(replacement.prev_table_id, Some(visible.table_id));

        mgr.name_to_id.remove(&desc);
        mgr.id_to_table.remove(&visible.table_id);
        let current = mgr
            .create_table(
                create_table_req(table_name, "MEMORY", CreateOption::Create, false),
                "session".to_string(),
            )
            .unwrap();

        let orphan_desc =
            TempTblMgr::temp_table_desc("db", replacement.orphan_table_name.as_ref().unwrap());
        let current_meta = mgr.id_to_table.get(&current.table_id).unwrap().meta.clone();
        let replacement_meta = mgr
            .id_to_table
            .get(&replacement.table_id)
            .unwrap()
            .meta
            .clone();

        let err = mgr
            .commit_table_meta(&commit_table_req(table_name, &replacement))
            .unwrap_err();
        let expected = ErrorCode::TableVersionMismatched(format!(
            "Temporary table {desc} changed while CTAS was running"
        ));
        assert_eq!(
            (err.code(), err.message()),
            (expected.code(), expected.message())
        );

        assert_eq!(mgr.name_to_id.get(&desc), Some(&current.table_id));
        assert_eq!(
            mgr.name_to_id.get(&orphan_desc),
            Some(&replacement.table_id)
        );
        assert_eq!(
            mgr.id_to_table.get(&current.table_id).unwrap().meta,
            current_meta
        );
        assert_eq!(
            mgr.id_to_table.get(&replacement.table_id).unwrap().meta,
            replacement_meta
        );
    }

    #[test]
    fn test_ctas_commit_publishes_its_own_temporary_table() {
        let mut mgr = new_temp_table_mgr();
        let table_name = "t";
        let desc = TempTblMgr::temp_table_desc("db", table_name);

        let visible = mgr
            .create_table(
                create_table_req(table_name, "FUSE", CreateOption::Create, false),
                "session".to_string(),
            )
            .unwrap();
        let first = mgr
            .create_table(
                create_table_req(table_name, "FUSE", CreateOption::CreateOrReplace, true),
                "session".to_string(),
            )
            .unwrap();
        let second = mgr
            .create_table(
                create_table_req(table_name, "FUSE", CreateOption::CreateOrReplace, true),
                "session".to_string(),
            )
            .unwrap();

        assert_eq!(first.prev_table_id, Some(visible.table_id));
        assert_eq!(second.prev_table_id, Some(visible.table_id));
        assert_ne!(first.orphan_table_name, second.orphan_table_name);
        let first_orphan_desc =
            TempTblMgr::temp_table_desc("db", first.orphan_table_name.as_ref().unwrap());
        let second_orphan_desc =
            TempTblMgr::temp_table_desc("db", second.orphan_table_name.as_ref().unwrap());
        assert_eq!(
            mgr.name_to_id.get(&first_orphan_desc),
            Some(&first.table_id)
        );
        assert_eq!(
            mgr.name_to_id.get(&second_orphan_desc),
            Some(&second.table_id)
        );

        let name_to_id_before = mgr.name_to_id.clone();
        let table_meta_before: HashMap<_, _> = mgr
            .id_to_table
            .iter()
            .map(|(id, table)| (*id, table.meta.clone()))
            .collect();
        let mut mismatched_req = commit_table_req(table_name, &first);
        mismatched_req.table_id = second.table_id;
        let err = mgr.commit_table_meta(&mismatched_req).unwrap_err();
        let expected = ErrorCode::TableVersionMismatched(format!(
            "Temporary table {desc} replacement changed while CTAS was running"
        ));
        assert_eq!(
            (err.code(), err.message()),
            (expected.code(), expected.message())
        );
        assert_eq!(mgr.name_to_id, name_to_id_before);
        assert_eq!(
            mgr.id_to_table
                .iter()
                .map(|(id, table)| (*id, table.meta.clone()))
                .collect::<HashMap<_, _>>(),
            table_meta_before
        );

        mgr.commit_table_meta(&commit_table_req(table_name, &first))
            .unwrap();

        assert_eq!(mgr.name_to_id.get(&desc), Some(&first.table_id));
        assert!(!mgr.name_to_id.contains_key(&first_orphan_desc));
        assert_eq!(
            mgr.name_to_id.get(&second_orphan_desc),
            Some(&second.table_id)
        );
        assert!(!mgr.id_to_table.contains_key(&visible.table_id));
        assert!(mgr.id_to_table.contains_key(&first.table_id));
        assert!(mgr.id_to_table.contains_key(&second.table_id));

        let err = mgr
            .commit_table_meta(&commit_table_req(table_name, &second))
            .unwrap_err();
        let expected = ErrorCode::TableVersionMismatched(format!(
            "Temporary table {desc} changed while CTAS was running"
        ));
        assert_eq!(
            (err.code(), err.message()),
            (expected.code(), expected.message())
        );
        assert_eq!(mgr.name_to_id.get(&desc), Some(&first.table_id));
        assert_eq!(
            mgr.name_to_id.get(&second_orphan_desc),
            Some(&second.table_id)
        );
        assert!(mgr.id_to_table.contains_key(&first.table_id));
        assert!(mgr.id_to_table.contains_key(&second.table_id));
    }

    #[test]
    fn test_ctas_orphan_prefix_is_reserved() {
        let mut mgr = new_temp_table_mgr();
        let reserved_name = format!("{TEMP_ORPHAN_TABLE_PREFIX}user");

        let err = mgr
            .create_table(
                create_table_req(&reserved_name, "MEMORY", CreateOption::Create, false),
                "session".to_string(),
            )
            .unwrap_err();
        let expected = ErrorCode::BadArguments(format!(
            "Temporary table names starting with '{TEMP_ORPHAN_TABLE_PREFIX}' are reserved"
        ));
        assert_eq!(
            (err.code(), err.message()),
            (expected.code(), expected.message())
        );

        let visible = mgr
            .create_table(
                create_table_req("t", "MEMORY", CreateOption::Create, false),
                "session".to_string(),
            )
            .unwrap();
        assert!(
            mgr.rename_table(&rename_table_req("t", "t"))
                .unwrap()
                .is_some()
        );
        let err = mgr
            .rename_table(&rename_table_req("t", &reserved_name))
            .unwrap_err();
        assert_eq!(
            (err.code(), err.message()),
            (expected.code(), expected.message())
        );
        assert_eq!(
            mgr.name_to_id.get(&TempTblMgr::temp_table_desc("db", "t")),
            Some(&visible.table_id)
        );

        // Names not owned by the temporary manager must fall through to the inner catalog.
        assert!(
            mgr.rename_table(&rename_table_req("persistent", &reserved_name))
                .unwrap()
                .is_none()
        );
        assert!(
            mgr.rename_table(&rename_table_req(&reserved_name, "persistent"))
                .unwrap()
                .is_none()
        );

        let replacement = mgr
            .create_table(
                create_table_req("t", "MEMORY", CreateOption::CreateOrReplace, true),
                "session".to_string(),
            )
            .unwrap();
        let orphan_table_name = replacement.orphan_table_name.as_ref().unwrap();
        let orphan_desc = TempTblMgr::temp_table_desc("db", orphan_table_name);
        assert_eq!(
            replacement.orphan_table_name.as_deref(),
            Some(format!("{TEMP_ORPHAN_TABLE_PREFIX}{}", replacement.table_id).as_str())
        );
        assert_eq!(
            mgr.name_to_id.get(&orphan_desc),
            Some(&replacement.table_id)
        );
        let err = mgr
            .rename_table(&rename_table_req(orphan_table_name, "hijacked"))
            .unwrap_err();
        assert_eq!(
            (err.code(), err.message()),
            (expected.code(), expected.message())
        );
        assert_eq!(
            mgr.name_to_id.get(&orphan_desc),
            Some(&replacement.table_id)
        );
    }
}
