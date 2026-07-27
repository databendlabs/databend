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
use std::collections::VecDeque;
use std::fmt;
use std::io::BufRead;
use std::io::Write;

use anyhow::Context;
use databend_common_meta_api::kv_pb_api::decode_pb;
use databend_common_meta_app::data_mask::MaskpolicyTableIdList;
use databend_common_meta_app::principal::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_meta_app::principal::BUILTIN_ROLE_PUBLIC;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::schema::DbIdList;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::TableIdList;
use databend_common_proto_conv::FromToProto;
use databend_meta::raft_store::key_spaces::RaftStoreEntry;
use databend_meta::types::SeqV;
use serde::de::DeserializeOwned;

#[derive(Debug, Clone)]
pub struct TenantFilterOptions {
    pub tenant: String,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct TenantFilterReport {
    pub total_lines: usize,
    pub header_lines: usize,
    pub raft_log_lines: usize,
    pub state_machine_lines: usize,
    pub kept_state_machine_lines: usize,
    pub dropped_state_machine_lines: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Decision {
    Keep,
    Drop,
}

impl Decision {
    fn as_str(self) -> &'static str {
        match self {
            Decision::Keep => "keep",
            Decision::Drop => "drop",
        }
    }
}

#[derive(Debug, Clone)]
struct Mark {
    decision: Decision,
    reason: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct DependencyKey {
    key: String,
    optional_reason: Option<&'static str>,
}

impl DependencyKey {
    fn required(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            optional_reason: None,
        }
    }

    fn optional(key: impl Into<String>, reason: &'static str) -> Self {
        Self {
            key: key.into(),
            optional_reason: Some(reason),
        }
    }

    fn is_required(&self) -> bool {
        self.optional_reason.is_none()
    }
}

#[derive(Debug, Default)]
struct DependencyKeySet {
    keys: BTreeMap<String, Option<&'static str>>,
}

impl DependencyKeySet {
    fn required(&mut self, key: impl Into<String>) {
        self.insert(key, None);
    }

    fn optional(&mut self, key: impl Into<String>, reason: &'static str) {
        self.insert(key, Some(reason));
    }

    fn insert(&mut self, key: impl Into<String>, optional_reason: Option<&'static str>) {
        self.keys
            .entry(key.into())
            .and_modify(|existing| {
                if optional_reason.is_none() {
                    *existing = None;
                }
            })
            .or_insert(optional_reason);
    }

    fn into_vec(self) -> Vec<DependencyKey> {
        self.keys
            .into_iter()
            .map(|(key, optional_reason)| {
                if let Some(reason) = optional_reason {
                    DependencyKey::optional(key, reason)
                } else {
                    DependencyKey::required(key)
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
enum StateKind {
    System { label: String },
    GenericKV { key: String, value: SeqV<Vec<u8>> },
    Expire { key: String },
}

#[derive(Debug, Clone)]
struct StateLine {
    line_no: usize,
    line: String,
    kind: StateKind,
    mark: Option<Mark>,
}

impl StateLine {
    fn display_key(&self) -> String {
        match &self.kind {
            StateKind::System { label } => label.clone(),
            StateKind::GenericKV { key, .. } => key.clone(),
            StateKind::Expire { key } => format!("expire:{key}"),
        }
    }
}

#[derive(Debug, Default)]
struct TenantDump {
    report: TenantFilterReport,
    header_lines: Vec<String>,
    state_lines: Vec<StateLine>,
    key_to_state: BTreeMap<String, usize>,
    expires_by_key: BTreeMap<String, Vec<usize>>,
    index_ids_by_table_id: BTreeMap<u64, Vec<u64>>,
    mark_queue: VecDeque<(String, Decision)>,
}

impl TenantDump {
    fn load<R>(reader: R) -> anyhow::Result<Self>
    where R: BufRead {
        let mut dump = Self::load_lines(reader)?;
        dump.build_indexes()?;
        Ok(dump)
    }

    fn load_lines<R>(reader: R) -> anyhow::Result<Self>
    where R: BufRead {
        let mut dump = TenantDump::default();

        for (line_offset, line) in reader.lines().enumerate() {
            let line_no = line_offset + 1;
            let line = line.with_context(|| format!("failed to read line {line_no}"))?;
            if line.is_empty() {
                continue;
            }

            dump.report.total_lines += 1;

            let (tree_name, entry): (String, RaftStoreEntry) = serde_json::from_str(&line)
                .with_context(|| {
                    format!("failed to parse exported raft-store entry at line {line_no}")
                })?;

            if tree_name == "header" {
                dump.header_lines.push(line);
                dump.report.header_lines += 1;
                continue;
            }

            if !tree_name.starts_with("state_machine/") {
                dump.report.raft_log_lines += 1;
                continue;
            }

            let kind = match entry {
                RaftStoreEntry::GenericKV { key, value } => StateKind::GenericKV { key, value },
                RaftStoreEntry::Expire { value, .. } => StateKind::Expire { key: value.key },
                RaftStoreEntry::DataHeader { .. } => StateKind::System {
                    label: "state-machine-data-header".to_string(),
                },
                RaftStoreEntry::Nodes { key, .. } => StateKind::System {
                    label: format!("node:{key}"),
                },
                RaftStoreEntry::StateMachineMeta { key, .. } => StateKind::System {
                    label: format!("state-machine-meta:{key:?}"),
                },
                RaftStoreEntry::Sequences { key, .. } => StateKind::System {
                    label: format!("sequence:{key}"),
                },
                other => {
                    anyhow::bail!(
                        "unsupported state machine entry at line {}: {:?}",
                        line_no,
                        other
                    );
                }
            };

            dump.state_lines.push(StateLine {
                line_no,
                line,
                kind,
                mark: None,
            });
            dump.report.state_machine_lines += 1;
        }

        Ok(dump)
    }

    fn build_indexes(&mut self) -> anyhow::Result<()> {
        self.key_to_state.clear();
        self.expires_by_key.clear();
        self.index_ids_by_table_id.clear();

        for (state_index, state_line) in self.state_lines.iter().enumerate() {
            match &state_line.kind {
                StateKind::GenericKV { key, .. } => {
                    if let Some(prev) = self.key_to_state.insert(key.clone(), state_index) {
                        anyhow::bail!(
                            "duplicated GenericKV key found at lines {} and {}: {}",
                            self.state_lines[prev].line_no,
                            state_line.line_no,
                            key
                        );
                    }
                }
                StateKind::Expire { key } => {
                    self.expires_by_key
                        .entry(key.clone())
                        .or_default()
                        .push(state_index);
                }
                StateKind::System { .. } => {}
            }
        }

        for state_line in &self.state_lines {
            let StateKind::GenericKV { key, value } = &state_line.kind else {
                continue;
            };

            let segments = split_key(key);
            let ["__fd_index_by_id", index_id] = segments.as_slice() else {
                continue;
            };

            let index_id = parse_u64_segment(key, index_id)?;
            let index_meta = decode_as::<IndexMeta>(key, &value.data)?;
            self.index_ids_by_table_id
                .entry(index_meta.table_id)
                .or_default()
                .push(index_id);
        }

        Ok(())
    }

    fn mark_all(&mut self, tenant: &str) -> anyhow::Result<()> {
        self.mark_queue.clear();

        for index in 0..self.state_lines.len() {
            if matches!(self.state_lines[index].kind, StateKind::System { .. }) {
                self.mark_line_by_index(index, Decision::Keep, "system state machine entry")?;
            }
        }

        let keys = self.key_to_state.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            if let Some(root) = classify_root(&key, tenant) {
                self.mark_required_key(&key, root.decision, root.reason)?;
            }
        }

        self.drain_mark_queue()?;

        let keys = self.key_to_state.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let Some(index) = self.key_to_state.get(&key).copied() else {
                anyhow::bail!("key is missing from state index: {key}");
            };
            if self.state_lines[index].mark.is_some() {
                continue;
            }

            if let Some(reason) = self.classify_snapshot_orphan_root(&key)? {
                eprintln!(
                    "filter-tenant: drop snapshot orphan at line {}: {}: {}",
                    self.state_lines[index].line_no, key, reason
                );
                self.mark_required_key(&key, Decision::Drop, reason)?;
            }
        }

        self.drain_mark_queue()?;
        self.assert_every_state_line_marked()?;
        self.update_report();

        Ok(())
    }

    fn classify_snapshot_orphan_root(&self, key: &str) -> anyhow::Result<Option<String>> {
        let segments = split_key(key);

        let Some(index) = self.key_to_state.get(key).copied() else {
            anyhow::bail!("key is missing from state index: {key}");
        };
        let StateKind::GenericKV { value, .. } = &self.state_lines[index].kind else {
            anyhow::bail!(
                "indexed key is not a GenericKV state entry at line {}: {}",
                self.state_lines[index].line_no,
                self.state_lines[index].display_key()
            );
        };

        let reason = match segments.as_slice() {
            ["__fd_table", db_id, _table_name] => {
                let db_id = parse_u64_segment(key, db_id)?;
                if self.has_database_primary_record(db_id) {
                    return Ok(None);
                }
                Some(format!(
                    "snapshot orphan table name key with missing database id {db_id}"
                ))
            }
            ["__fd_index_by_id", _index_id] => {
                let index_meta = decode_as::<IndexMeta>(key, &value.data)?;
                if self.has_table_primary_record(index_meta.table_id) {
                    return Ok(None);
                }
                Some(format!(
                    "snapshot orphan index key with missing table id {}",
                    index_meta.table_id
                ))
            }
            ["__fd_marked_deleted_index", table_id, index_id] => {
                let table_id = parse_u64_segment(key, table_id)?;
                let index_id = parse_u64_segment(key, index_id)?;
                if self.has_table_primary_record(table_id)
                    || self.has_index_primary_record(index_id)
                {
                    return Ok(None);
                }
                Some(format!(
                    "snapshot orphan deleted index key with missing table id {table_id} and index id {index_id}"
                ))
            }
            ["__fd_marked_deleted_table_index", table_id, ..] => {
                let table_id = parse_u64_segment(key, table_id)?;
                if self.has_table_primary_record(table_id) {
                    return Ok(None);
                }
                Some(format!(
                    "snapshot orphan deleted table index key with missing table id {table_id}"
                ))
            }
            ["__fd_table_copied_file_lock", table_id] | ["__fd_table_lvt", table_id] => {
                let table_id = parse_u64_segment(key, table_id)?;
                if self.has_table_primary_record(table_id) {
                    return Ok(None);
                }
                Some(format!(
                    "snapshot orphan table auxiliary key with missing table id {table_id}"
                ))
            }
            ["__fd_table_copied_files", table_id, ..] | ["__fd_table_tag", table_id, ..] => {
                let table_id = parse_u64_segment(key, table_id)?;
                if self.has_table_primary_record(table_id) {
                    return Ok(None);
                }
                Some(format!(
                    "snapshot orphan table auxiliary key with missing table id {table_id}"
                ))
            }
            _ => None,
        };

        Ok(reason)
    }

    fn drain_mark_queue(&mut self) -> anyhow::Result<()> {
        while let Some((key, decision)) = self.mark_queue.pop_front() {
            let children = self.dependency_keys(&key)?;
            for child in children {
                if child.is_required() {
                    if decision == Decision::Keep {
                        let reason = format!("referenced by {key}");
                        self.mark_required_key(&child.key, decision, reason)?;
                    } else {
                        let reason = format!(
                            "referenced by dropped key {key}; missing dropped dependencies are allowed"
                        );
                        self.mark_optional_key(&child.key, decision, reason)?;
                    }
                    continue;
                }

                let optional_reason = child
                    .optional_reason
                    .expect("optional dependency must have a reason");
                let reason = format!("referenced by {key}; optional because {optional_reason}");
                self.mark_optional_key(&child.key, decision, reason)?;
            }
        }

        Ok(())
    }

    fn write_filtered<W>(&self, mut writer: W) -> anyhow::Result<()>
    where W: Write {
        for line in &self.header_lines {
            writeln!(writer, "{line}")?;
        }

        for state_line in &self.state_lines {
            let mark = state_line.mark.as_ref().expect("mark checked before write");
            if mark.decision == Decision::Keep {
                writeln!(writer, "{}", state_line.line)?;
            }
        }

        Ok(())
    }

    fn update_report(&mut self) {
        self.report.kept_state_machine_lines = self
            .state_lines
            .iter()
            .filter(|line| {
                line.mark
                    .as_ref()
                    .is_some_and(|mark| mark.decision == Decision::Keep)
            })
            .count();

        self.report.dropped_state_machine_lines = self
            .state_lines
            .iter()
            .filter(|line| {
                line.mark
                    .as_ref()
                    .is_some_and(|mark| mark.decision == Decision::Drop)
            })
            .count();
    }

    fn assert_every_state_line_marked(&self) -> anyhow::Result<()> {
        let unmarked = self
            .state_lines
            .iter()
            .filter(|line| line.mark.is_none())
            .take(20)
            .map(|line| format!("line {}: {}", line.line_no, line.display_key()))
            .collect::<Vec<_>>();

        if !unmarked.is_empty() {
            anyhow::bail!(
                "tenant dump filter did not mark all state machine entries; examples: {}",
                unmarked.join("; ")
            );
        }

        Ok(())
    }

    fn mark_required_key(
        &mut self,
        key: &str,
        decision: Decision,
        reason: impl Into<String>,
    ) -> anyhow::Result<bool> {
        let reason = reason.into();
        let Some(index) = self.key_to_state.get(key).copied() else {
            anyhow::bail!("required key not found: {key}; reason: {reason}");
        };

        self.mark_existing_key(index, key, decision, reason)
    }

    fn mark_optional_key(
        &mut self,
        key: &str,
        decision: Decision,
        reason: impl Into<String>,
    ) -> anyhow::Result<bool> {
        let Some(index) = self.key_to_state.get(key).copied() else {
            return Ok(false);
        };

        self.mark_existing_key(index, key, decision, reason.into())
    }

    fn mark_existing_key(
        &mut self,
        index: usize,
        key: &str,
        decision: Decision,
        reason: String,
    ) -> anyhow::Result<bool> {
        let marked = self.mark_line_by_index(index, decision, reason)?;
        if marked {
            self.mark_expires_for_key(key, decision, format!("expire of {key}"))?;
        }

        Ok(marked)
    }

    fn mark_line_by_index(
        &mut self,
        index: usize,
        decision: Decision,
        reason: impl Into<String>,
    ) -> anyhow::Result<bool> {
        let reason = reason.into();
        let state_line = &mut self.state_lines[index];

        match &state_line.mark {
            Some(mark) if mark.decision == decision => return Ok(false),
            Some(mark) => {
                anyhow::bail!(
                    "conflicting tenant filter marks for line {} ({}): existing {} because {}; new {} because {}",
                    state_line.line_no,
                    state_line.display_key(),
                    mark.decision.as_str(),
                    mark.reason,
                    decision.as_str(),
                    reason
                );
            }
            None => {}
        }

        state_line.mark = Some(Mark { decision, reason });

        if let StateKind::GenericKV { key, .. } = &state_line.kind {
            self.mark_queue.push_back((key.clone(), decision));
        }

        Ok(true)
    }

    fn mark_expires_for_key(
        &mut self,
        key: &str,
        decision: Decision,
        reason: impl Into<String>,
    ) -> anyhow::Result<()> {
        let Some(indices) = self.expires_by_key.get(key).cloned() else {
            return Ok(());
        };

        let reason = reason.into();
        for index in indices {
            self.mark_line_by_index(index, decision, reason.clone())?;
        }

        Ok(())
    }

    fn dependency_keys(&self, key: &str) -> anyhow::Result<Vec<DependencyKey>> {
        let Some(index) = self.key_to_state.get(key).copied() else {
            anyhow::bail!("marked key is missing from state index: {key}");
        };

        let StateKind::GenericKV { value, .. } = &self.state_lines[index].kind else {
            anyhow::bail!(
                "marked key is not a GenericKV state entry at line {}: {}",
                self.state_lines[index].line_no,
                self.state_lines[index].display_key()
            );
        };
        let data = &value.data;

        let mut out = DependencyKeySet::default();
        let segments = split_key(key);

        match segments.as_slice() {
            ["__fd_database", _tenant, _db_name] => {
                let db_id = decode_json_u64(key, data)?;
                self.add_database_id_keys(db_id, &mut out);
            }
            ["__fd_db_id_list", _tenant, _db_name] => {
                let ids = decode_as::<DbIdList>(key, data)?;
                for db_id in ids.id_list {
                    self.add_database_id_keys(db_id, &mut out);
                }
            }
            ["__fd_table", db_id, _table_name] => {
                let _ = parse_u64_segment(key, db_id)?;
                let table_id = decode_json_u64(key, data)?;
                self.add_table_id_keys(table_id, &mut out);
            }
            ["__fd_table_id_list", db_id, _table_name] => {
                let _ = parse_u64_segment(key, db_id)?;
                let ids = decode_as::<TableIdList>(key, data)?;
                for table_id in ids.id_list {
                    self.add_table_id_keys(table_id, &mut out);
                }
            }
            ["__fd_users", tenant, _user] => {
                let info = decode_pb_or_json::<UserInfo>(key, data)?;
                self.add_grant_keys(tenant, &info.grants, &mut out)?;
            }
            ["__fd_roles", tenant, _role] => {
                let info = decode_pb_or_json::<RoleInfo>(key, data)?;
                self.add_grant_keys(tenant, &info.grants, &mut out)?;
            }
            ["__fd_object_owners", tenant, rest @ ..] => {
                self.add_ownership_object_keys(tenant, rest, &mut out)?;
            }
            ["__fd_datamask", tenant, _name] => {
                let id = decode_json_u64(key, data)?;
                self.add_data_mask_id_keys(tenant, id, &mut out);
            }
            ["__fd_datamask_id_list", tenant, _name] => {
                let ids = decode_as::<MaskpolicyTableIdList>(key, data)?;
                for table_id in ids.id_list {
                    self.add_table_id_keys(table_id, &mut out);
                }
                let _ = tenant;
            }
            ["__fd_row_access_policy", tenant, _name] => {
                let id = decode_json_u64(key, data)?;
                self.add_row_access_policy_id_keys(tenant, id, &mut out);
            }
            ["__fd_index", tenant, _name] => {
                let id = decode_json_u64(key, data)?;
                self.add_index_id_keys(tenant, id, &mut out);
            }
            ["__fd_catalog", tenant, _name] => {
                let id = decode_json_u64(key, data)?;
                self.add_catalog_id_keys(tenant, id, &mut out);
            }
            ["__fd_background_job", _tenant, _name] => {
                let id = decode_json_u64(key, data)?;
                self.add_background_job_id_keys(id, &mut out);
            }
            ["__fd_procedure", _tenant, _name, ..] => {
                let id = decode_json_u64(key, data)?;
                self.add_procedure_id_keys(id, &mut out);
            }
            ["__fd_share", tenant, _name] => {
                let id = decode_json_u64(key, data)?;
                self.add_share_id_keys(tenant, id, &mut out);
            }
            ["__fd_index_by_id", index_id] => {
                let index_id = parse_u64_segment(key, index_id)?;
                let index_meta = decode_as::<IndexMeta>(key, data)?;
                self.add_index_id_keys("", index_id, &mut out);
                self.add_table_id_keys(index_meta.table_id, &mut out);
            }
            _ => {}
        }

        Ok(out.into_vec())
    }

    fn add_grant_keys(
        &self,
        tenant: &str,
        grants: &UserGrantSet,
        out: &mut DependencyKeySet,
    ) -> anyhow::Result<()> {
        for role in grants.roles() {
            let key = format!("__fd_roles/{tenant}/{role}");
            if is_builtin_role(role) {
                out.optional(
                    key,
                    "built-in roles are constructed outside persisted role records",
                );
            } else {
                out.required(key);
            }
        }

        for entry in grants.entries() {
            self.add_grant_object_keys(tenant, entry.object(), out)?;
        }

        Ok(())
    }

    fn add_grant_object_keys(
        &self,
        tenant: &str,
        object: &GrantObject,
        out: &mut DependencyKeySet,
    ) -> anyhow::Result<()> {
        match object {
            GrantObject::Global => {}
            GrantObject::Database(_, db_name) => {
                out.required(format!("__fd_database/{tenant}/{db_name}"));
                out.required(format!("__fd_db_id_list/{tenant}/{db_name}"));
            }
            GrantObject::DatabaseById(_, db_id) => {
                self.add_database_id_keys(*db_id, out);
            }
            GrantObject::Table(_, db_name, table_name) => {
                self.add_table_name_keys_for_grant(tenant, db_name, table_name, out)?;
            }
            GrantObject::TableById(_, db_id, table_id) => {
                self.add_database_id_keys(*db_id, out);
                self.add_table_id_keys(*table_id, out);
            }
            GrantObject::UDF(name) => {
                out.required(format!("__fd_udfs/{tenant}/{name}"));
            }
            GrantObject::Stage(name) => {
                out.required(format!("__fd_stages/{tenant}/{name}"));
                self.add_existing_keys_with_prefix(
                    &format!("__fd_stage_files/{tenant}/{name}/"),
                    None,
                    out,
                );
            }
            GrantObject::Warehouse(id) => {
                out.required(format!("__fd_warehouses/v1/{tenant}/{id}"));
                self.add_existing_keys_with_prefix(
                    &format!("__fd_clusters_v6/{tenant}/online_clusters/{id}/"),
                    None,
                    out,
                );
            }
            GrantObject::Connection(name) => {
                out.required(format!("__fd_connection/{tenant}/{name}"));
            }
            GrantObject::Sequence(name) => {
                out.required(format!("__fd_sequence/{tenant}/{name}"));
            }
            GrantObject::Procedure(id) => {
                self.add_procedure_id_keys(*id, out);
            }
            GrantObject::MaskingPolicy(id) => {
                self.add_data_mask_id_keys(tenant, *id, out);
            }
            GrantObject::RowAccessPolicy(id) => {
                self.add_row_access_policy_id_keys(tenant, *id, out);
            }
        }

        Ok(())
    }

    fn add_table_name_keys_for_grant(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
        out: &mut DependencyKeySet,
    ) -> anyhow::Result<()> {
        let db_key = format!("__fd_database/{tenant}/{db_name}");
        out.required(db_key.clone());
        out.required(format!("__fd_db_id_list/{tenant}/{db_name}"));

        let Some(index) = self.key_to_state.get(&db_key).copied() else {
            return Ok(());
        };

        let StateKind::GenericKV { value, .. } = &self.state_lines[index].kind else {
            anyhow::bail!(
                "indexed key is not a GenericKV state entry at line {}: {}",
                self.state_lines[index].line_no,
                self.state_lines[index].display_key()
            );
        };

        let db_id = decode_json_u64(&db_key, &value.data)?;
        out.required(format!("__fd_table/{db_id}/{table_name}"));
        out.required(format!("__fd_table_id_list/{db_id}/{table_name}"));

        Ok(())
    }

    fn add_ownership_object_keys(
        &self,
        tenant: &str,
        rest: &[&str],
        out: &mut DependencyKeySet,
    ) -> anyhow::Result<()> {
        match rest {
            ["database-by-id", db_id] => {
                self.add_database_id_keys(parse_u64_segment("__fd_object_owners", db_id)?, out);
            }
            ["database-by-catalog-id", _catalog, db_id] => {
                self.add_database_id_keys(parse_u64_segment("__fd_object_owners", db_id)?, out);
            }
            ["table-by-id", table_id] => {
                self.add_table_id_keys(parse_u64_segment("__fd_object_owners", table_id)?, out);
            }
            ["table-by-catalog-id", _catalog, table_id] => {
                self.add_table_id_keys(parse_u64_segment("__fd_object_owners", table_id)?, out);
            }
            ["stage-by-name", name] => {
                out.required(format!("__fd_stages/{tenant}/{name}"));
            }
            ["udf-by-name", name] => {
                out.required(format!("__fd_udfs/{tenant}/{name}"));
            }
            ["warehouse-by-id", id] => {
                out.required(format!("__fd_warehouses/v1/{tenant}/{id}"));
                self.add_existing_keys_with_prefix(
                    &format!("__fd_clusters_v6/{tenant}/online_clusters/{id}/"),
                    None,
                    out,
                );
            }
            ["connection-by-name", name] => {
                out.required(format!("__fd_connection/{tenant}/{name}"));
            }
            ["sequence-by-name", name] => {
                out.required(format!("__fd_sequence/{tenant}/{name}"));
            }
            ["procedure-by-id", id] => {
                self.add_procedure_id_keys(parse_u64_segment("__fd_object_owners", id)?, out);
            }
            ["masking-policy-by-id", id] => {
                self.add_data_mask_id_keys(
                    tenant,
                    parse_u64_segment("__fd_object_owners", id)?,
                    out,
                );
            }
            ["row-access-policy-by-id", id] => {
                self.add_row_access_policy_id_keys(
                    tenant,
                    parse_u64_segment("__fd_object_owners", id)?,
                    out,
                );
            }
            _ => {}
        }

        Ok(())
    }

    fn add_database_id_keys(&self, db_id: u64, out: &mut DependencyKeySet) {
        out.required(format!("__fd_database_by_id/{db_id}"));
        out.required(format!("__fd_database_id_to_name/{db_id}"));
        self.add_existing_keys_with_prefix(&format!("__fd_table/{db_id}/"), None, out);
        self.add_existing_keys_with_prefix(&format!("__fd_table_id_list/{db_id}/"), None, out);
        self.add_existing_keys_with_prefix(
            &format!("__fd_dictionaries_by_database/{db_id}/"),
            None,
            out,
        );
        self.add_existing_keys_with_prefix(&format!("__fd_share_by/db/{db_id}"), None, out);
    }

    fn add_table_id_keys(&self, table_id: u64, out: &mut DependencyKeySet) {
        out.required(format!("__fd_table_by_id/{table_id}"));
        out.required(format!("__fd_table_id_to_name/{table_id}"));
        self.add_existing_key(format!("__fd_table_lvt/{table_id}"), out);
        self.add_existing_keys_with_prefix(
            &format!("__fd_table_copied_files/{table_id}/"),
            None,
            out,
        );
        self.add_existing_key(format!("__fd_table_copied_file_lock/{table_id}"), out);
        self.add_existing_keys_with_prefix(&format!("__fd_table_tag/{table_id}/"), None, out);
        self.add_existing_keys_with_prefix(
            &format!("__fd_marked_deleted_table_index/{table_id}/"),
            None,
            out,
        );
        self.add_existing_keys_with_prefix(
            &format!("__fd_marked_deleted_index/{table_id}/"),
            None,
            out,
        );
        if let Some(index_ids) = self.index_ids_by_table_id.get(&table_id) {
            for index_id in index_ids {
                self.add_index_id_keys("", *index_id, out);
            }
        }
    }

    fn add_data_mask_id_keys(&self, tenant: &str, id: u64, out: &mut DependencyKeySet) {
        out.required(format!("__fd_datamask_by_id/{id}"));
        out.required(format!("__fd_datamask_id_to_name/{tenant}/{id}"));
        self.add_existing_keys_with_prefix(
            &format!("__fd_mask_policy_apply_table_id/{tenant}/{id}/"),
            None,
            out,
        );
    }

    fn add_row_access_policy_id_keys(&self, tenant: &str, id: u64, out: &mut DependencyKeySet) {
        out.required(format!("__fd_row_access_policy_by_id/{tenant}/{id}"));
        out.required(format!("__fd_row_access_policy_id_to_name/{tenant}/{id}"));
        self.add_existing_keys_with_prefix(
            &format!("__fd_row_access_policy_apply_table_id/{tenant}/{id}/"),
            None,
            out,
        );
    }

    fn add_index_id_keys(&self, tenant: &str, id: u64, out: &mut DependencyKeySet) {
        let _ = tenant;
        out.required(format!("__fd_index_by_id/{id}"));
        out.required(format!("__fd_index_id_to_name/{id}"));
        let deleted_index_suffix = format!("/{id}");
        self.add_existing_keys_with_prefix(
            "__fd_marked_deleted_index/",
            Some(&deleted_index_suffix),
            out,
        );
    }

    fn add_catalog_id_keys(&self, tenant: &str, id: u64, out: &mut DependencyKeySet) {
        let _ = tenant;
        out.required(format!("__fd_catalog_by_id/{id}"));
        out.required(format!("__fd_catalog_id_to_name/{id}"));
    }

    fn add_procedure_id_keys(&self, id: u64, out: &mut DependencyKeySet) {
        out.required(format!("__fd_procedure_by_id/{id}"));
        out.required(format!("__fd_procedure_id_to_name/{id}"));
    }

    fn add_background_job_id_keys(&self, id: u64, out: &mut DependencyKeySet) {
        out.required(format!("__fd_background_job_by_id/{id}"));
    }

    fn add_share_id_keys(&self, tenant: &str, id: u64, out: &mut DependencyKeySet) {
        let _ = tenant;
        out.required(format!("__fd_share_id/{id}"));
        out.required(format!("__fd_share_id_to_name/{id}"));
    }

    fn has_database_primary_record(&self, db_id: u64) -> bool {
        self.key_to_state
            .contains_key(&format!("__fd_database_by_id/{db_id}"))
            || self
                .key_to_state
                .contains_key(&format!("__fd_database_id_to_name/{db_id}"))
    }

    fn has_table_primary_record(&self, table_id: u64) -> bool {
        self.key_to_state
            .contains_key(&format!("__fd_table_by_id/{table_id}"))
            || self
                .key_to_state
                .contains_key(&format!("__fd_table_id_to_name/{table_id}"))
    }

    fn has_index_primary_record(&self, index_id: u64) -> bool {
        self.key_to_state
            .contains_key(&format!("__fd_index_by_id/{index_id}"))
            || self
                .key_to_state
                .contains_key(&format!("__fd_index_id_to_name/{index_id}"))
    }

    fn add_existing_keys_with_prefix(
        &self,
        prefix: &str,
        suffix: Option<&str>,
        out: &mut DependencyKeySet,
    ) {
        // These dependencies are discovered from the in-memory index, so every
        // emitted key is known to exist and can be required.
        for key in self
            .key_to_state
            .range(prefix.to_string()..)
            .map(|(key, _)| key)
            .take_while(|key| key.starts_with(prefix))
        {
            if suffix.is_none_or(|suffix| key.ends_with(suffix)) {
                out.required(key.clone());
            }
        }
    }

    fn add_existing_key(&self, key: String, out: &mut DependencyKeySet) {
        if self.key_to_state.contains_key(&key) {
            out.required(key);
        }
    }
}

pub fn filter_tenant_dump<R, W>(
    reader: R,
    writer: W,
    options: TenantFilterOptions,
) -> anyhow::Result<TenantFilterReport>
where
    R: BufRead,
    W: Write,
{
    let mut dump = TenantDump::load(reader)?;
    dump.mark_all(&options.tenant)?;
    dump.write_filtered(writer)?;
    Ok(dump.report)
}

fn classify_root(key: &str, tenant: &str) -> Option<Mark> {
    let segments = split_key(key);
    let first = *segments.first()?;

    if first == "_txn_id" {
        return Some(Mark {
            decision: Decision::Drop,
            reason: "transient idempotent transaction key".to_string(),
        });
    }

    if segments
        .get(1)
        .copied()
        .is_some_and(|segment| segment.starts_with("history_log_"))
    {
        return mark_by_tenant_segment(key, tenant, Some(first));
    }

    if first == "__fd_explain_perf" {
        return Some(Mark {
            decision: Decision::Keep,
            reason: "global explain perf key".to_string(),
        });
    }

    if first == "__fd_queries_queue" {
        return Some(Mark {
            decision: Decision::Drop,
            reason: "transient query queue key".to_string(),
        });
    }

    if first == "__fd_id_gen" {
        return Some(Mark {
            decision: Decision::Keep,
            reason: "global id generator".to_string(),
        });
    }

    if first == "__fd_warehouses" && segments.get(1).copied() == Some("v1") {
        return mark_by_tenant_segment(key, tenant, segments.get(2).copied());
    }

    if first.starts_with("__fd_clusters") {
        return mark_by_tenant_segment(key, tenant, segments.get(1).copied());
    }

    if TENANT_SCOPED_PREFIXES.contains(&first) {
        return mark_by_tenant_segment(key, tenant, segments.get(1).copied());
    }

    None
}

fn mark_by_tenant_segment(key: &str, target: &str, tenant_segment: Option<&str>) -> Option<Mark> {
    let key_tenant = tenant_segment?;
    let decision = if key_tenant == target {
        Decision::Keep
    } else {
        Decision::Drop
    };

    Some(Mark {
        decision,
        reason: format!("tenant root {key_tenant} for {key}"),
    })
}

fn split_key(key: &str) -> Vec<&str> {
    key.split('/').collect()
}

fn parse_u64_segment(context: &str, segment: &str) -> anyhow::Result<u64> {
    segment
        .parse::<u64>()
        .with_context(|| format!("failed to parse u64 segment {segment:?} in {context}"))
}

fn decode_json_u64(key: &str, data: &[u8]) -> anyhow::Result<u64> {
    serde_json::from_slice::<u64>(data)
        .with_context(|| format!("failed to decode JSON u64 value for key {key}"))
}

fn is_builtin_role(role: &str) -> bool {
    matches!(role, BUILTIN_ROLE_ACCOUNT_ADMIN | BUILTIN_ROLE_PUBLIC)
}

fn decode_as<T>(key: &str, data: &[u8]) -> anyhow::Result<T>
where
    T: FromToProto,
    T::PB: prost::Message + Default,
{
    decode_pb::<T>(data).with_context(|| {
        format!(
            "failed to decode protobuf value for key {} as {}",
            key,
            TypeName::<T>::new()
        )
    })
}

fn decode_pb_or_json<T>(key: &str, data: &[u8]) -> anyhow::Result<T>
where
    T: FromToProto + DeserializeOwned,
    T::PB: prost::Message + Default,
{
    match decode_pb::<T>(data) {
        Ok(value) => Ok(value),
        Err(pb_err) => serde_json::from_slice::<T>(data).with_context(|| {
            format!(
                "failed to decode value for key {} as protobuf or JSON {}; protobuf error: {}",
                key,
                TypeName::<T>::new(),
                pb_err
            )
        }),
    }
}

struct TypeName<T>(std::marker::PhantomData<T>);

impl<T> TypeName<T> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> fmt::Display for TypeName<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", std::any::type_name::<T>())
    }
}

const TENANT_SCOPED_PREFIXES: &[&str] = &[
    "__fd_autoincrement_storage",
    "__fd_background_job",
    "__fd_catalog",
    "__fd_connection",
    "__fd_database",
    "__fd_datamask",
    "__fd_datamask_id_list",
    "__fd_db_id_list",
    "__fd_dictionaries",
    "__fd_file_formats",
    "__fd_index",
    "__fd_mask_policy_apply_table_id",
    "__fd_materialized_view_by_source",
    "__fd_materialized_view_definition",
    "__fd_network_policies",
    "__fd_object_owners",
    "__fd_object_tag_ref",
    "__fd_password_policies",
    "__fd_procedure",
    "__fd_quotas",
    "__fd_roles",
    "__fd_row_access_policy",
    "__fd_row_access_policy_apply_table_id",
    "__fd_row_access_policy_by_id",
    "__fd_row_access_policy_id_to_name",
    "__fd_sequence",
    "__fd_sequence_storage",
    "__fd_session",
    "__fd_settings",
    "__fd_share",
    "__fd_stage_files",
    "__fd_stages",
    "__fd_table_count",
    "__fd_table_lock",
    "__fd_tag",
    "__fd_tag_by_id",
    "__fd_tag_id_to_name",
    "__fd_tag_object_ref",
    "__fd_tasks",
    "__fd_task_messages",
    "__fd_tenant",
    "__fd_token",
    "__fd_udfs",
    "__fd_users",
    "__fd_vacuum_watermark_ts",
    "__fd_virtual_column",
];

#[cfg(test)]
mod tests {
    use databend_common_meta_api::kv_pb_api;
    use databend_common_meta_app::schema::DatabaseMeta;
    use databend_common_meta_app::schema::IndexNameIdentRaw;
    use databend_common_meta_app::schema::TableMeta;
    use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdentRaw;
    use databend_meta::raft_store::key_spaces::RaftStoreEntry;
    use databend_meta::types::SeqV;

    use super::*;

    fn generic_kv_line(key: &str, data: Vec<u8>) -> String {
        let entry = RaftStoreEntry::GenericKV {
            key: key.to_string(),
            value: SeqV::new(1, data),
        };
        serde_json::to_string(&("state_machine/0", entry)).unwrap()
    }

    fn expire_line(key: &str) -> String {
        format!(
            r#"["state_machine/0",{{"Expire":{{"key":{{"time_ms":1000,"seq":1}},"value":{{"seq":1,"key":{}}}}}}}]"#,
            serde_json::to_string(key).unwrap()
        )
    }

    fn encode_pb<T>(value: &T) -> Vec<u8>
    where T: FromToProto {
        kv_pb_api::encode_pb(value)
    }

    fn filter(input: Vec<String>, tenant: &str) -> anyhow::Result<(TenantFilterReport, String)> {
        let mut output = Vec::new();
        let report = filter_tenant_dump(
            input.join("\n").as_bytes(),
            &mut output,
            TenantFilterOptions {
                tenant: tenant.to_string(),
            },
        )?;
        Ok((report, String::from_utf8(output).unwrap()))
    }

    fn user_with_grant(object: GrantObject) -> UserInfo {
        let mut info = UserInfo::new_no_auth("u1", "%");
        info.grants
            .grant_privileges(&object, object.available_privileges(false));
        info
    }

    #[test]
    fn test_filter_database_table_graph() -> anyhow::Result<()> {
        let input = vec![
            generic_kv_line("__fd_id_gen/table_id", b"10".to_vec()),
            generic_kv_line("__fd_database/tenant_a/default", b"11".to_vec()),
            generic_kv_line(
                "__fd_database_by_id/11",
                encode_pb(&DatabaseMeta::default()),
            ),
            generic_kv_line(
                "__fd_database_id_to_name/11",
                encode_pb(&DatabaseNameIdentRaw::new("tenant_a", "default")),
            ),
            generic_kv_line("__fd_table/11/t1", b"101".to_vec()),
            generic_kv_line("__fd_table_by_id/101", encode_pb(&TableMeta::default())),
            generic_kv_line(
                "__fd_table_id_to_name/101",
                encode_pb(&databend_common_meta_app::schema::DBIdTableName::new(
                    11, "t1",
                )),
            ),
            generic_kv_line("__fd_database/tenant_b/default", b"22".to_vec()),
            generic_kv_line(
                "__fd_database_by_id/22",
                encode_pb(&DatabaseMeta::default()),
            ),
            generic_kv_line(
                "__fd_database_id_to_name/22",
                encode_pb(&DatabaseNameIdentRaw::new("tenant_b", "default")),
            ),
            expire_line("__fd_database/tenant_a/default"),
            expire_line("__fd_database/tenant_b/default"),
        ];

        let (report, output) = filter(input, "tenant_a")?;

        assert_eq!(report.kept_state_machine_lines, 8);
        assert_eq!(report.dropped_state_machine_lines, 4);
        assert!(output.contains("__fd_database/tenant_a/default"));
        assert!(output.contains("__fd_table_by_id/101"));
        assert!(!output.contains("__fd_database/tenant_b/default"));

        Ok(())
    }

    #[test]
    fn test_unmarked_generic_kv_is_an_error() {
        let input = vec![generic_kv_line("__unknown_unowned_key/101", vec![])];
        let err = filter(input, "tenant_a").unwrap_err();
        assert!(
            err.to_string()
                .contains("did not mark all state machine entries")
        );
    }

    #[test]
    fn test_unmarked_id_scoped_key_is_an_error() {
        let input = vec![generic_kv_line(
            "__fd_table_by_id/101",
            encode_pb(&TableMeta::default()),
        )];
        let err = filter(input, "tenant_a").unwrap_err();
        assert!(
            err.to_string()
                .contains("did not mark all state machine entries")
        );
    }

    #[test]
    fn test_orphan_expire_is_an_error() {
        let input = vec![expire_line("__fd_database/tenant_a/default")];

        let err = filter(input, "tenant_a").unwrap_err();
        assert!(
            err.to_string()
                .contains("did not mark all state machine entries")
        );
    }

    #[test]
    fn test_database_id_to_name_is_not_a_value_root() {
        let input = vec![
            generic_kv_line(
                "__fd_database_id_to_name/11",
                encode_pb(&DatabaseNameIdentRaw::new("tenant_a", "default")),
            ),
            generic_kv_line(
                "__fd_database_by_id/11",
                encode_pb(&DatabaseMeta::default()),
            ),
        ];

        let err = filter(input, "tenant_a").unwrap_err();
        assert!(
            err.to_string()
                .contains("did not mark all state machine entries")
        );
    }

    #[test]
    fn test_table_id_to_name_does_not_traverse_back_to_name_key() -> anyhow::Result<()> {
        let input = vec![
            generic_kv_line("__fd_database/tenant_a/default", b"11".to_vec()),
            generic_kv_line(
                "__fd_database_by_id/11",
                encode_pb(&DatabaseMeta::default()),
            ),
            generic_kv_line(
                "__fd_database_id_to_name/11",
                encode_pb(&DatabaseNameIdentRaw::new("tenant_a", "default")),
            ),
            generic_kv_line(
                "__fd_table_id_list/11/missing_name_key",
                encode_pb(&TableIdList::new_with_ids([101])),
            ),
            generic_kv_line("__fd_table_by_id/101", encode_pb(&TableMeta::default())),
            generic_kv_line(
                "__fd_table_id_to_name/101",
                encode_pb(&databend_common_meta_app::schema::DBIdTableName::new(
                    11,
                    "missing_name_key",
                )),
            ),
        ];

        let (report, output) = filter(input, "tenant_a")?;

        assert_eq!(report.kept_state_machine_lines, 6);
        assert!(output.contains("__fd_table_id_to_name/101"));
        assert!(!output.contains("__fd_table/11/missing_name_key"));

        Ok(())
    }

    #[test]
    fn test_index_is_traversed_from_table_meta() -> anyhow::Result<()> {
        let index_meta = IndexMeta {
            table_id: 101,
            ..Default::default()
        };

        let input = vec![
            generic_kv_line("__fd_database/tenant_a/default", b"11".to_vec()),
            generic_kv_line(
                "__fd_database_by_id/11",
                encode_pb(&DatabaseMeta::default()),
            ),
            generic_kv_line(
                "__fd_database_id_to_name/11",
                encode_pb(&DatabaseNameIdentRaw::new("tenant_a", "default")),
            ),
            generic_kv_line("__fd_table/11/t1", b"101".to_vec()),
            generic_kv_line("__fd_table_by_id/101", encode_pb(&TableMeta::default())),
            generic_kv_line(
                "__fd_table_id_to_name/101",
                encode_pb(&databend_common_meta_app::schema::DBIdTableName::new(
                    11, "t1",
                )),
            ),
            generic_kv_line("__fd_index_by_id/201", encode_pb(&index_meta)),
            generic_kv_line(
                "__fd_index_id_to_name/201",
                encode_pb(&IndexNameIdentRaw::new("tenant_a", "idx1")),
            ),
        ];

        let (report, output) = filter(input, "tenant_a")?;

        assert_eq!(report.kept_state_machine_lines, 8);
        assert!(output.contains("__fd_index_by_id/201"));
        assert!(output.contains("__fd_index_id_to_name/201"));

        Ok(())
    }

    #[test]
    fn test_orphan_table_name_key_is_dropped() -> anyhow::Result<()> {
        let input = vec![generic_kv_line("__fd_table/11/t1", b"101".to_vec())];

        let (report, output) = filter(input, "tenant_a")?;

        assert_eq!(report.kept_state_machine_lines, 0);
        assert_eq!(report.dropped_state_machine_lines, 1);
        assert!(output.is_empty());

        Ok(())
    }

    #[test]
    fn test_table_name_key_with_existing_database_primary_is_not_orphan_root() {
        let input = vec![
            generic_kv_line("__fd_table/11/t1", b"101".to_vec()),
            generic_kv_line(
                "__fd_database_by_id/11",
                encode_pb(&DatabaseMeta::default()),
            ),
        ];

        let err = filter(input, "tenant_a").unwrap_err();
        assert!(
            err.to_string()
                .contains("did not mark all state machine entries")
        );
    }

    #[test]
    fn test_table_name_key_with_only_table_primary_is_orphan_root() -> anyhow::Result<()> {
        let input = vec![
            generic_kv_line("__fd_table/11/t1", b"101".to_vec()),
            generic_kv_line("__fd_table_by_id/101", encode_pb(&TableMeta::default())),
        ];

        let (report, output) = filter(input, "tenant_a")?;

        assert_eq!(report.kept_state_machine_lines, 0);
        assert_eq!(report.dropped_state_machine_lines, 2);
        assert!(output.is_empty());

        Ok(())
    }

    #[test]
    fn test_orphan_deleted_index_key_is_dropped() -> anyhow::Result<()> {
        let input = vec![generic_kv_line("__fd_marked_deleted_index/101/201", vec![])];

        let (report, output) = filter(input, "tenant_a")?;

        assert_eq!(report.kept_state_machine_lines, 0);
        assert_eq!(report.dropped_state_machine_lines, 1);
        assert!(output.is_empty());

        Ok(())
    }

    #[test]
    fn test_orphan_table_auxiliary_key_is_dropped() -> anyhow::Result<()> {
        let input = vec![generic_kv_line("__fd_table_lvt/101", vec![])];

        let (report, output) = filter(input, "tenant_a")?;

        assert_eq!(report.kept_state_machine_lines, 0);
        assert_eq!(report.dropped_state_machine_lines, 1);
        assert!(output.is_empty());

        Ok(())
    }

    #[test]
    fn test_table_auxiliary_key_with_existing_primary_is_not_orphan_root() {
        let input = vec![
            generic_kv_line("__fd_table_by_id/101", encode_pb(&TableMeta::default())),
            generic_kv_line("__fd_table_lvt/101", vec![]),
        ];

        let err = filter(input, "tenant_a").unwrap_err();
        assert!(
            err.to_string()
                .contains("did not mark all state machine entries")
        );
    }

    #[test]
    fn test_orphan_index_by_id_is_dropped_with_reverse_name() -> anyhow::Result<()> {
        let index_meta = IndexMeta {
            table_id: 101,
            ..Default::default()
        };

        let input = vec![
            generic_kv_line("__fd_index_by_id/201", encode_pb(&index_meta)),
            generic_kv_line(
                "__fd_index_id_to_name/201",
                encode_pb(&IndexNameIdentRaw::new("tenant_a", "idx1")),
            ),
        ];

        let (report, output) = filter(input, "tenant_a")?;

        assert_eq!(report.kept_state_machine_lines, 0);
        assert_eq!(report.dropped_state_machine_lines, 2);
        assert!(output.is_empty());

        Ok(())
    }

    #[test]
    fn test_index_by_id_with_existing_table_primary_is_not_orphan_root() {
        let index_meta = IndexMeta {
            table_id: 101,
            ..Default::default()
        };

        let input = vec![
            generic_kv_line("__fd_table_by_id/101", encode_pb(&TableMeta::default())),
            generic_kv_line("__fd_index_by_id/201", encode_pb(&index_meta)),
        ];

        let err = filter(input, "tenant_a").unwrap_err();
        assert!(
            err.to_string()
                .contains("did not mark all state machine entries")
        );
    }

    #[test]
    fn test_missing_required_dependency_is_an_error() {
        let input = vec![generic_kv_line(
            "__fd_database/tenant_a/default",
            b"11".to_vec(),
        )];
        let err = filter(input, "tenant_a").unwrap_err();
        assert!(
            err.to_string()
                .contains("required key not found: __fd_database_by_id/11")
        );
    }

    #[test]
    fn test_grant_sequence_object_is_required() {
        let input = vec![generic_kv_line(
            "__fd_users/tenant_a/u1",
            encode_pb(&user_with_grant(GrantObject::Sequence("s1".to_string()))),
        )];

        let err = filter(input, "tenant_a").unwrap_err();
        assert!(
            err.to_string()
                .contains("required key not found: __fd_sequence/tenant_a/s1")
        );
    }

    #[test]
    fn test_granted_builtin_role_is_optional() -> anyhow::Result<()> {
        let mut info = UserInfo::new_no_auth("u1", "%");
        info.grants.grant_role("account_admin".to_string());
        info.grants.grant_role("public".to_string());

        let input = vec![generic_kv_line("__fd_users/tenant_a/u1", encode_pb(&info))];

        let (report, output) = filter(input, "tenant_a")?;

        assert_eq!(report.kept_state_machine_lines, 1);
        assert!(output.contains("__fd_users/tenant_a/u1"));

        Ok(())
    }

    #[test]
    fn test_granted_regular_role_is_required() {
        let mut info = UserInfo::new_no_auth("u1", "%");
        info.grants.grant_role("role1".to_string());

        let input = vec![generic_kv_line("__fd_users/tenant_a/u1", encode_pb(&info))];

        let err = filter(input, "tenant_a").unwrap_err();
        assert!(
            err.to_string()
                .contains("required key not found: __fd_roles/tenant_a/role1")
        );
    }

    #[test]
    fn test_missing_dropped_dependency_is_not_an_error() -> anyhow::Result<()> {
        let input = vec![generic_kv_line(
            "__fd_database/tenant_b/default",
            b"22".to_vec(),
        )];

        let (report, output) = filter(input, "tenant_a")?;

        assert_eq!(report.kept_state_machine_lines, 0);
        assert_eq!(report.dropped_state_machine_lines, 1);
        assert!(output.is_empty());

        Ok(())
    }
}
