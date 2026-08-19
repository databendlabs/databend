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

use databend_common_expression::generate_like_pattern;
use databend_common_meta_app::data_share::DataShareMeta;
use databend_common_meta_app::tenant::Tenant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareShowEntry {
    pub created_on: String,
    pub kind: String,
    pub owner_account: String,
    pub name: String,
    pub database_name: String,
    pub to: String,
    pub owner: String,
    pub comment: String,
    pub listing_global_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareDescEntry {
    pub kind: String,
    pub name: String,
    pub shared_on: String,
}

pub(super) fn show_share_entries(
    tenant: &Tenant,
    metas: Vec<DataShareMeta>,
    like_pattern: Option<&str>,
    limit: Option<u64>,
) -> Vec<ShareShowEntry> {
    let mut rows = Vec::new();
    for meta in metas {
        let kind = if meta.provider == tenant.tenant_name() {
            "OUTBOUND"
        } else if meta.accounts.contains(tenant.tenant_name()) {
            "INBOUND"
        } else {
            continue;
        };
        if like_pattern.is_some_and(|pattern| !like_match(pattern, &meta.name)) {
            continue;
        }
        let mut accounts = meta.accounts.iter().cloned().collect::<Vec<_>>();
        accounts.sort();
        rows.push(ShareShowEntry {
            created_on: meta.created_on.to_rfc3339(),
            kind: kind.to_string(),
            owner_account: meta.provider,
            name: meta.name,
            database_name: meta
                .database
                .as_ref()
                .map(|grant| grant.database.clone())
                .unwrap_or_default(),
            to: if kind == "OUTBOUND" {
                accounts.join(", ")
            } else {
                String::new()
            },
            owner: String::new(),
            comment: meta.comment.unwrap_or_default(),
            listing_global_name: String::new(),
        });
    }
    rows.sort_by(|a, b| {
        a.kind
            .cmp(&b.kind)
            .then(a.owner_account.cmp(&b.owner_account))
            .then(a.name.cmp(&b.name))
    });
    if let Some(limit) = limit {
        rows.truncate(limit as usize);
    }
    rows
}

pub(super) fn describe_share_entries(meta: &DataShareMeta) -> Vec<ShareDescEntry> {
    let mut rows = Vec::new();
    if let Some(database) = &meta.database {
        rows.push(ShareDescEntry {
            kind: "DATABASE".to_string(),
            name: database.database.clone(),
            shared_on: database.shared_on.to_rfc3339(),
        });
    }
    rows.extend(
        meta.tables
            .iter()
            .map(|(table_name, table)| ShareDescEntry {
                kind: "TABLE".to_string(),
                name: format!(
                    "{}.{}",
                    meta.database
                        .as_ref()
                        .map(|grant| grant.database.as_str())
                        .unwrap_or_default(),
                    table_name
                ),
                shared_on: table.shared_on.to_rfc3339(),
            }),
    );
    rows
}

fn like_match(pattern: &str, value: &str) -> bool {
    generate_like_pattern(pattern.as_bytes(), value.len()).compare(value.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn show_shares_like_supports_sql_wildcards() {
        assert!(like_match("share_e2_", "share_e2e"));
        assert!(like_match("share%", "share_e2e"));
        assert!(!like_match("share_e2_", "share_e2"));
    }
}
