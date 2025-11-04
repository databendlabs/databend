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

use std::collections::HashSet;

use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_proto_conv::FromToProto;

use crate::kv_pb_api::KVPbApi;

/// Upper bound for the number of stale bindings we try to clean up in a single transaction.
pub(crate) const MAX_POLICY_CLEANUP_BATCH: usize = 100;

/// Describes an update that needs to be applied to a table meta entry.
#[derive(Clone)]
pub(crate) struct PolicyTableUpdate {
    pub(crate) table_id: TableId,
    pub(crate) seq: u64,
    pub(crate) meta: TableMeta,
}

pub(crate) struct PolicyUsage<K> {
    pub(crate) active_tables: Vec<(u64, u64)>, // (table_id, seq)
    pub(crate) stale_bindings: Vec<(K, u64)>,  // (binding_key, seq)
    pub(crate) table_updates: Vec<PolicyTableUpdate>,
}

impl<K> Default for PolicyUsage<K> {
    fn default() -> Self {
        Self {
            active_tables: Vec::new(),
            stale_bindings: Vec::new(),
            table_updates: Vec::new(),
        }
    }
}

/// Represents the action to take when dropping a security policy.
/// This enum eliminates the need for a boolean flag to determine behavior.
pub(crate) enum PolicyDropAction<K> {
    /// Can drop the policy now - all stale bindings fit in one transaction.
    FinalDrop {
        prefix: DirName<K>,
        binding_count: u64,
        bindings: Vec<(K, u64)>,
        table_updates: Vec<PolicyTableUpdate>,
    },
    /// Need to cleanup a batch of stale bindings first, then retry.
    CleanupBatch {
        prefix: DirName<K>,
        binding_count: u64,
        bindings: Vec<(K, u64)>,
        table_updates: Vec<PolicyTableUpdate>,
    },
}

impl<K> PolicyUsage<K> {
    pub(crate) fn binding_count(&self) -> u64 {
        (self.active_tables.len() + self.stale_bindings.len()) as u64
    }

    pub(crate) fn prepare_drop_action(
        self,
        tenant: &Tenant,
        policy_id: u64,
    ) -> PolicyDropAction<K>
    where
        K: PolicyBinding + Clone,
    {
        let prefix = K::prefix_for(tenant, policy_id);
        let binding_count = self.binding_count();

        if self.stale_bindings.len() > MAX_POLICY_CLEANUP_BATCH {
            let bindings: Vec<_> = self
                .stale_bindings
                .iter()
                .take(MAX_POLICY_CLEANUP_BATCH)
                .cloned()
                .collect();

            let touched_tables: HashSet<_> =
                bindings.iter().map(|(key, _seq)| key.table_id()).collect();
            let mut updates = Vec::new();
            let mut seen_tables = HashSet::new();
            for update in &self.table_updates {
                let table_id = update.table_id.table_id;
                if touched_tables.contains(&table_id) && seen_tables.insert(table_id) {
                    updates.push(update.clone());
                }
            }

            PolicyDropAction::CleanupBatch {
                prefix,
                binding_count,
                bindings,
                table_updates: updates,
            }
        } else {
            PolicyDropAction::FinalDrop {
                prefix,
                binding_count,
                bindings: self.stale_bindings,
                table_updates: self.table_updates,
            }
        }
    }
}

pub(crate) trait PolicyBinding: kvapi::Key + Sized {
    fn prefix_for(tenant: &Tenant, policy_id: u64) -> DirName<Self>;
    fn table_id(&self) -> u64;
    fn remove_security_policy_from_table_meta(meta: &mut TableMeta, policy_id: u64) -> bool;
}

/// Collects the usage information of a security policy by scanning table bindings.
///
/// # Concurrency Safety
///
/// This function performs non-transactional reads but ensures correctness through
/// optimistic concurrency control in the subsequent transaction:
///
/// 1. **Active tables are protected**: Records `(table_id, seq)` for all active tables.
///    The transaction will include `txn_cond_eq_seq` guards to detect if any active
///    table is modified (e.g., dropped) concurrently. If detected, transaction fails
///    and we retry with fresh state.
///
/// 2. **New bindings are detected**: Uses `txn_cond_eq_keys_with_prefix` to ensure
///    no new bindings are created between scanning and transaction execution.
///
/// 3. **Stale bindings cleanup is atomic**: All stale binding deletions and table meta
///    updates occur in a single transaction with proper seq guards.
///
/// The retry loop ensures eventual consistency even under high concurrency.
pub(crate) async fn collect_policy_usage<K>(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    tenant: &Tenant,
    policy_id: u64,
) -> Result<PolicyUsage<K>, MetaError>
where
    K: PolicyBinding + Clone + Send + Sync + 'static,
    K::ValueType: FromToProto + Send,
{
    let binding_prefix = K::prefix_for(tenant, policy_id);
    let bindings = kv_api.list_pb_vec(&binding_prefix).await?;

    let mut usage = PolicyUsage::default();

    for (binding_ident, seqv) in bindings {
        let table_id = binding_ident.table_id();
        let table_key = TableId::new(table_id);
        let binding_seq = seqv.seq;
        let table_meta_opt = kv_api.get_pb(&table_key).await?;
        match table_meta_opt {
            Some(SeqV { seq, data, .. }) if data.drop_on.is_none() => {
                usage.active_tables.push((table_id, seq));
            }
            Some(SeqV { seq, mut data, .. }) => {
                if K::remove_security_policy_from_table_meta(&mut data, policy_id) {
                    usage.table_updates.push(PolicyTableUpdate {
                        table_id: table_key,
                        seq,
                        meta: data,
                    });
                }
                usage.stale_bindings.push((binding_ident, binding_seq));
            }
            None => usage.stale_bindings.push((binding_ident, binding_seq)),
        }
    }

    Ok(usage)
}
