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

use databend_common_meta_app::schema::LineageColumn;
use databend_common_meta_app::schema::LineageDetail;
use databend_common_meta_app::schema::LineageDirection;
use databend_common_meta_app::schema::LineageIdent;
use databend_common_meta_app::schema::LineageIdentity;
use databend_common_meta_app::schema::LineageKey;
use databend_common_meta_app::schema::LineageObjectRef;
use databend_common_meta_app::schema::LineageObjectType;
use databend_common_meta_app::schema::LineageUpdate;
use databend_common_meta_app::schema::LineageUpdateMode;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi::DirName;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::ConditionResult::Eq;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::TxnRequest;
use fastrace::func_name;

use crate::kv_app_error::KVAppError;
use crate::kv_fetch_util::mget_pb_values;
use crate::kv_pb_api::KVPbApi;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_op_builder_util::txn_del;
use crate::txn_op_builder_util::txn_put_pb;

const LINEAGE_MERGE_MAX_RETRIES: u32 = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeLineageReq {
    pub updates: Vec<LineageUpdate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListLineageReq {
    pub tenant: Tenant,
    pub direction: LineageDirection,
    pub object: LineageObjectRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListLineageReply {
    pub entries: Vec<(LineageKey, LineageDetail)>,
}

pub(crate) fn append_replace_lineage_txn_ops<'a>(
    txn: &mut TxnRequest,
    updates: impl IntoIterator<Item = &'a LineageUpdate>,
) {
    for update in updates {
        debug_assert_eq!(update.mode, LineageUpdateMode::Replace);
        if update.mode != LineageUpdateMode::Replace {
            continue;
        }

        let idents = LineageEdgeIdents::from_update(update);
        txn.if_then
            .push(txn_put_pb(&idents.downstream, &update.detail));
        txn.if_then
            .push(txn_put_pb(&idents.upstream, &update.detail));
    }
}

pub(crate) async fn append_delete_lineage_for_table_id_txn_ops<KV>(
    kv: &KV,
    tenant: &Tenant,
    txn: &mut TxnRequest,
    table_id: u64,
) -> Result<(), MetaError>
where
    KV: KVPbApi<Error = MetaError> + ?Sized,
{
    let object = LineageObjectRef {
        object_type: LineageObjectType::Table,
        identity: LineageIdentity::Id {
            id: table_id.to_string(),
        },
    };

    append_delete_lineage_for_object_txn_ops(kv, tenant, txn, LineageDirection::Upstream, &object)
        .await?;
    append_delete_lineage_for_object_txn_ops(
        kv,
        tenant,
        txn,
        LineageDirection::Downstream,
        &object,
    )
    .await?;
    Ok(())
}

async fn append_delete_lineage_for_object_txn_ops<KV>(
    kv: &KV,
    tenant: &Tenant,
    txn: &mut TxnRequest,
    direction: LineageDirection,
    object: &LineageObjectRef,
) -> Result<(), MetaError>
where
    KV: KVPbApi<Error = MetaError> + ?Sized,
{
    let dir = lineage_object_dir(tenant.clone(), direction, object.clone());
    let entries = kv.list_pb_vec(ListOptions::unlimited(&dir)).await?;

    for (ident, _) in entries {
        let key = ident.name();
        let reverse_key = LineageKey {
            direction: reverse_direction(&key.direction),
            object: key.related_object.clone(),
            related_object: key.object.clone(),
        };
        let reverse_ident = LineageIdent::new_generic(tenant.clone(), reverse_key);
        txn.if_then.push(txn_del(&ident));
        txn.if_then.push(txn_del(&reverse_ident));
    }

    Ok(())
}

fn lineage_object_dir(
    tenant: Tenant,
    direction: LineageDirection,
    object: LineageObjectRef,
) -> DirName<LineageIdent> {
    let dummy_related = LineageObjectRef {
        object_type: LineageObjectType::Table,
        identity: LineageIdentity::Id {
            id: "0".to_string(),
        },
    };
    let prefix = LineageIdent::new_generic(tenant.clone(), LineageKey {
        direction,
        object,
        related_object: dummy_related,
    });
    // A related object is encoded as type / identity kind / identity.
    DirName::new_with_level(prefix, 3)
}

#[derive(Clone)]
struct LineageEdgeIdents {
    downstream: LineageIdent,
    upstream: LineageIdent,
}

impl LineageEdgeIdents {
    fn from_update(update: &LineageUpdate) -> Self {
        Self::new(
            Tenant::new_literal(&update.tenant_name),
            update.upstream.clone(),
            update.downstream.clone(),
        )
    }

    fn new(tenant: Tenant, upstream: LineageObjectRef, downstream: LineageObjectRef) -> Self {
        Self {
            downstream: LineageIdent::new_generic(tenant.clone(), LineageKey {
                direction: LineageDirection::Downstream,
                object: upstream.clone(),
                related_object: downstream.clone(),
            }),
            upstream: LineageIdent::new_generic(tenant, LineageKey {
                direction: LineageDirection::Upstream,
                object: downstream,
                related_object: upstream,
            }),
        }
    }
}

fn reverse_direction(direction: &LineageDirection) -> LineageDirection {
    match direction {
        LineageDirection::Upstream => LineageDirection::Downstream,
        LineageDirection::Downstream => LineageDirection::Upstream,
    }
}

fn merge_lineage_detail(
    existing: Option<LineageDetail>,
    incoming: &LineageDetail,
) -> LineageDetail {
    let Some(mut detail) = existing else {
        return incoming.clone();
    };

    detail.last_query_id = incoming.last_query_id.clone();
    detail.updated_on = incoming.updated_on;
    for column in &incoming.column_lineage {
        if !detail
            .column_lineage
            .iter()
            .any(|existing| lineage_column_eq(existing, column))
        {
            detail.column_lineage.push(column.clone());
        }
    }

    detail
}

fn lineage_column_eq(left: &LineageColumn, right: &LineageColumn) -> bool {
    left.upstream == right.upstream && left.downstream == right.downstream
}

#[async_trait::async_trait]
pub trait LineageApi: Send + Sync {
    async fn merge_lineage(&self, req: MergeLineageReq) -> Result<(), KVAppError>;

    async fn list_lineage(&self, req: ListLineageReq) -> Result<ListLineageReply, MetaError>;
}

#[async_trait::async_trait]
impl<KV: KVPbApi<Error = MetaError> + ?Sized> LineageApi for KV {
    async fn merge_lineage(&self, req: MergeLineageReq) -> Result<(), KVAppError> {
        let updates = aggregate_merge_updates(&req.updates);
        if updates.is_empty() {
            return Ok(());
        }

        let mut trials = txn_backoff(Some(LINEAGE_MERGE_MAX_RETRIES), func_name!());
        loop {
            trials.next().unwrap()?.await;
            let txn = build_merge_lineage_txn(self, &updates).await?;
            let (success, _) = send_txn(self, txn).await?;
            if success {
                return Ok(());
            }
        }
    }

    async fn list_lineage(&self, req: ListLineageReq) -> Result<ListLineageReply, MetaError> {
        let dir = lineage_object_dir(req.tenant, req.direction, req.object);
        let entries = self
            .list_pb_vec(ListOptions::unlimited(&dir))
            .await?
            .into_iter()
            .map(|(ident, seqv)| (ident.name().clone(), seqv.data))
            .collect();

        Ok(ListLineageReply { entries })
    }
}

struct AggregatedLineageUpdate {
    idents: LineageEdgeIdents,
    detail: LineageDetail,
}

fn aggregate_merge_updates(updates: &[LineageUpdate]) -> Vec<AggregatedLineageUpdate> {
    let mut aggregated = BTreeMap::new();

    for update in updates
        .iter()
        .filter(|update| update.mode == LineageUpdateMode::Merge)
    {
        let idents = LineageEdgeIdents::from_update(update);
        let key = idents.downstream.to_string_key();
        aggregated
            .entry(key)
            .and_modify(|existing: &mut AggregatedLineageUpdate| {
                existing.detail =
                    merge_lineage_detail(Some(existing.detail.clone()), &update.detail);
            })
            .or_insert_with(|| AggregatedLineageUpdate {
                idents,
                detail: update.detail.clone(),
            });
    }

    aggregated.into_values().collect()
}

async fn build_merge_lineage_txn<KV>(
    kv: &KV,
    updates: &[AggregatedLineageUpdate],
) -> Result<TxnRequest, KVAppError>
where
    KV: KVPbApi<Error = MetaError> + ?Sized,
{
    let mut merge_keys = Vec::with_capacity(updates.len() * 2);
    for update in updates {
        merge_keys.push(update.idents.downstream.to_string_key());
        merge_keys.push(update.idents.upstream.to_string_key());
    }

    let merge_values: Vec<(u64, Option<LineageDetail>)> = mget_pb_values(kv, &merge_keys).await?;
    let mut txn = TxnRequest::default();
    for (update, values) in updates.iter().zip(merge_values.chunks_exact(2)) {
        let (downstream_seq, downstream_detail) = values[0].clone();
        let (upstream_seq, _) = values[1].clone();
        let merged_detail = merge_lineage_detail(downstream_detail, &update.detail);

        txn.condition
            .push(txn_cond_seq(&update.idents.downstream, Eq, downstream_seq));
        txn.condition
            .push(txn_cond_seq(&update.idents.upstream, Eq, upstream_seq));
        txn.if_then
            .push(txn_put_pb(&update.idents.downstream, &merged_detail));
        txn.if_then
            .push(txn_put_pb(&update.idents.upstream, &merged_detail));
    }

    Ok(txn)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use chrono::Utc;
    use databend_common_meta_app::schema::ColumnRef;
    use databend_common_meta_app::schema::LineageColumn;
    use databend_common_meta_app::schema::LineageIdentity;
    use databend_common_meta_app::schema::LineageKind;
    use databend_common_meta_app::schema::LineageObjectType;
    use databend_common_meta_app::tenant::Tenant;
    use databend_meta_client::kvapi::StructKey;

    use super::*;
    use crate::testing::new_local_meta_store;

    #[test]
    fn test_merge_lineage_detail_updates_last_observed_fields_and_preserves_kind() {
        let existing = LineageDetail {
            kind: LineageKind::Ctas,
            last_query_id: Some("query1".to_string()),
            updated_on: lineage_time(),
            column_lineage: vec![LineageColumn {
                upstream: ColumnRef::Id(1),
                downstream: ColumnRef::Id(2),
            }],
        };
        let incoming = LineageDetail {
            kind: LineageKind::DataMovement,
            last_query_id: Some("query2".to_string()),
            updated_on: Utc.with_ymd_and_hms(2026, 7, 24, 0, 0, 0).unwrap(),
            column_lineage: vec![
                LineageColumn {
                    upstream: ColumnRef::Id(1),
                    downstream: ColumnRef::Id(2),
                },
                LineageColumn {
                    upstream: ColumnRef::Id(3),
                    downstream: ColumnRef::Id(4),
                },
            ],
        };

        let got = merge_lineage_detail(Some(existing), &incoming);

        assert_eq!(got.kind, LineageKind::Ctas);
        assert_eq!(got.last_query_id.as_deref(), Some("query2"));
        assert_eq!(got.updated_on, incoming.updated_on);
        assert_eq!(got.column_lineage.len(), 2);
    }

    #[tokio::test]
    async fn test_merge_lineage_aggregates_duplicate_edges_and_updates_both_directions()
    -> anyhow::Result<()> {
        let meta = new_local_meta_store().await;
        let tenant = Tenant::new_literal("tenant1");
        let existing = LineageDetail {
            kind: LineageKind::Ctas,
            last_query_id: Some("query1".to_string()),
            updated_on: lineage_time(),
            column_lineage: vec![LineageColumn {
                upstream: ColumnRef::Id(1),
                downstream: ColumnRef::Id(2),
            }],
        };
        replace_lineage(
            &meta,
            &lineage_update(&tenant, "10", "20", existing, LineageUpdateMode::Replace),
        )
        .await?;

        let updated_on = Utc.with_ymd_and_hms(2026, 7, 24, 0, 0, 0).unwrap();
        meta.merge_lineage(MergeLineageReq {
            updates: vec![
                lineage_update(
                    &tenant,
                    "10",
                    "20",
                    LineageDetail {
                        kind: LineageKind::DataMovement,
                        last_query_id: Some("query2".to_string()),
                        updated_on: lineage_time(),
                        column_lineage: vec![LineageColumn {
                            upstream: ColumnRef::Id(1),
                            downstream: ColumnRef::Id(2),
                        }],
                    },
                    LineageUpdateMode::Merge,
                ),
                lineage_update(
                    &tenant,
                    "10",
                    "20",
                    LineageDetail {
                        kind: LineageKind::DataMovement,
                        last_query_id: Some("query3".to_string()),
                        updated_on,
                        column_lineage: vec![
                            LineageColumn {
                                upstream: ColumnRef::Id(1),
                                downstream: ColumnRef::Id(2),
                            },
                            LineageColumn {
                                upstream: ColumnRef::Id(3),
                                downstream: ColumnRef::Id(4),
                            },
                        ],
                    },
                    LineageUpdateMode::Merge,
                ),
            ],
        })
        .await?;

        let downstream =
            list_entries(&meta, tenant.clone(), LineageDirection::Downstream, "10").await?;
        let upstream = list_entries(&meta, tenant, LineageDirection::Upstream, "20").await?;
        assert_eq!(downstream.len(), 1);
        assert_eq!(upstream.len(), 1);
        assert_eq!(downstream[0].1, upstream[0].1);

        let detail = &downstream[0].1;
        assert_eq!(detail.kind, LineageKind::Ctas);
        assert_eq!(detail.last_query_id.as_deref(), Some("query3"));
        assert_eq!(detail.updated_on, updated_on);
        assert_eq!(detail.column_lineage, vec![
            LineageColumn {
                upstream: ColumnRef::Id(1),
                downstream: ColumnRef::Id(2),
            },
            LineageColumn {
                upstream: ColumnRef::Id(3),
                downstream: ColumnRef::Id(4),
            },
        ]);
        Ok(())
    }

    #[tokio::test]
    async fn test_lineage_merge_retry_is_bounded() {
        let mut trials = txn_backoff(Some(LINEAGE_MERGE_MAX_RETRIES), "test");
        for _ in 0..LINEAGE_MERGE_MAX_RETRIES {
            trials.next().unwrap().unwrap().await;
        }
        assert!(trials.next().unwrap().is_err());
    }

    #[tokio::test]
    async fn test_delete_lineage_for_table_id_is_scoped_to_tenant_and_object() -> anyhow::Result<()>
    {
        let meta = new_local_meta_store().await;
        let tenant_a = Tenant::new_literal("tenant_a");
        let tenant_b = Tenant::new_literal("tenant_b");
        let detail = lineage_detail();

        for (tenant, upstream, downstream) in [
            (tenant_a.clone(), "10", "20"),
            (tenant_a.clone(), "20", "30"),
            (tenant_a.clone(), "10", "21"),
            (tenant_a.clone(), "200", "30"),
            (tenant_b.clone(), "10", "20"),
        ] {
            replace_lineage(
                &meta,
                &lineage_update(
                    &tenant,
                    upstream,
                    downstream,
                    detail.clone(),
                    LineageUpdateMode::Replace,
                ),
            )
            .await?;
        }

        let dir = lineage_object_dir(
            tenant_a.clone(),
            LineageDirection::Upstream,
            table_ref("20"),
        );
        assert_eq!(
            dir.to_string_key(),
            "__fd_lineage/tenant_a/upstream/table/id/20"
        );

        let mut txn = TxnRequest::default();
        append_delete_lineage_for_table_id_txn_ops(&meta, &tenant_a, &mut txn, 20).await?;
        let (success, _) = send_txn(&meta, txn).await?;
        assert!(success);

        assert!(
            list_entries(&meta, tenant_a.clone(), LineageDirection::Upstream, "20")
                .await?
                .is_empty()
        );
        assert!(
            list_entries(&meta, tenant_a.clone(), LineageDirection::Downstream, "20")
                .await?
                .is_empty()
        );
        assert_eq!(
            list_entries(&meta, tenant_a.clone(), LineageDirection::Downstream, "10")
                .await?
                .len(),
            1
        );
        assert_eq!(
            list_entries(&meta, tenant_a, LineageDirection::Downstream, "200")
                .await?
                .len(),
            1
        );
        assert_eq!(
            list_entries(&meta, tenant_b, LineageDirection::Upstream, "20")
                .await?
                .len(),
            1
        );
        Ok(())
    }

    async fn list_entries<KV>(
        kv: &KV,
        tenant: Tenant,
        direction: LineageDirection,
        object_id: &str,
    ) -> Result<Vec<(LineageKey, LineageDetail)>, MetaError>
    where
        KV: LineageApi + ?Sized,
    {
        Ok(kv
            .list_lineage(ListLineageReq {
                tenant,
                direction,
                object: table_ref(object_id),
            })
            .await?
            .entries)
    }

    fn table_ref(id: &str) -> LineageObjectRef {
        LineageObjectRef {
            object_type: LineageObjectType::Table,
            identity: LineageIdentity::Id { id: id.to_string() },
        }
    }

    fn lineage_update(
        tenant: &Tenant,
        upstream: &str,
        downstream: &str,
        detail: LineageDetail,
        mode: LineageUpdateMode,
    ) -> LineageUpdate {
        LineageUpdate {
            tenant_name: tenant.tenant_name().to_string(),
            upstream: table_ref(upstream),
            downstream: table_ref(downstream),
            detail,
            mode,
        }
    }

    async fn replace_lineage<KV>(kv: &KV, update: &LineageUpdate) -> Result<(), MetaError>
    where KV: KVPbApi<Error = MetaError> + ?Sized {
        let mut txn = TxnRequest::default();
        append_replace_lineage_txn_ops(&mut txn, [update]);
        let (success, _) = send_txn(kv, txn).await?;
        assert!(success);
        Ok(())
    }

    fn lineage_detail() -> LineageDetail {
        LineageDetail {
            kind: LineageKind::DataMovement,
            last_query_id: Some("query".to_string()),
            updated_on: lineage_time(),
            column_lineage: vec![],
        }
    }

    fn lineage_time() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 23, 0, 0, 0).unwrap()
    }
}
