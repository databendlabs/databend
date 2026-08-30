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
use std::collections::BTreeSet;

use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::TableLockExpired;
use databend_common_meta_app::schema::CreateSegmentRewriteClaimReply;
use databend_common_meta_app::schema::CreateSegmentRewriteClaimReq;
use databend_common_meta_app::schema::DeleteSegmentRewriteClaimReq;
use databend_common_meta_app::schema::ExtendSegmentRewriteClaimReq;
use databend_common_meta_app::schema::ListSegmentRewriteClaimsReq;
use databend_common_meta_app::schema::LockMeta;
use databend_common_meta_app::schema::LockType;
use databend_common_meta_app::schema::SegmentRewriteClaimIdent;
use databend_common_meta_app::schema::SegmentRewriteTarget;
use databend_common_meta_app::schema::segment_rewrite_claim_ident::SEGMENT_REWRITE_CLAIM_SEQ_KEY;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::DirName;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::TxnOp;
use databend_meta_client::types::TxnRequest;
use futures::TryStreamExt;

use crate::error_util::invalid_reply;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::encode_pb;
use crate::kv_pb_crud_api::KVPbCrudApi;
use crate::txn_core_util::send_txn;

const SEGMENTS_KEY: &str = "segments";

#[async_trait::async_trait]
pub trait SegmentRewriteClaimApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    async fn list_segment_rewrite_claims(
        &self,
        req: ListSegmentRewriteClaimsReq,
    ) -> Result<Vec<(u64, Vec<SegmentRewriteTarget>)>, KVAppError> {
        let dir = DirName::new(SegmentRewriteClaimIdent::new(req.tenant, req.table_id, 0));
        let mut stream = self.list_pb(ListOptions::unlimited(&dir)).await?;
        let mut claims = Vec::new();

        while let Some(item) = stream.try_next().await? {
            claims.push((
                item.key.try_revision().map_err(|error| {
                    invalid_reply(format!("invalid segment rewrite claim revision: {error}"))
                })?,
                decode_segments(&item.seqv.data)?,
            ));
        }
        claims.sort_unstable_by_key(|(revision, _)| *revision);
        Ok(claims)
    }

    async fn create_segment_rewrite_claim(
        &self,
        mut req: CreateSegmentRewriteClaimReq,
    ) -> Result<CreateSegmentRewriteClaimReply, KVAppError> {
        req.segments.sort_unstable();
        req.segments.dedup();
        if req.segments.is_empty() {
            return Err(invalid_reply("segment rewrite claim must contain segments").into());
        }

        let mut extra_info = BTreeMap::new();
        extra_info.insert(
            SEGMENTS_KEY.to_string(),
            serde_json::to_string(&req.segments)
                .map_err(|error| invalid_reply(format!("encode segment rewrite claim: {error}")))?,
        );
        let meta = LockMeta {
            user: req.user,
            node: req.node,
            query_id: req.query_id,
            created_on: Utc::now(),
            acquired_on: None,
            lock_type: LockType::TABLE,
            extra_info,
        };
        let dir = DirName::new(SegmentRewriteClaimIdent::new(
            req.tenant.clone(),
            req.table_id,
            0,
        ));
        let op = TxnOp::put_sequential(
            dir.dir_name_with_slash(),
            SEGMENT_REWRITE_CLAIM_SEQ_KEY,
            encode_pb(&meta),
        )
        .with_ttl(Some(req.ttl));
        let (_, responses) = send_txn(self, TxnRequest::new(vec![], vec![op])).await?;
        let Some(put) = responses.first().and_then(|response| response.try_as_put()) else {
            return Err(
                invalid_reply("segment rewrite claim PutSequential returned no key").into(),
            );
        };
        let ident = SegmentRewriteClaimIdent::from_str_key(&put.key).map_err(|error| {
            invalid_reply(format!(
                "invalid segment rewrite claim key '{}': {error}",
                put.key
            ))
        })?;
        let revision = ident.try_revision().map_err(|error| {
            invalid_reply(format!("invalid segment rewrite claim revision: {error}"))
        })?;

        let claims = self
            .list_segment_rewrite_claims(ListSegmentRewriteClaimsReq {
                tenant: req.tenant.clone(),
                table_id: req.table_id,
            })
            .await?;
        let requested = req.segments.iter().collect::<BTreeSet<_>>();
        let conflicts = claims.iter().any(|(other_revision, segments)| {
            *other_revision < revision && segments.iter().any(|segment| requested.contains(segment))
        });
        if conflicts {
            self.delete_segment_rewrite_claim(DeleteSegmentRewriteClaimReq {
                tenant: req.tenant,
                table_id: req.table_id,
                revision,
            })
            .await?;
            return Ok(CreateSegmentRewriteClaimReply { revision: None });
        }

        let key = SegmentRewriteClaimIdent::new(req.tenant, req.table_id, revision);
        self.crud_update_existing(
            &key,
            |mut meta| {
                meta.acquired_on = Some(Utc::now());
                Some((meta, Some(req.ttl)))
            },
            || {
                Err(AppError::TableLockExpired(TableLockExpired::new(
                    req.table_id,
                    "create_segment_rewrite_claim",
                )))
            },
        )
        .await??;

        Ok(CreateSegmentRewriteClaimReply {
            revision: Some(revision),
        })
    }

    async fn extend_segment_rewrite_claim(
        &self,
        req: ExtendSegmentRewriteClaimReq,
    ) -> Result<(), KVAppError> {
        let key = SegmentRewriteClaimIdent::new(req.tenant, req.table_id, req.revision);
        self.crud_update_existing(
            &key,
            |meta| Some((meta, Some(req.ttl))),
            || {
                Err(AppError::TableLockExpired(TableLockExpired::new(
                    req.table_id,
                    "extend_segment_rewrite_claim",
                )))
            },
        )
        .await??;
        Ok(())
    }

    async fn delete_segment_rewrite_claim(
        &self,
        req: DeleteSegmentRewriteClaimReq,
    ) -> Result<(), KVAppError> {
        let key = SegmentRewriteClaimIdent::new(req.tenant, req.table_id, req.revision);
        self.crud_remove(&key, || Ok::<(), ()>(())).await?.unwrap();
        Ok(())
    }
}

fn decode_segments(meta: &LockMeta) -> Result<Vec<SegmentRewriteTarget>, KVAppError> {
    let encoded = meta
        .extra_info
        .get(SEGMENTS_KEY)
        .ok_or_else(|| invalid_reply("segment rewrite claim has no segment set"))?;
    serde_json::from_str(encoded)
        .map_err(|error| invalid_reply(format!("decode segment rewrite claim: {error}")).into())
}

#[async_trait::async_trait]
impl<KV> SegmentRewriteClaimApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use databend_common_meta_app::schema::CreateSegmentRewriteClaimReq;
    use databend_common_meta_app::schema::DeleteSegmentRewriteClaimReq;
    use databend_common_meta_app::schema::ListSegmentRewriteClaimsReq;
    use databend_common_meta_app::schema::SegmentRewriteTarget;

    use super::SegmentRewriteClaimApi;
    use crate::testing;

    const TABLE_ID: u64 = 7;

    fn segment(location: &str) -> SegmentRewriteTarget {
        SegmentRewriteTarget {
            location: location.to_string(),
            format_version: 1,
        }
    }

    fn create_req(
        query_id: &str,
        segments: Vec<SegmentRewriteTarget>,
    ) -> CreateSegmentRewriteClaimReq {
        CreateSegmentRewriteClaimReq {
            tenant: testing::tenant("tenant1"),
            table_id: TABLE_ID,
            ttl: Duration::from_secs(30),
            user: "user1".to_string(),
            node: "node1".to_string(),
            query_id: query_id.to_string(),
            segments,
        }
    }

    #[tokio::test]
    async fn test_intersecting_claims_have_one_winner() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        let (left, right) = tokio::join!(
            store.create_segment_rewrite_claim(create_req("q1", vec![segment("s1")])),
            store.create_segment_rewrite_claim(create_req("q2", vec![segment("s1")]))
        );
        let left = left?;
        let right = right?;

        assert_ne!(left.revision.is_some(), right.revision.is_some());
        let claims = store
            .list_segment_rewrite_claims(ListSegmentRewriteClaimsReq {
                tenant: testing::tenant("tenant1"),
                table_id: TABLE_ID,
            })
            .await?;
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].1, vec![segment("s1")]);
        Ok(())
    }

    #[tokio::test]
    async fn test_disjoint_claims_can_coexist_and_release() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        let first = store
            .create_segment_rewrite_claim(create_req("q1", vec![segment("s1")]))
            .await?
            .revision
            .expect("first disjoint claim should succeed");
        let second = store
            .create_segment_rewrite_claim(create_req("q2", vec![segment("s2")]))
            .await?
            .revision
            .expect("second disjoint claim should succeed");

        let claims = store
            .list_segment_rewrite_claims(ListSegmentRewriteClaimsReq {
                tenant: testing::tenant("tenant1"),
                table_id: TABLE_ID,
            })
            .await?;
        assert_eq!(claims.len(), 2);

        store
            .delete_segment_rewrite_claim(DeleteSegmentRewriteClaimReq {
                tenant: testing::tenant("tenant1"),
                table_id: TABLE_ID,
                revision: first,
            })
            .await?;
        let replacement = store
            .create_segment_rewrite_claim(create_req("q3", vec![segment("s1")]))
            .await?;
        assert!(replacement.revision.is_some());

        store
            .delete_segment_rewrite_claim(DeleteSegmentRewriteClaimReq {
                tenant: testing::tenant("tenant1"),
                table_id: TABLE_ID,
                revision: second,
            })
            .await?;
        Ok(())
    }
}
