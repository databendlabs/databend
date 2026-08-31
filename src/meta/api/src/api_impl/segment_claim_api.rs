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

use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::TableLockExpired;
use databend_common_meta_app::schema::CreateSegmentClaimReply;
use databend_common_meta_app::schema::CreateSegmentClaimReq;
use databend_common_meta_app::schema::DeleteSegmentClaimReq;
use databend_common_meta_app::schema::ExtendSegmentClaimReq;
use databend_common_meta_app::schema::ListSegmentClaimsReq;
use databend_common_meta_app::schema::SegmentClaimIdent;
use databend_common_meta_app::schema::SegmentClaimMeta;
use databend_common_meta_app::schema::segment_claim_ident::SEGMENT_CLAIM_SEQ_KEY;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::DirName;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::TxnOp;
use databend_meta_client::types::TxnRequest;

use crate::error_util::invalid_reply;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::kv_pb_api::encode_pb;
use crate::kv_pb_crud_api::KVPbCrudApi;
use crate::txn_core_util::send_txn;

#[async_trait::async_trait]
pub trait SegmentClaimApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    async fn list_segment_claims(
        &self,
        req: ListSegmentClaimsReq,
    ) -> Result<Vec<(u64, SegmentClaimMeta)>, KVAppError> {
        let dir = DirName::new(SegmentClaimIdent::new(req.tenant, req.table_id, 0));
        let mut claims = self
            .list_pb_vec(ListOptions::unlimited(&dir))
            .await?
            .into_iter()
            .map(|(key, seqv)| {
                key.try_claim_id()
                    .map(|claim_id| (claim_id, seqv.data))
                    .map_err(|error| invalid_reply(format!("invalid segment claim ID: {error}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        claims.sort_unstable_by_key(|(claim_id, _)| *claim_id);
        Ok(claims)
    }

    async fn create_segment_claim(
        &self,
        mut req: CreateSegmentClaimReq,
    ) -> Result<CreateSegmentClaimReply, KVAppError> {
        req.segment_locations.sort_unstable();
        req.segment_locations.dedup();
        if req.segment_locations.is_empty() {
            return Err(invalid_reply("segment claim must contain locations").into());
        }

        let meta = SegmentClaimMeta {
            user: req.user,
            node: req.node,
            query_id: req.query_id,
            created_on: Utc::now(),
            segment_locations: req.segment_locations,
        };
        let dir = DirName::new(SegmentClaimIdent::new(req.tenant.clone(), req.table_id, 0));
        let op = TxnOp::put_sequential(
            dir.dir_name_with_slash(),
            SEGMENT_CLAIM_SEQ_KEY,
            encode_pb(&meta),
        )
        .with_ttl(Some(req.ttl));
        let (_, responses) = send_txn(self, TxnRequest::new(vec![], vec![op])).await?;
        let Some(put) = responses.first().and_then(|response| response.try_as_put()) else {
            return Err(invalid_reply("segment claim PutSequential returned no key").into());
        };
        let ident = SegmentClaimIdent::from_str_key(&put.key).map_err(|error| {
            invalid_reply(format!("invalid segment claim key '{}': {error}", put.key))
        })?;
        let claim_id = ident
            .try_claim_id()
            .map_err(|error| invalid_reply(format!("invalid segment claim ID: {error}")))?;

        let claims = self
            .list_segment_claims(ListSegmentClaimsReq {
                tenant: req.tenant.clone(),
                table_id: req.table_id,
            })
            .await?;
        let requested = &meta.segment_locations;
        let conflicts = claims
            .iter()
            .take_while(|(other_claim_id, _)| *other_claim_id < claim_id)
            .any(|(_, other)| {
                other
                    .segment_locations
                    .iter()
                    .any(|location| requested.binary_search(location).is_ok())
            });
        if conflicts {
            self.delete_segment_claim(DeleteSegmentClaimReq {
                tenant: req.tenant,
                table_id: req.table_id,
                claim_id,
            })
            .await?;
            return Ok(CreateSegmentClaimReply { claim_id: None });
        }

        // Confirm that the winning claim still exists after overlap detection and
        // refresh its full TTL before handing ownership to the query node.
        let key = SegmentClaimIdent::new(req.tenant, req.table_id, claim_id);
        self.crud_update_existing(
            &key,
            |meta| Some((meta, Some(req.ttl))),
            || {
                Err(AppError::TableLockExpired(TableLockExpired::new(
                    req.table_id,
                    "acquire segment claim",
                )))
            },
        )
        .await??;

        Ok(CreateSegmentClaimReply {
            claim_id: Some(claim_id),
        })
    }

    async fn extend_segment_claim(&self, req: ExtendSegmentClaimReq) -> Result<(), KVAppError> {
        let key = SegmentClaimIdent::new(req.tenant, req.table_id, req.claim_id);
        self.crud_update_existing(
            &key,
            |meta| Some((meta, Some(req.ttl))),
            || {
                Err(AppError::TableLockExpired(TableLockExpired::new(
                    req.table_id,
                    "renew segment claim",
                )))
            },
        )
        .await??;
        Ok(())
    }

    async fn delete_segment_claim(&self, req: DeleteSegmentClaimReq) -> Result<(), KVAppError> {
        let key = SegmentClaimIdent::new(req.tenant, req.table_id, req.claim_id);
        self.crud_remove(&key, || Ok::<(), ()>(())).await?.unwrap();
        Ok(())
    }
}

#[async_trait::async_trait]
impl<KV> SegmentClaimApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use databend_common_meta_app::app_error::AppError;
    use databend_common_meta_app::schema::CreateSegmentClaimReq;
    use databend_common_meta_app::schema::DeleteSegmentClaimReq;
    use databend_common_meta_app::schema::ExtendSegmentClaimReq;
    use databend_common_meta_app::schema::ListSegmentClaimsReq;

    use super::SegmentClaimApi;
    use crate::kv_app_error::KVAppError;
    use crate::testing;

    const TABLE_ID: u64 = 7;

    fn create_req(query_id: &str, segment_locations: Vec<&str>) -> CreateSegmentClaimReq {
        CreateSegmentClaimReq {
            tenant: testing::tenant("tenant1"),
            table_id: TABLE_ID,
            ttl: Duration::from_secs(30),
            user: "user1".to_string(),
            node: "node1".to_string(),
            query_id: query_id.to_string(),
            segment_locations: segment_locations
                .into_iter()
                .map(ToString::to_string)
                .collect(),
        }
    }

    #[tokio::test]
    async fn test_intersecting_claims_have_one_winner() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        let (left, right) = tokio::join!(
            store.create_segment_claim(create_req("q1", vec!["s1"])),
            store.create_segment_claim(create_req("q2", vec!["s1"]))
        );
        let left = left?;
        let right = right?;

        assert_ne!(left.claim_id.is_some(), right.claim_id.is_some());
        let claims = store
            .list_segment_claims(ListSegmentClaimsReq {
                tenant: testing::tenant("tenant1"),
                table_id: TABLE_ID,
            })
            .await?;
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].1.segment_locations, vec!["s1"]);
        Ok(())
    }

    #[tokio::test]
    async fn test_disjoint_claims_can_coexist_and_release() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        let first = store
            .create_segment_claim(create_req("q1", vec!["s1"]))
            .await?
            .claim_id
            .expect("first disjoint claim should succeed");
        store
            .create_segment_claim(create_req("q2", vec!["s2"]))
            .await?
            .claim_id
            .expect("second disjoint claim should succeed");

        let claims = store
            .list_segment_claims(ListSegmentClaimsReq {
                tenant: testing::tenant("tenant1"),
                table_id: TABLE_ID,
            })
            .await?;
        assert_eq!(claims.len(), 2);

        store
            .delete_segment_claim(DeleteSegmentClaimReq {
                tenant: testing::tenant("tenant1"),
                table_id: TABLE_ID,
                claim_id: first,
            })
            .await?;
        let replacement = store
            .create_segment_claim(create_req("q3", vec!["s1"]))
            .await?;
        assert!(replacement.claim_id.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_renew_expired_claim_returns_terminal_error() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        let claim_id = store
            .create_segment_claim(create_req("q1", vec!["s1"]))
            .await?
            .claim_id
            .expect("claim should succeed");
        store
            .delete_segment_claim(DeleteSegmentClaimReq {
                tenant: testing::tenant("tenant1"),
                table_id: TABLE_ID,
                claim_id,
            })
            .await?;

        let error = store
            .extend_segment_claim(ExtendSegmentClaimReq {
                tenant: testing::tenant("tenant1"),
                table_id: TABLE_ID,
                claim_id,
                ttl: Duration::from_secs(30),
            })
            .await
            .expect_err("expired claim renewal must fail");
        assert!(matches!(
            error,
            KVAppError::AppError(AppError::TableLockExpired(_))
        ));
        Ok(())
    }
}
