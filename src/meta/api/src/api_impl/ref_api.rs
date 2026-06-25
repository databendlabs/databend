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
use databend_common_meta_app::app_error::ReferenceAlreadyExists;
use databend_common_meta_app::app_error::ReferenceExpired;
use databend_common_meta_app::app_error::TableSnapshotExpired;
use databend_common_meta_app::app_error::TableVersionMismatched;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::schema::CreateTableTagReq;
use databend_common_meta_app::schema::DropTableTagReq;
use databend_common_meta_app::schema::GetTableTagReq;
use databend_common_meta_app::schema::ListTableTagsReq;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdTagName;
use databend_common_meta_app::schema::TableLvtCheck;
use databend_common_meta_app::schema::TableTag;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::DirName;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::ConditionResult::Eq;
use databend_meta_client::types::MatchSeqExt;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnCondition;
use databend_meta_client::types::TxnRequest;
use fastrace::func_name;
use log::debug;

use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::txn::meta_txn;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_seq;
use crate::txn_core_util::send_txn;
use crate::txn_put_pb;

async fn build_lvt_condition(
    kv_api: &(impl KVPbApi<Error = MetaError> + ?Sized),
    table_id: u64,
    lvt_check: &TableLvtCheck,
) -> Result<TxnCondition, KVAppError> {
    let lvt_ident = LeastVisibleTimeIdent::new(&lvt_check.tenant, table_id);
    let res = kv_api.get_pb(&lvt_ident).await?;
    let (lvt_seq, current_lvt) = match res {
        Some(v) => (v.seq, Some(v.data)),
        None => (0, None),
    };

    if let Some(current_lvt) = current_lvt {
        if current_lvt.time > lvt_check.time {
            return Err(KVAppError::AppError(AppError::TableSnapshotExpired(
                TableSnapshotExpired::new(
                    table_id,
                    format!(
                        "snapshot timestamp {:?} is older than the table's least visible time {:?}",
                        lvt_check.time, current_lvt.time
                    ),
                ),
            )));
        }
    }

    Ok(txn_cond_seq(&lvt_ident, Eq, lvt_seq))
}

#[async_trait::async_trait]
pub trait RefApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    /// Create a table tag on the base table.
    ///
    /// A tag does not generate a new snapshot; it only records the snapshot location it points to.
    /// Tags can only be created on the base table.
    ///
    /// Writes: `__fd_table_tag/<table_id>/<tag_name> -> TableTag`
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_table_tag(&self, req: CreateTableTagReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let table_id = req.table_id;
        let key_table_id = TableId { table_id };
        let key_tag = TableIdTagName::new(table_id, &req.tag_name);
        let table_tag = TableTag {
            expire_at: req.expire_at,
            snapshot_loc: req.snapshot_loc.clone(),
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Ensure the base table exists and has not changed.
            let seq_table_meta = self.get_pb(&key_table_id).await?;
            let Some(seq_table_meta) = seq_table_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(table_id, "create_table_tag"),
                )));
            };
            // Reject tags on soft-deleted tables so dropped-table cleanup cannot race with
            // a late tag creation that still sees the old table seq.
            if seq_table_meta.data.drop_on.is_some() {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(table_id, "create_table_tag"),
                )));
            }
            // Check seq matches caller's expectation.
            if req.seq.match_seq(&seq_table_meta).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        table_id,
                        req.seq,
                        seq_table_meta.seq,
                        "create_table_tag",
                    ),
                )));
            }

            // Check if tag already exists.
            let seq_tag = self.get_pb(&key_tag).await?;
            if seq_tag.is_some() {
                return Err(KVAppError::AppError(AppError::from(
                    ReferenceAlreadyExists::new(format!("Tag '{}' already exists", req.tag_name)),
                )));
            }

            let conditions = vec![
                // Table must not change.
                txn_cond_seq(&key_table_id, Eq, seq_table_meta.seq),
                // Tag must not already exist.
                txn_cond_seq(&key_tag, Eq, 0),
                // Check table lvt.
                build_lvt_condition(self, table_id, &req.lvt_check).await?,
            ];

            let txn = TxnRequest::new(conditions, vec![txn_put_pb(&key_tag, &table_tag)]);
            let (succ, _responses) = send_txn(self, txn).await?;
            if succ {
                return Ok(());
            }
        }
    }

    /// Drop a table tag.
    ///
    /// Deletes: `__fd_table_tag/<table_id>/<tag_name>`
    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_table_tag(&self, req: DropTableTagReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let key_tag = TableIdTagName::new(req.table_id, &req.tag_name);
        let ctx = format!(
            "{}: table_id={}, tag_name='{}', seq={}",
            func_name!(),
            req.table_id,
            req.tag_name,
            req.seq
                .map_or_else(|| "any".to_string(), |seq| seq.to_string())
        );

        let mgr = meta_txn::MetaTxnManager::new(self, ctx);
        mgr.remove_key(key_tag, req.seq)
            .await
            .map_err(|err| match err {
                meta_txn::RunError::KvApi(err) => KVAppError::MetaError(err),
                meta_txn::RunError::TxnRetryMaxTimes(err) => {
                    KVAppError::AppError(AppError::from(err))
                }
                meta_txn::RunError::User(err) => KVAppError::AppError(AppError::from(err)),
            })
    }

    /// Get a table tag.
    ///
    /// Reads: `__fd_table_tag/<table_id>/<tag_name> -> TableTag`
    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_tag(
        &self,
        req: GetTableTagReq,
    ) -> Result<Option<SeqV<TableTag>>, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let key_tag = TableIdTagName::new(req.table_id, &req.tag_name);
        let seq_tag = self.get_pb(&key_tag).await?;
        if !req.include_expired {
            if let Some(tag) = &seq_tag {
                if let Some(expire_at) = tag.data.expire_at.as_ref() {
                    if *expire_at <= Utc::now() {
                        return Err(KVAppError::AppError(AppError::from(ReferenceExpired::new(
                            format!("Tag '{}' expired at '{}'", req.tag_name, expire_at),
                        ))));
                    }
                }
            }
        }
        Ok(seq_tag)
    }

    /// List table tags.
    ///
    /// Reads: `__fd_table_tag/<table_id>/<tag_name> -> TableTag`
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_table_tags(
        &self,
        req: ListTableTagsReq,
    ) -> Result<Vec<(String, SeqV<TableTag>)>, KVAppError> {
        debug!(req :? =(&req); "RefApi: {}", func_name!());

        let key_prefix = TableIdTagName::new(req.table_id, "");
        let dir = DirName::new(key_prefix);
        let entries = self.list_pb_vec(ListOptions::unlimited(&dir)).await?;
        let now = Utc::now();

        let mut tags = Vec::with_capacity(entries.len());
        for (key, seq_tag) in entries {
            if !req.include_expired
                && seq_tag
                    .data
                    .expire_at
                    .as_ref()
                    .is_some_and(|expire_at| *expire_at <= now)
            {
                continue;
            }
            tags.push((key.tag_name, seq_tag));
        }

        Ok(tags)
    }
}

#[async_trait::async_trait]
impl<KV> RefApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono::Utc;
    use databend_common_meta_app::app_error::AppError;
    use databend_common_meta_app::schema::LeastVisibleTime;
    use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
    use databend_meta_client::types::MatchSeq;

    use super::CreateTableTagReq;
    use super::DropTableTagReq;
    use super::GetTableTagReq;
    use super::ListTableTagsReq;
    use super::RefApi;
    use super::TableId;
    use super::TableIdTagName;
    use super::TableLvtCheck;
    use super::TableTag;
    use crate::kv_app_error::KVAppError;
    use crate::testing;
    use crate::testing::KVPbApiTestExt;

    const TABLE_ID: u64 = 10;
    const TENANT: &str = "tenant1";

    fn table_tag(snapshot_loc: &str, expire_at: Option<DateTime<Utc>>) -> TableTag {
        TableTag {
            expire_at,
            snapshot_loc: snapshot_loc.to_string(),
        }
    }

    fn table_key(table_id: u64) -> TableId {
        TableId::new(table_id)
    }

    fn tag_key(tag_name: &str) -> TableIdTagName {
        tag_key_for(TABLE_ID, tag_name)
    }

    fn tag_key_for(table_id: u64, tag_name: &str) -> TableIdTagName {
        TableIdTagName::new(table_id, tag_name)
    }

    fn create_req(
        tag_name: &str,
        snapshot_loc: &str,
        table_seq: u64,
        expire_at: Option<DateTime<Utc>>,
        lvt_time: DateTime<Utc>,
    ) -> CreateTableTagReq {
        create_req_with_match_seq(
            tag_name,
            snapshot_loc,
            MatchSeq::Exact(table_seq),
            expire_at,
            lvt_time,
        )
    }

    fn create_req_with_match_seq(
        tag_name: &str,
        snapshot_loc: &str,
        seq: MatchSeq,
        expire_at: Option<DateTime<Utc>>,
        lvt_time: DateTime<Utc>,
    ) -> CreateTableTagReq {
        CreateTableTagReq {
            table_id: TABLE_ID,
            seq,
            tag_name: tag_name.to_string(),
            snapshot_loc: snapshot_loc.to_string(),
            expire_at,
            lvt_check: TableLvtCheck {
                tenant: testing::tenant(TENANT),
                time: lvt_time,
            },
        }
    }

    fn drop_tag_req(tag_name: &str, seq: Option<u64>) -> DropTableTagReq {
        DropTableTagReq {
            table_id: TABLE_ID,
            tag_name: tag_name.to_string(),
            seq,
        }
    }

    fn get_tag_req(tag_name: &str, include_expired: bool) -> GetTableTagReq {
        GetTableTagReq {
            table_id: TABLE_ID,
            tag_name: tag_name.to_string(),
            include_expired,
        }
    }

    fn list_tags_req(include_expired: bool) -> ListTableTagsReq {
        ListTableTagsReq {
            table_id: TABLE_ID,
            include_expired,
        }
    }

    async fn seed_table(
        store: &(impl KVPbApiTestExt + ?Sized),
        table_id: u64,
        seq: u64,
    ) -> anyhow::Result<()> {
        store
            .insert_pb_assert_seq(&table_key(table_id), testing::table_meta(), seq)
            .await
    }

    async fn seed_dropped_table(
        store: &(impl KVPbApiTestExt + ?Sized),
        table_id: u64,
        seq: u64,
    ) -> anyhow::Result<()> {
        store
            .insert_pb_assert_seq(&table_key(table_id), testing::dropped_table_meta(), seq)
            .await
    }

    async fn create_tag(
        store: &(impl RefApi + ?Sized),
        tag_name: &str,
        snapshot_loc: &str,
        table_seq: u64,
        expire_at: Option<DateTime<Utc>>,
    ) -> Result<(), KVAppError> {
        let req = create_req(
            tag_name,
            snapshot_loc,
            table_seq,
            expire_at,
            testing::fixed_time(3_000),
        );
        store.create_table_tag(req).await
    }

    #[tokio::test]
    async fn test_create_table_tag_writes_value_and_seq() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        seed_table(&store, TABLE_ID, 1).await?;

        create_tag(&store, "tag1", "snapshot-location", 1, None).await?;

        store
            .assert_pb(&tag_key("tag1"), 2, table_tag("snapshot-location", None))
            .await?;
        store
            .assert_pb(&table_key(TABLE_ID), 1, testing::table_meta())
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_accepts_match_seq_variants() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        seed_table(&store, TABLE_ID, 1).await?;

        let req = create_req_with_match_seq(
            "ge-fail",
            "snapshot-location-ge-fail",
            MatchSeq::GE(2),
            None,
            testing::fixed_time(3_000),
        );
        let err = store.create_table_tag(req).await.unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::TableVersionMismatched(_))
        ));
        store.assert_no_pb(&tag_key("ge-fail")).await?;

        let req = create_req_with_match_seq(
            "any",
            "snapshot-location-any",
            MatchSeq::Any,
            None,
            testing::fixed_time(3_000),
        );
        store.create_table_tag(req).await?;
        store
            .assert_pb(&tag_key("any"), 2, table_tag("snapshot-location-any", None))
            .await?;

        let req = create_req_with_match_seq(
            "ge",
            "snapshot-location-ge",
            MatchSeq::GE(1),
            None,
            testing::fixed_time(3_000),
        );
        store.create_table_tag(req).await?;
        store
            .assert_pb(&tag_key("ge"), 3, table_tag("snapshot-location-ge", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_rejects_missing_table_without_write() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;

        let err = create_tag(&store, "tag1", "snapshot-location", 1, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::UnknownTableId(_))
        ));
        store.assert_no_pb(&tag_key("tag1")).await?;

        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "tag1", "snapshot-location", 1, None).await?;
        store
            .assert_pb(&tag_key("tag1"), 2, table_tag("snapshot-location", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_rejects_dropped_table_without_write() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        seed_dropped_table(&store, TABLE_ID, 1).await?;

        let err = create_tag(&store, "tag1", "snapshot-location", 1, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::UnknownTableId(_))
        ));
        store.assert_no_pb(&tag_key("tag1")).await?;

        let tenant = testing::tenant(TENANT);
        let lvt_ident = LeastVisibleTimeIdent::new(&tenant, TABLE_ID);
        store
            .insert_pb_assert_seq(
                &lvt_ident,
                LeastVisibleTime::new(testing::fixed_time(3_000)),
                2,
            )
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_rejects_table_seq_mismatch_without_write() -> anyhow::Result<()>
    {
        let store = testing::new_local_meta_store().await;
        seed_table(&store, TABLE_ID, 1).await?;

        let err = create_tag(&store, "tag1", "snapshot-location", 2, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::TableVersionMismatched(_))
        ));
        store.assert_no_pb(&tag_key("tag1")).await?;

        create_tag(&store, "tag1", "snapshot-location", 1, None).await?;
        store
            .assert_pb(&tag_key("tag1"), 2, table_tag("snapshot-location", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_rejects_duplicate_without_write() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "tag1", "snapshot-location-1", 1, None).await?;

        let err = create_tag(&store, "tag1", "snapshot-location-2", 1, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::ReferenceAlreadyExists(_))
        ));
        store
            .assert_pb(&tag_key("tag1"), 2, table_tag("snapshot-location-1", None))
            .await?;

        create_tag(&store, "tag2", "snapshot-location-2", 1, None).await?;
        store
            .assert_pb(&tag_key("tag2"), 3, table_tag("snapshot-location-2", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_rejects_expired_duplicate_without_write() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        let expire_at = Some(testing::fixed_time(1));
        let expired_tag = table_tag("snapshot-location-1", expire_at);

        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "tag1", "snapshot-location-1", 1, expire_at).await?;

        let err = create_tag(&store, "tag1", "snapshot-location-2", 1, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::ReferenceAlreadyExists(_))
        ));
        store.assert_pb(&tag_key("tag1"), 2, expired_tag).await?;

        create_tag(&store, "tag2", "snapshot-location-2", 1, None).await?;
        store
            .assert_pb(&tag_key("tag2"), 3, table_tag("snapshot-location-2", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_tag_checks_lvt_without_failed_write() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        seed_table(&store, TABLE_ID, 1).await?;

        let tenant = testing::tenant(TENANT);
        let lvt_ident = LeastVisibleTimeIdent::new(&tenant, TABLE_ID);
        store
            .insert_pb_assert_seq(
                &lvt_ident,
                LeastVisibleTime::new(testing::fixed_time(4_000)),
                2,
            )
            .await?;

        let req = create_req(
            "tag1",
            "snapshot-location",
            1,
            None,
            testing::fixed_time(3_000),
        );
        let err = store.create_table_tag(req).await.unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::TableSnapshotExpired(_))
        ));
        store.assert_no_pb(&tag_key("tag1")).await?;

        let req = create_req(
            "tag1",
            "snapshot-location",
            1,
            None,
            testing::fixed_time(4_000),
        );
        store.create_table_tag(req).await?;
        store
            .assert_pb(&tag_key("tag1"), 3, table_tag("snapshot-location", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_table_tag_checks_seq_and_deletes() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "tag1", "snapshot-location-1", 1, None).await?;

        let req = drop_tag_req("tag1", Some(3));
        let err = store.drop_table_tag(req).await.unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::UnknownReference(_))
        ));
        store
            .assert_pb(&tag_key("tag1"), 2, table_tag("snapshot-location-1", None))
            .await?;

        let req = drop_tag_req("tag1", Some(2));
        store.drop_table_tag(req).await?;
        store.assert_no_pb(&tag_key("tag1")).await?;

        create_tag(&store, "tag2", "snapshot-location-2", 1, None).await?;
        store
            .assert_pb(&tag_key("tag2"), 3, table_tag("snapshot-location-2", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_table_tag_without_seq() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;

        let req = drop_tag_req("tag1", None);
        let err = store.drop_table_tag(req).await.unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::UnknownReference(_))
        ));

        let req = drop_tag_req("tag1", Some(7));
        let err = store.drop_table_tag(req).await.unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::UnknownReference(_))
        ));

        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "tag1", "snapshot-location-1", 1, None).await?;

        let req = drop_tag_req("tag1", None);
        store.drop_table_tag(req).await?;
        store.assert_no_pb(&tag_key("tag1")).await?;

        create_tag(&store, "tag2", "snapshot-location-2", 1, None).await?;
        store
            .assert_pb(&tag_key("tag2"), 3, table_tag("snapshot-location-2", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_get_table_tag_returns_none_and_keeps_future_tag_active() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;

        let req = get_tag_req("missing", false);
        let got = store.get_table_tag(req).await?;
        assert_eq!(got, None);

        seed_table(&store, TABLE_ID, 1).await?;
        let expire_at = Some(testing::fixed_time(4_000_000_000));
        let future_tag = table_tag("snapshot-location-future", expire_at);
        create_tag(&store, "future", "snapshot-location-future", 1, expire_at).await?;

        let req = get_tag_req("future", false);
        let got = store.get_table_tag(req).await?.unwrap();
        assert_eq!((got.seq, got.data), (2, future_tag));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_table_tag_checks_expiration_without_write() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        let expire_at = Some(testing::fixed_time(1));
        let expired_tag = table_tag("snapshot-location-1", expire_at);

        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "tag1", "snapshot-location-1", 1, expire_at).await?;

        let req = get_tag_req("tag1", true);
        let got = store.get_table_tag(req).await?;
        let got = got.unwrap();
        assert_eq!((got.seq, got.data), (2, expired_tag.clone()));

        let req = get_tag_req("tag1", false);
        let err = store.get_table_tag(req).await.unwrap_err();
        assert!(matches!(
            err,
            KVAppError::AppError(AppError::ReferenceExpired(_))
        ));
        store.assert_pb(&tag_key("tag1"), 2, expired_tag).await?;

        create_tag(&store, "tag2", "snapshot-location-2", 1, None).await?;
        store
            .assert_pb(&tag_key("tag2"), 3, table_tag("snapshot-location-2", None))
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_list_table_tags_empty_and_table_id_isolated() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;

        let req = list_tags_req(true);
        let got = store.list_table_tags(req).await?;
        assert_eq!(got, vec![]);

        store
            .insert_pb_assert_seq(
                &tag_key_for(1, "one"),
                table_tag("snapshot-location-one", None),
                1,
            )
            .await?;
        store
            .insert_pb_assert_seq(&tag_key("ten"), table_tag("snapshot-location-ten", None), 2)
            .await?;
        store
            .insert_pb_assert_seq(
                &tag_key_for(100, "hundred"),
                table_tag("snapshot-location-hundred", None),
                3,
            )
            .await?;

        let req = list_tags_req(true);
        let got = store.list_table_tags(req).await?;
        let got = got
            .into_iter()
            .map(|(name, seqv)| (name, seqv.seq, seqv.data))
            .collect::<Vec<_>>();
        assert_eq!(got, vec![(
            "ten".to_string(),
            2,
            table_tag("snapshot-location-ten", None)
        )]);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_table_tags_filters_expired_tags() -> anyhow::Result<()> {
        let store = testing::new_local_meta_store().await;
        let expired_at = Some(testing::fixed_time(1));
        let future_at = Some(testing::fixed_time(4_000_000_000));
        let active_tag = table_tag("snapshot-location-1", None);
        let expired_tag = table_tag("snapshot-location-2", expired_at);
        let future_tag = table_tag("snapshot-location-3", future_at);

        seed_table(&store, TABLE_ID, 1).await?;
        create_tag(&store, "active", "snapshot-location-1", 1, None).await?;
        create_tag(&store, "expired", "snapshot-location-2", 1, expired_at).await?;
        create_tag(&store, "future", "snapshot-location-3", 1, future_at).await?;

        let req = list_tags_req(false);
        let got = store.list_table_tags(req).await?;
        let got = got
            .into_iter()
            .map(|(name, seqv)| (name, seqv.seq, seqv.data))
            .collect::<Vec<_>>();
        assert_eq!(got, vec![
            ("active".to_string(), 2, active_tag.clone()),
            ("future".to_string(), 4, future_tag.clone()),
        ]);

        let req = list_tags_req(true);
        let got = store.list_table_tags(req).await?;
        let got = got
            .into_iter()
            .map(|(name, seqv)| (name, seqv.seq, seqv.data))
            .collect::<Vec<_>>();
        assert_eq!(got, vec![
            ("active".to_string(), 2, active_tag),
            ("expired".to_string(), 3, expired_tag),
            ("future".to_string(), 4, future_tag),
        ]);

        Ok(())
    }
}
