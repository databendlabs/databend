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

use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::principal::ProcedureIdentity;
use databend_common_meta_app::principal::ProcedureNameIdent;
use databend_common_meta_app::principal::UdfIdent;
use databend_common_meta_app::principal::connection_ident::ConnectionIdent;
use databend_common_meta_app::principal::user_stage_ident::StageIdent;
use databend_common_meta_app::schema::CreateTagReply;
use databend_common_meta_app::schema::CreateTagReq;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::EmptyProto;
use databend_common_meta_app::schema::GetTagReply;
use databend_common_meta_app::schema::ObjectTagIdRef;
use databend_common_meta_app::schema::ObjectTagIdRefIdent;
use databend_common_meta_app::schema::ObjectTagIdRefValue;
use databend_common_meta_app::schema::ObjectTagValue;
use databend_common_meta_app::schema::SetObjectTagsReq;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TagError;
use databend_common_meta_app::schema::TagIdIdent;
use databend_common_meta_app::schema::TagIdObjectRef;
use databend_common_meta_app::schema::TagIdObjectRefIdent;
use databend_common_meta_app::schema::TagInfo;
use databend_common_meta_app::schema::TagMeta;
use databend_common_meta_app::schema::TagReferenceInfo;
use databend_common_meta_app::schema::TaggableObject;
use databend_common_meta_app::schema::UnsetObjectTagsReq;
use databend_common_meta_app::schema::tag::error::TagMetaError;
use databend_common_meta_app::schema::tag::id_ident::Resource as TagIdResource;
use databend_common_meta_app::schema::tag::id_ident::TagId;
use databend_common_meta_app::schema::tag::id_to_name_ident::TagIdToNameIdent;
use databend_common_meta_app::schema::tag::name_ident::Resource as TagNameResource;
use databend_common_meta_app::schema::tag::name_ident::TagNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::ConditionResult;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnCondition;
use databend_meta_types::TxnRequest;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;
use log::warn;
use seq_marked::SeqValue;

use super::name_id_value_api::NameIdValueApi;
use crate::fetch_id;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_keys_with_prefix;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_core_util::send_txn;
use crate::txn_op_builder_util::txn_del;
use crate::txn_op_builder_util::txn_put_pb_with_ttl;

/// Returns the meta-service key for a taggable object.
///
/// The key is used to check if the object exists (seq >= 1) before setting tags.
/// This prevents orphaned tag references when the object is dropped concurrently.
fn get_object_key(tenant: &Tenant, object: &TaggableObject) -> String {
    match object {
        TaggableObject::Connection { name } => {
            ConnectionIdent::new(tenant.clone(), name).to_string_key()
        }
        TaggableObject::Stage { name } => StageIdent::new(tenant.clone(), name).to_string_key(),
        TaggableObject::Database { db_id } => DatabaseId::new(*db_id).to_string_key(),
        TaggableObject::Table { table_id } => TableId::new(*table_id).to_string_key(),
        TaggableObject::UDF { name } => UdfIdent::new(tenant.clone(), name).to_string_key(),
        TaggableObject::Procedure { name, args } => {
            ProcedureNameIdent::new(tenant.clone(), ProcedureIdentity::new(name, args))
                .to_string_key()
        }
    }
}

impl<T> TagApi for T
where
    T: Send + Sync + ?Sized,
    T: kvapi::KVApi<Error = MetaError>,
    T: NameIdValueApi<TagNameIdent, TagIdResource>,
{
}

#[async_trait::async_trait]
pub trait TagApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
    Self: NameIdValueApi<TagNameIdent, TagIdResource>,
{
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_tag(
        &self,
        req: CreateTagReq,
    ) -> Result<Result<CreateTagReply, ExistError<TagNameResource>>, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let CreateTagReq { name_ident, meta } = req;

        let tag_id_value = fetch_id(self, IdGenerator::generic()).await?;
        let tag_id = TagId::new(tag_id_value);
        let id_ident = tag_id.into_t_ident(name_ident.tenant());
        let id_to_name_ident = TagIdToNameIdent::new_generic(name_ident.tenant(), tag_id);
        let name_raw = name_ident.to_raw();

        let mut txn = TxnRequest::default();
        txn.condition.push(txn_cond_eq_seq(&name_ident, 0));
        txn.if_then.extend(vec![
            txn_put_pb_with_ttl(&name_ident, &tag_id, None)?, // name -> id
            txn_put_pb_with_ttl(&id_ident, &meta, None)?,     // id -> meta
            txn_put_pb_with_ttl(&id_to_name_ident, &name_raw, None)?, // id -> name
        ]);

        let (succ, _) = send_txn(self, txn).await?;

        if succ {
            Ok(Ok(CreateTagReply { tag_id: *tag_id }))
        } else {
            Ok(Err(name_ident.exist_error(format!(
                "tag '{}' already exists",
                name_ident.tag_name()
            ))))
        }
    }

    /// Hard-delete a tag by removing all its keys.
    ///
    /// Returns `Err(TagError::TagHasReferences)` if the tag is still referenced by objects.
    /// Since tags do not support UNDROP, we must prevent deletion when references exist
    /// to avoid orphaned tag-object mappings.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_tag(
        &self,
        name_ident: &TagNameIdent,
    ) -> Result<Result<Option<(SeqV<TagId>, SeqV<TagMeta>)>, TagError>, MetaTxnError> {
        debug!(name_ident :? =(&name_ident); "SchemaApi: {}", func_name!());

        let tenant = name_ident.tenant();
        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;
            let Some((id_seqv, meta_seqv)) = self.get_id_and_value(name_ident).await? else {
                return Ok(Ok(None));
            };

            let tag_id = *id_seqv.data;
            let tag_meta_key = id_seqv.data.into_t_ident(name_ident.tenant());
            let id_to_name_key = TagIdToNameIdent::new_generic(name_ident.tenant(), id_seqv.data);

            // Build the prefix for tag references: __fd_tag_id_object_ref/<tenant>/<tag_id>/
            let refs_dir = DirName::new_with_level(
                TagIdObjectRefIdent::new_generic(tenant.clone(), TagIdObjectRef::prefix(tag_id)),
                2,
            );

            // Hard delete: remove all keys
            let mut txn = TxnRequest::default();
            txn.condition.push(txn_cond_eq_seq(name_ident, id_seqv.seq));
            // Ensure no references exist before deletion
            txn.condition
                .push(txn_cond_eq_keys_with_prefix(&refs_dir, 0));
            txn.if_then.push(txn_del(name_ident)); // name -> id
            txn.if_then.push(txn_del(&tag_meta_key)); // id -> meta
            txn.if_then.push(txn_del(&id_to_name_key)); // id -> name

            let (success, _) = send_txn(self, txn).await?;
            if success {
                return Ok(Ok(Some((id_seqv, meta_seqv))));
            }

            // Transaction failed. Check if it's due to references or concurrent modification.
            let strm = self.list_pb(ListOptions::unlimited(&refs_dir)).await?;
            let refs: Vec<String> = strm
                .map_ok(|entry| entry.key.name().object.to_string())
                .try_collect()
                .await?;
            if !refs.is_empty() {
                return Ok(Err(TagError::tag_has_references(
                    name_ident.tag_name().to_string(),
                    refs,
                )));
            }
            // No references, must be concurrent modification. Retry.
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_tag(&self, name_ident: &TagNameIdent) -> Result<Option<GetTagReply>, MetaError> {
        debug!(name_ident :? =(name_ident); "SchemaApi: {}", func_name!());

        match self.get_id_value(name_ident).await? {
            Some((id_seqv, meta_seqv)) => Ok(Some(GetTagReply {
                tag_id: id_seqv,
                meta: meta_seqv,
            })),
            None => Ok(None),
        }
    }

    /// List all tags for a tenant.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_tags(&self, tenant: &Tenant) -> Result<Vec<TagInfo>, MetaError> {
        debug!(tenant :? =(tenant); "SchemaApi: {}", func_name!());

        let tag_name_key = TagNameIdent::new(tenant, "dummy");
        let tag_dir = DirName::new(tag_name_key);

        let tags = self
            .list_id_value(&tag_dir)
            .await?
            .map(|(name_ident, tag_id_seqv, meta_seqv)| TagInfo {
                name: name_ident.tag_name().to_string(),
                tag_id: tag_id_seqv,
                meta: meta_seqv,
            })
            .collect();

        Ok(tags)
    }

    /// Attach multiple tags to an object in a single transaction.
    /// Validates tag existence and allowed_values constraints.
    ///
    /// # Optimistic Locking
    ///
    /// The txn uses optimistic locking:
    /// - We must re-fetch `TagMeta` and verify values on every retry so each attempt uses the
    ///   latest `allowed_values`.
    /// - The generated conditions include `tag_meta.seq`, ensuring no concurrent update sneaks in
    ///   between validation and commit. If any `TagMeta` changes, the txn fails and the loop
    ///   re-executes the entire validation/write process.
    ///
    /// # Concurrency Safety with DROP
    ///
    /// Two mechanisms prevent orphaned tag references when DROP runs concurrently:
    ///
    /// 1. **Transaction condition**: The txn includes `seq >= 1` for the object key.
    ///    - For Stage/Connection: hard delete sets seq=0, failing the condition.
    ///
    /// 2. **Soft-delete check**: For Database/Table which use soft delete (drop_on field):
    ///    - When txn fails, we check if `drop_on` is set in the object's meta.
    ///    - If soft-deleted, we reject the SET TAG operation with ObjectNotFound.
    ///    - This prevents writing tag refs to objects that are logically deleted.
    ///
    /// The meta-api layer handles tag cleanup during DROP operations (in `drop_database_meta`
    /// and `construct_drop_table_txn_operations`), ensuring existing tag refs are removed
    /// atomically with the drop operation.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn set_object_tags(
        &self,
        req: SetObjectTagsReq,
    ) -> Result<Result<(), TagMetaError>, MetaTxnError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        if req.tags.is_empty() {
            return Ok(Ok(()));
        }

        // Build the object existence check key.
        let object_key = get_object_key(&req.tenant, &req.taggable_object);

        // Build tag meta keys for batch fetching
        let tag_meta_keys: Vec<TagIdIdent> = req
            .tags
            .iter()
            .map(|(tag_id, _)| TagId::new(*tag_id).into_t_ident(&req.tenant))
            .collect();

        let mut trials = txn_backoff(None, func_name!());

        loop {
            trials.next().unwrap()?.await;

            // Fetch tag metadata to validate allowed_values
            let tag_metas = self
                .get_pb_values_vec::<TagIdIdent, _>(tag_meta_keys.clone())
                .await?;

            // +1 for the object existence condition
            let mut txn_conditions = Vec::with_capacity(1 + req.tags.len());
            let mut txn_ops = Vec::with_capacity(req.tags.len() * 2);

            // Condition: object must exist (seq >= 1).
            // This ensures that if the object is dropped concurrently, the transaction fails.
            txn_conditions.push(TxnCondition::match_seq(
                object_key.clone(),
                ConditionResult::Ge,
                1,
            ));

            for (((tag_id, tag_value), meta_opt), tag_meta_key) in req
                .tags
                .iter()
                .zip(tag_metas.into_iter())
                .zip(tag_meta_keys.iter())
            {
                // Verify tag exists and validate allowed_values
                let Some(meta_seqv) = meta_opt else {
                    return Ok(Err(TagMetaError::not_found(&req.tenant, *tag_id)));
                };
                if let Some(allowed_values) = meta_seqv.data.allowed_values.as_ref() {
                    let allowed_lookup: HashSet<&str> =
                        allowed_values.iter().map(|value| value.as_str()).collect();
                    if !allowed_lookup.contains(tag_value.as_str()) {
                        return Ok(Err(TagMetaError::not_allowed_value(
                            *tag_id,
                            tag_value.clone(),
                            allowed_values.clone(),
                        )));
                    }
                }

                // Condition: tag meta hasn't been modified (allowed_values unchanged)
                txn_conditions.push(txn_cond_eq_seq(tag_meta_key, meta_seqv.seq));

                let obj_ref_key = ObjectTagIdRefIdent::new_generic(
                    req.tenant.clone(),
                    ObjectTagIdRef::new(req.taggable_object.clone(), *tag_id),
                );
                let tag_ref_key = TagIdObjectRefIdent::new_generic(
                    req.tenant.clone(),
                    TagIdObjectRef::new(*tag_id, req.taggable_object.clone()),
                );

                txn_ops.push(txn_put_pb_with_ttl(
                    &obj_ref_key,
                    &ObjectTagIdRefValue {
                        tag_allowed_value: tag_value.clone(),
                    },
                    None,
                )?);
                txn_ops.push(txn_put_pb_with_ttl(&tag_ref_key, &EmptyProto {}, None)?);
            }

            let (succ, _) = send_txn(self, TxnRequest::new(txn_conditions, txn_ops)).await?;
            if succ {
                return Ok(Ok(()));
            }

            // Transaction failed. Check why and decide whether to retry or return error.
            // 1. Object was dropped (hard delete: seq=0, or soft delete: drop_on is Some) → return ObjectNotFound
            // 2. Tag was modified/deleted → retry will re-fetch and re-validate
            let object_seqv = self.get_kv(&object_key).await?;
            if object_seqv.seq() == 0 {
                return Ok(Err(TagMetaError::object_not_found(
                    req.taggable_object.clone(),
                )));
            }

            // For Database and Table, also check if soft-deleted (drop_on is Some).
            // This prevents orphaned tag references when DROP runs concurrently.
            match &req.taggable_object {
                TaggableObject::Database { db_id } => {
                    let db_id_key = DatabaseId::new(*db_id);
                    if let Some(db_meta_seqv) = self.get_pb(&db_id_key).await? {
                        let db_meta: DatabaseMeta = db_meta_seqv.data;
                        if db_meta.drop_on.is_some() {
                            warn!(
                                "set_object_tags: database {} is soft-deleted, rejecting tag operation",
                                db_id
                            );
                            return Ok(Err(TagMetaError::object_not_found(
                                req.taggable_object.clone(),
                            )));
                        }
                    }
                }
                TaggableObject::Table { table_id } => {
                    let table_id_key = TableId::new(*table_id);
                    if let Some(table_meta_seqv) = self.get_pb(&table_id_key).await? {
                        let table_meta: TableMeta = table_meta_seqv.data;
                        if table_meta.drop_on.is_some() {
                            warn!(
                                "set_object_tags: table {} is soft-deleted, rejecting tag operation",
                                table_id
                            );
                            return Ok(Err(TagMetaError::object_not_found(
                                req.taggable_object.clone(),
                            )));
                        }
                    }
                }
                // Stage, Connection, UDF, and Procedure use name-based keys, no soft delete concept
                TaggableObject::Stage { .. }
                | TaggableObject::Connection { .. }
                | TaggableObject::UDF { .. }
                | TaggableObject::Procedure { .. } => {}
            }

            debug!(req :? =(&req); "set_object_tags retry due to concurrent modification");
        }
    }

    /// Remove a batch of tag bindings from an object via one transaction.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn unset_object_tags(&self, req: UnsetObjectTagsReq) -> Result<(), MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        if req.tags.is_empty() {
            return Ok(());
        }

        let mut txn_ops = Vec::with_capacity(req.tags.len() * 2);

        for tag_id in &req.tags {
            let obj_ref_key = ObjectTagIdRefIdent::new_generic(
                req.tenant.clone(),
                ObjectTagIdRef::new(req.taggable_object.clone(), *tag_id),
            );
            let tag_ref_key = TagIdObjectRefIdent::new_generic(
                req.tenant.clone(),
                TagIdObjectRef::new(*tag_id, req.taggable_object.clone()),
            );

            txn_ops.push(txn_del(&obj_ref_key));
            txn_ops.push(txn_del(&tag_ref_key));
        }

        let (succ, _) = send_txn(self, TxnRequest::new(vec![], txn_ops)).await?;
        debug_assert!(
            succ,
            "unset_object_tags txn has no conditions and should not fail"
        );
        Ok(())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_object_tags(
        &self,
        tenant: &Tenant,
        object: &TaggableObject,
    ) -> Result<Vec<ObjectTagValue>, MetaError> {
        debug!(tenant :? =(tenant), object :? =(object); "SchemaApi: {}", func_name!());

        let obj_ref_key =
            ObjectTagIdRefIdent::new_generic(tenant, ObjectTagIdRef::new(object.clone(), 0));
        let refs_dir = DirName::new(obj_ref_key);
        let stream = self.list_pb(ListOptions::unlimited(&refs_dir)).await?;
        Ok(stream
            .map_ok(|entry| ObjectTagValue {
                tag_id: entry.key.name().tag_id,
                tag_value: entry.seqv,
            })
            .try_collect::<Vec<_>>()
            .await?)
    }

    /// List all references for a tag by ID.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_tag_references(
        &self,
        tenant: &Tenant,
        tag_id: u64,
    ) -> Result<Vec<TagReferenceInfo>, MetaError> {
        debug!(tenant :? =(tenant), tag_id :? =(tag_id); "SchemaApi: {}", func_name!());

        // Collect all referenced objects
        let refs_dir = DirName::new_with_level(
            TagIdObjectRefIdent::new_generic(tenant, TagIdObjectRef::prefix(tag_id)),
            2,
        );
        let strm = self.list_pb(ListOptions::unlimited(&refs_dir)).await?;
        let refs = strm
            .map_ok(|entry| {
                let object = entry.key.name().object.clone();
                let value_key = ObjectTagIdRefIdent::new_generic(
                    tenant,
                    ObjectTagIdRef::new(object.clone(), tag_id),
                );
                (object, value_key)
            })
            .try_collect::<Vec<_>>()
            .await?;
        let (tagged_objects, value_keys): (Vec<_>, Vec<_>) = refs.into_iter().unzip();

        let tag_values = self
            .get_pb_values_vec::<ObjectTagIdRefIdent, _>(value_keys)
            .await?;

        // Combine objects with their values
        Ok(tagged_objects
            .into_iter()
            .zip(tag_values)
            .filter_map(|(obj, value_opt)| {
                value_opt.map(|value_seqv| TagReferenceInfo {
                    tag_id,
                    taggable_object: obj,
                    tag_value: value_seqv,
                })
            })
            .collect())
    }
}
