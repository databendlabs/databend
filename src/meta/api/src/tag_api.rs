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

use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::schema::tag::id_ident::Resource as TagIdResource;
use databend_common_meta_app::schema::tag::id_ident::TagId;
use databend_common_meta_app::schema::tag::id_to_name_ident::TagIdToNameIdent;
use databend_common_meta_app::schema::tag::name_ident::Resource as TagNameResource;
use databend_common_meta_app::schema::tag::name_ident::TagNameIdent;
use databend_common_meta_app::schema::CreateTagReply;
use databend_common_meta_app::schema::CreateTagReq;
use databend_common_meta_app::schema::EmptyProto;
use databend_common_meta_app::schema::GetObjectTagsReply;
use databend_common_meta_app::schema::GetObjectTagsReq;
use databend_common_meta_app::schema::GetTagReply;
use databend_common_meta_app::schema::ListTagReferencesReply;
use databend_common_meta_app::schema::ListTagReferencesReq;
use databend_common_meta_app::schema::ObjectTagIdRef;
use databend_common_meta_app::schema::ObjectTagIdRefIdent;
use databend_common_meta_app::schema::ObjectTagIdRefValue;
use databend_common_meta_app::schema::ObjectTagValue;
use databend_common_meta_app::schema::SetObjectTagsReq;
use databend_common_meta_app::schema::TagIdIdent;
use databend_common_meta_app::schema::TagIdObjectRef;
use databend_common_meta_app::schema::TagIdObjectRefIdent;
use databend_common_meta_app::schema::TagInfo;
use databend_common_meta_app::schema::TagMeta;
use databend_common_meta_app::schema::TagReferenceInfo;
use databend_common_meta_app::schema::TagableObject;
use databend_common_meta_app::schema::UnsetObjectTagsReq;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;

use crate::errors::TagError;
use crate::fetch_id;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::name_id_value_api::NameIdValueApi;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_keys_with_prefix;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_core_util::send_txn;
use crate::txn_core_util::txn_delete_exact;
use crate::txn_op_builder_util::txn_op_del;
use crate::txn_op_builder_util::txn_op_put_pb;

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
            txn_op_put_pb(&name_ident, &tag_id, None)?, // name -> id
            txn_op_put_pb(&id_ident, &meta, None)?,     // id -> meta
            txn_op_put_pb(&id_to_name_ident, &name_raw, None)?, // id -> name
        ]);

        let (succ, _responses) = send_txn(self, txn).await?;

        if succ {
            Ok(Ok(CreateTagReply { tag_id: *tag_id }))
        } else {
            Ok(Err(name_ident.exist_error(format!(
                "tag '{}' already exists",
                name_ident.tag_name()
            ))))
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_tag(
        &self,
        name_ident: TagNameIdent,
    ) -> Result<Result<Option<(SeqV<TagId>, SeqV<TagMeta>)>, TagError>, MetaTxnError> {
        debug!(name_ident :? =(&name_ident); "SchemaApi: {}", func_name!());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let Some((id_seqv, meta_seqv)) = self
                .get_id_value(&name_ident)
                .await
                .map_err(MetaTxnError::from)?
            else {
                return Ok(Ok(None));
            };

            let tenant = name_ident.tenant();
            let tag_id = *id_seqv.data;

            // Check if tag has any references
            let refs_dir = DirName::new_with_level(
                TagIdObjectRefIdent::new_generic(tenant.clone(), TagIdObjectRef::prefix(tag_id)),
                1,
            );
            let entries = self
                .list_pb_vec(&refs_dir)
                .await
                .map_err(MetaTxnError::from)?;
            let reference_count = entries.len();

            if reference_count > 0 {
                return Ok(Err(TagError::has_references(
                    name_ident.tag_name().to_string(),
                    reference_count,
                )));
            }

            let mut txn = TxnRequest::default();
            let tag_meta_key = id_seqv.data.into_t_ident(name_ident.tenant());
            let id_to_name_key = TagIdToNameIdent::new_generic(name_ident.tenant(), id_seqv.data);

            txn_delete_exact(&mut txn, &name_ident, id_seqv.seq);
            txn_delete_exact(&mut txn, &tag_meta_key, meta_seqv.seq);
            txn.if_then.push(txn_op_del(&id_to_name_key));

            // Ensure no references were added during the transaction
            txn.condition
                .push(txn_cond_eq_keys_with_prefix(&refs_dir, 0));

            let (success, _) = send_txn(self, txn).await?;
            if success {
                return Ok(Ok(Some((id_seqv, meta_seqv))));
            }
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

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_tags(&self, tenant: &Tenant) -> Result<Vec<TagInfo>, MetaError> {
        debug!(tenant :? =(tenant); "SchemaApi: {}", func_name!());

        let tag_name_key = TagNameIdent::new(tenant, "dummy");
        let tag_dir = DirName::new(tag_name_key);

        let tags = self
            .list_id_value(&tag_dir)
            .await?
            .map(|(name_ident, tag_id, meta_seqv)| TagInfo {
                name: name_ident.tag_name().to_string(),
                tag_id: *tag_id,
                meta: meta_seqv,
            })
            .collect();

        Ok(tags)
    }

    /// Attach multiple tags to an object in a single transaction.
    /// Validates tag existence and allowed_values constraints.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn set_object_tags(
        &self,
        req: SetObjectTagsReq,
    ) -> Result<Result<(), TagError>, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        if req.tags.is_empty() {
            return Ok(Ok(()));
        }

        // Batch fetch all tag metadata to validate allowed_values
        let tag_meta_keys: Vec<TagIdIdent> = req
            .tags
            .iter()
            .map(|(tag_id, _)| TagId::new(*tag_id).into_t_ident(&req.tenant))
            .collect();

        let tag_metas = self
            .get_pb_values_vec::<TagIdIdent, _>(tag_meta_keys.clone())
            .await?;

        let mut txn_conditions = Vec::with_capacity(req.tags.len());
        let mut txn_ops = Vec::with_capacity(req.tags.len() * 2);

        for (((tag_id, tag_value), meta_opt), tag_meta_key) in
            req.tags.iter().zip(tag_metas).zip(tag_meta_keys)
        {
            // Verify tag exists
            let Some(meta_seqv) = meta_opt else {
                return Ok(Err(TagError::not_found(*tag_id)));
            };
            // Check allowed_values: None means any value is allowed
            if let Some(allowed) = &meta_seqv.data.allowed_values {
                if !allowed.contains(tag_value) {
                    return Ok(Err(TagError::invalid_value(
                        *tag_id,
                        tag_value.clone(),
                        Some(allowed.clone()),
                    )));
                }
            }
            // Add condition to ensure tag meta hasn't been modified
            // object set value must be occurs in allowed_values if it's not none.
            txn_conditions.push(txn_cond_eq_seq(&tag_meta_key, meta_seqv.seq));

            let obj_ref_key = ObjectTagIdRefIdent::new_generic(
                req.tenant.clone(),
                ObjectTagIdRef::new(req.object.clone(), *tag_id),
            );
            let tag_ref_key = TagIdObjectRefIdent::new_generic(
                req.tenant.clone(),
                TagIdObjectRef::new(*tag_id, req.object.clone()),
            );

            txn_ops.push(txn_op_put_pb(
                &obj_ref_key,
                &ObjectTagIdRefValue {
                    value: tag_value.clone(),
                },
                None,
            )?);
            txn_ops.push(txn_op_put_pb(&tag_ref_key, &EmptyProto {}, None)?);
        }

        send_txn(self, TxnRequest::new(txn_conditions, txn_ops)).await?;
        Ok(Ok(()))
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
                ObjectTagIdRef::new(req.object.clone(), *tag_id),
            );
            let tag_ref_key = TagIdObjectRefIdent::new_generic(
                req.tenant.clone(),
                TagIdObjectRef::new(*tag_id, req.object.clone()),
            );

            txn_ops.push(txn_op_del(&obj_ref_key));
            txn_ops.push(txn_op_del(&tag_ref_key));
        }

        send_txn(self, TxnRequest::new(vec![], txn_ops)).await?;
        Ok(())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_object_tags(
        &self,
        req: GetObjectTagsReq,
    ) -> Result<GetObjectTagsReply, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let obj_ref_key = ObjectTagIdRefIdent::new_generic(
            req.tenant.clone(),
            ObjectTagIdRef::new(req.object.clone(), 0),
        );
        let refs_dir = DirName::new(obj_ref_key);
        let stream = self.list_pb(&refs_dir).await?;
        let entries = stream.try_collect::<Vec<_>>().await?;

        let tags = entries
            .into_iter()
            .map(|entry| ObjectTagValue {
                tag_id: entry.key.name().tag_id,
                tag_value: entry.seqv,
            })
            .collect();

        Ok(GetObjectTagsReply { tags })
    }

    /// List all references for a tag by ID.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_tag_references(
        &self,
        req: ListTagReferencesReq,
    ) -> Result<ListTagReferencesReply, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tag_id = req.tag_id;

        // Collect all referenced objects
        let refs_dir = DirName::new_with_level(
            TagIdObjectRefIdent::new_generic(req.tenant.clone(), TagIdObjectRef::prefix(tag_id)),
            1,
        );
        let stream = self.list_pb(&refs_dir).await?;
        let entries = stream.try_collect::<Vec<_>>().await?;

        let tagged_objects: Vec<TagableObject> = entries
            .into_iter()
            .map(|entry| entry.key.name().object.clone())
            .collect();

        let value_keys: Vec<ObjectTagIdRefIdent> = tagged_objects
            .iter()
            .map(|obj| {
                ObjectTagIdRefIdent::new_generic(
                    req.tenant.clone(),
                    ObjectTagIdRef::new(obj.clone(), tag_id),
                )
            })
            .collect();

        let tag_values = self
            .get_pb_values_vec::<ObjectTagIdRefIdent, _>(value_keys)
            .await?;

        // Combine objects with their values
        let references = tagged_objects
            .into_iter()
            .zip(tag_values)
            .filter_map(|(obj, value_opt)| {
                value_opt.map(|value_seqv| TagReferenceInfo {
                    tag_id,
                    object: obj,
                    tag_value: value_seqv,
                })
            })
            .collect();

        Ok(ListTagReferencesReply { references })
    }
}
