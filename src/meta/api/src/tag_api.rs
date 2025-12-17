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

use databend_common_meta_app::schema::tag_id_ident::Resource as TagIdResource;
use databend_common_meta_app::schema::tag_id_to_name_ident::TagIdToNameIdent;
use databend_common_meta_app::schema::tag_name_ident::TagNameIdent;
use databend_common_meta_app::schema::tag_name_ident::TagNameIdentRaw;
use databend_common_meta_app::schema::CreateTagReply;
use databend_common_meta_app::schema::CreateTagReq;
use databend_common_meta_app::schema::DropTagReq;
use databend_common_meta_app::schema::EmptyProto;
use databend_common_meta_app::schema::GetObjectTagsReply;
use databend_common_meta_app::schema::GetObjectTagsReq;
use databend_common_meta_app::schema::GetTagReply;
use databend_common_meta_app::schema::GetTagReq;
use databend_common_meta_app::schema::ListTagReferencesReply;
use databend_common_meta_app::schema::ListTagReferencesReq;
use databend_common_meta_app::schema::ListTagsReply;
use databend_common_meta_app::schema::ListTagsReq;
use databend_common_meta_app::schema::ObjectTagValue;
use databend_common_meta_app::schema::SetObjectTagsReq;
use databend_common_meta_app::schema::TagInfo;
use databend_common_meta_app::schema::TagObject;
use databend_common_meta_app::schema::TagObjectType;
use databend_common_meta_app::schema::TagRefByIdIdent;
use databend_common_meta_app::schema::TagRefIdent;
use databend_common_meta_app::schema::TagRefName;
use databend_common_meta_app::schema::TagRefObject;
use databend_common_meta_app::schema::TagRefObjectValue;
use databend_common_meta_app::schema::TagReferenceInfo;
use databend_common_meta_app::schema::UnsetObjectTagsReq;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;

use crate::errors::TagError;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::name_id_value_api::NameIdValueApi;
use crate::serialization_util::serialize_struct;
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
    ) -> Result<Result<CreateTagReply, TagError>, MetaTxnError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tag_name_key = &req.name_ident;
        let name_key_raw = serialize_struct(&TagNameIdentRaw::from(tag_name_key))?;

        let result = self
            .create_id_value(
                tag_name_key,
                &req.meta,
                false,
                |id| {
                    vec![(
                        TagIdToNameIdent::new_generic(tag_name_key.tenant(), id).to_string_key(),
                        name_key_raw.clone(),
                    )]
                },
                |_, _| Ok(vec![]),
                |_, _| {},
            )
            .await?;

        Ok(match result {
            Ok(id) => Ok(CreateTagReply { tag_id: *id }),
            Err(existing) => {
                if req.if_not_exists {
                    Ok(CreateTagReply {
                        tag_id: *existing.data,
                    })
                } else {
                    Err(TagError::already_exists(
                        req.name_ident.tag_name().to_string(),
                    ))
                }
            }
        })
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_tag(&self, req: DropTagReq) -> Result<Result<(), TagError>, MetaTxnError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let Some((id_seqv, meta_seqv)) = self
                .get_id_value(&req.name_ident)
                .await
                .map_err(MetaTxnError::from)?
            else {
                if req.if_exists {
                    return Ok(Ok(()));
                }
                return Ok(Err(TagError::not_found(
                    req.name_ident.tag_name().to_string(),
                    "drop_tag",
                )));
            };

            let mut txn = TxnRequest::default();
            let tag_meta_key = id_seqv.data.into_t_ident(req.name_ident.tenant());
            let id_to_name_key =
                TagIdToNameIdent::new_generic(req.name_ident.tenant(), id_seqv.data);

            txn_delete_exact(&mut txn, &req.name_ident, id_seqv.seq);
            txn_delete_exact(&mut txn, &tag_meta_key, meta_seqv.seq);
            txn.if_then.push(txn_op_del(&id_to_name_key));

            // Ensure no references exist for any object type
            for object_type in TagObjectType::all() {
                let refs_dir =
                    tag_refs_by_id_type_dir(req.name_ident.tenant(), *id_seqv.data, object_type);
                txn.condition
                    .push(txn_cond_eq_keys_with_prefix(&refs_dir, 0));
            }

            let (success, _) = send_txn(self, txn).await?;
            if success {
                return Ok(Ok(()));
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_tag(&self, req: GetTagReq) -> Result<Option<GetTagReply>, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        match self.get_id_value(&req.name_ident).await? {
            Some((id_seqv, meta_seqv)) => Ok(Some(GetTagReply {
                tag_id: *id_seqv.data,
                meta: meta_seqv.data,
            })),
            None => Ok(None),
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_tags(&self, req: ListTagsReq) -> Result<ListTagsReply, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tag_name_key = TagNameIdent::new(&req.tenant, "dummy");
        let tag_dir = DirName::new(tag_name_key);

        let tags = self
            .list_id_value(&tag_dir)
            .await?
            .map(|(name_ident, tag_id, meta_seqv)| TagInfo {
                name: name_ident.tag_name().to_string(),
                tag_id: *tag_id,
                meta: meta_seqv.data,
            })
            .collect();

        Ok(ListTagsReply { tags })
    }

    /// Attach multiple tags to an object in a single transaction.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn set_object_tags(
        &self,
        req: SetObjectTagsReq,
    ) -> Result<Result<(), TagError>, MetaTxnError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        if req.tags.is_empty() {
            return Ok(Ok(()));
        }

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut conditions = Vec::with_capacity(req.tags.len());
            let mut txn_ops = Vec::with_capacity(req.tags.len() * 2);

            for (tag_name, tag_value) in &req.tags {
                let tag_name_key = TagNameIdent::new(&req.tenant, tag_name);
                let Some((id_seqv, meta_seqv)) = self.get_id_value(&tag_name_key).await? else {
                    return Ok(Err(TagError::not_found(
                        tag_name.clone(),
                        "set_object_tags",
                    )));
                };

                let tag_id = *id_seqv.data;
                let tag_meta_key = id_seqv.data.into_t_ident(tag_name_key.tenant());
                let object_to_tag_key = TagRefIdent::new_generic(
                    req.tenant.clone(),
                    TagRefName::new(req.object.clone(), tag_id),
                );
                let tag_to_object_key = TagRefByIdIdent::new_generic(
                    req.tenant.clone(),
                    TagRefObject::new(tag_id, req.object.clone()),
                );

                conditions.push(txn_cond_eq_seq(&tag_meta_key, meta_seqv.seq));
                txn_ops.push(txn_op_put_pb(
                    &object_to_tag_key,
                    &TagRefObjectValue {
                        value: tag_value.clone(),
                    },
                    None,
                )?);
                txn_ops.push(txn_op_put_pb(&tag_to_object_key, &EmptyProto {}, None)?);
            }

            let (success, _) = send_txn(self, TxnRequest::new(conditions, txn_ops)).await?;
            if success {
                return Ok(Ok(()));
            }
        }
    }

    /// Remove a batch of tag bindings from an object via one transaction.
    #[logcall::logcall]
    #[fastrace::trace]
    async fn unset_object_tags(
        &self,
        req: UnsetObjectTagsReq,
    ) -> Result<Result<(), TagError>, MetaTxnError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        if req.tags.is_empty() {
            return Ok(Ok(()));
        }

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut conditions = Vec::with_capacity(req.tags.len());
            let mut txn_ops = Vec::with_capacity(req.tags.len() * 2);

            for tag_name in &req.tags {
                let tag_name_key = TagNameIdent::new(&req.tenant, tag_name);
                let Some((id_seqv, meta_seqv)) = self.get_id_value(&tag_name_key).await? else {
                    return Ok(Err(TagError::not_found(
                        tag_name.clone(),
                        "unset_object_tags",
                    )));
                };

                let tag_id = *id_seqv.data;
                let tag_meta_key = id_seqv.data.into_t_ident(tag_name_key.tenant());
                let object_to_tag_key = TagRefIdent::new_generic(
                    req.tenant.clone(),
                    TagRefName::new(req.object.clone(), tag_id),
                );
                let tag_to_object_key = TagRefByIdIdent::new_generic(
                    req.tenant.clone(),
                    TagRefObject::new(tag_id, req.object.clone()),
                );

                conditions.push(txn_cond_eq_seq(&tag_meta_key, meta_seqv.seq));
                txn_ops.push(txn_op_del(&object_to_tag_key));
                txn_ops.push(txn_op_del(&tag_to_object_key));
            }

            let (success, _) = send_txn(self, TxnRequest::new(conditions, txn_ops)).await?;
            if success {
                return Ok(Ok(()));
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_object_tags(
        &self,
        req: GetObjectTagsReq,
    ) -> Result<GetObjectTagsReply, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let object_ref_key =
            TagRefIdent::new_generic(req.tenant.clone(), TagRefName::new(req.object.clone(), 0));
        let refs_dir = DirName::new(object_ref_key);
        let stream = self.list_pb(&refs_dir).await?;
        let entries = stream.try_collect::<Vec<_>>().await?;

        let tags = entries
            .into_iter()
            .map(|entry| ObjectTagValue {
                tag_id: entry.key.name().tag_id,
                tag_value: entry.seqv.data.value,
            })
            .collect();

        Ok(GetObjectTagsReply { tags })
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_tag_references(
        &self,
        req: ListTagReferencesReq,
    ) -> Result<ListTagReferencesReply, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tag_name_key = TagNameIdent::new(&req.tenant, &req.tag_name);
        let tag_id = match self.get_pb(&tag_name_key).await? {
            Some(id_seqv) => *id_seqv.data,
            None => return Ok(ListTagReferencesReply { references: vec![] }),
        };

        // Collect all referenced objects across all object types
        let mut tagged_objects: Vec<TagObject> = Vec::new();
        for object_type in TagObjectType::all() {
            let refs_dir = tag_refs_by_id_type_dir(&req.tenant, tag_id, object_type);
            let stream = self.list_pb(&refs_dir).await?;
            let entries = stream.try_collect::<Vec<_>>().await?;
            tagged_objects.extend(
                entries
                    .into_iter()
                    .map(|entry| entry.key.name().object.clone()),
            );
        }

        if tagged_objects.is_empty() {
            return Ok(ListTagReferencesReply { references: vec![] });
        }

        let value_keys: Vec<TagRefIdent> = tagged_objects
            .iter()
            .map(|obj| {
                TagRefIdent::new_generic(req.tenant.clone(), TagRefName::new(obj.clone(), tag_id))
            })
            .collect();

        let tag_values = self.get_pb_values_vec::<TagRefIdent, _>(value_keys).await?;

        // Combine objects with their values
        let references = tagged_objects
            .into_iter()
            .zip(tag_values)
            .filter_map(|(obj, value_opt)| {
                value_opt.map(|value_seqv| TagReferenceInfo {
                    tag_id,
                    object_type: obj.object_type(),
                    object_id: obj.object_id(),
                    tag_value: value_seqv.data.value,
                })
            })
            .collect();

        Ok(ListTagReferencesReply { references })
    }
}

fn tag_refs_by_id_type_dir(
    tenant: &Tenant,
    tag_id: u64,
    object_type: TagObjectType,
) -> DirName<TagRefByIdIdent> {
    let ident = TagRefByIdIdent::new_generic(
        tenant.clone(),
        TagRefObject::new(tag_id, object_type.dummy_object()),
    );
    let mut dir = DirName::new(ident);
    dir.with_level(object_type.dir_level());
    dir
}
