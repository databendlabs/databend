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
use databend_common_meta_app::schema::tag_id_ident::Resource as TagIdResource;
use databend_common_meta_app::schema::tag_name_ident::TagNameIdent;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTagReply;
use databend_common_meta_app::schema::CreateTagReq;
use databend_common_meta_app::schema::DropTagReq;
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
use databend_common_meta_app::schema::TagObjectType;
use databend_common_meta_app::schema::TagRefIdent;
use databend_common_meta_app::schema::TagRefName;
use databend_common_meta_app::schema::TagRefValue;
use databend_common_meta_app::schema::TagReferenceInfo;
use databend_common_meta_app::schema::TaggableObject;
use databend_common_meta_app::schema::UnsetObjectTagsReq;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;

use crate::errors::TagError;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::name_id_value_api::NameIdValueApi;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_core_util::send_txn;
use crate::txn_op_builder_util::txn_op_del;
use crate::txn_op_builder_util::txn_op_put_pb;

#[async_trait::async_trait]
pub trait TagApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
    Self: NameIdValueApi<TagNameIdent, TagIdResource>,
{
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_tag(&self, req: CreateTagReq) -> Result<CreateTagReply, TagError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let create_res = self
            .create_id_value(
                &req.name_ident,
                &req.meta,
                false,
                |_| vec![],
                |_, _| Ok(vec![]),
                |_, _| {},
            )
            .await?;

        match create_res {
            Ok(id) => Ok(CreateTagReply { tag_id: *id }),
            Err(existent) => match req.create_option {
                CreateOption::Create => Err(TagError::already_exists(
                    req.name_ident.tag_name().to_string(),
                )),
                CreateOption::CreateIfNotExists => Ok(CreateTagReply {
                    tag_id: *existent.data,
                }),
                CreateOption::CreateOrReplace => unreachable!("CreateOrReplace handled above"),
            },
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_tag(&self, req: DropTagReq) -> Result<(), TagError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let references = collect_tag_references(self, &ListTagReferencesReq {
            tenant: req.name_ident.tenant().clone(),
            tag_name: Some(req.name_ident.tag_name().to_string()),
            object_type: None,
        })
        .await?;

        if !references.is_empty() {
            return Err(TagError::has_references(
                req.name_ident.tag_name().to_string(),
                references.len(),
            ));
        }

        let removed = self.remove_id_value(&req.name_ident, |_| vec![]).await?;

        if removed.is_none() && !req.if_exists {
            return Err(TagError::not_found(
                req.name_ident.tag_name().to_string(),
                "drop_tag",
            ));
        }
        Ok(())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_tag(&self, req: GetTagReq) -> Result<Option<GetTagReply>, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        match self.get_id_value(&req.name_ident).await? {
            Some((seq_id, seq_meta)) => Ok(Some(GetTagReply {
                tag_id: *seq_id.data,
                meta: seq_meta.data,
            })),
            None => Ok(None),
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_tags(&self, req: ListTagsReq) -> Result<ListTagsReply, MetaError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let ident = TagNameIdent::new(&req.tenant, "dummy");
        let dir = DirName::new(ident);
        let entries = self.list_id_value(&dir).await?;

        let mut tags = Vec::new();
        for (name_ident, tag_id, seq_meta) in entries {
            tags.push(TagInfo {
                name: name_ident.tag_name().to_string(),
                tag_id: *tag_id,
                meta: seq_meta.data,
            });
        }

        Ok(ListTagsReply { tags })
    }

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

        let mut bindings = Vec::with_capacity(req.tags.len());

        for (tag_name, tag_value) in &req.tags {
            let tag_ident = TagNameIdent::new(&req.tenant, tag_name);
            let Some((seq_id, seq_meta)) = self.get_id_value(&tag_ident).await? else {
                return Ok(Err(TagError::not_found(
                    tag_name.clone(),
                    "set_object_tags",
                )));
            };

            if let Some(allowed) = &seq_meta.data.allowed_values {
                if !allowed.iter().any(|v| v == tag_value) {
                    return Ok(Err(TagError::invalid_value(
                        tag_name.clone(),
                        tag_value.clone(),
                        Some(allowed.clone()),
                    )));
                }
            }

            let tag_id_ident = seq_id.data.into_t_ident(tag_ident.tenant());
            let tag_ref_ident = TagRefIdent::new_generic(
                req.tenant.clone(),
                TagRefName::new(req.object.clone(), tag_name.clone()),
            );
            bindings.push((
                tag_value.clone(),
                seq_meta.seq,
                tag_id_ident,
                tag_ref_ident,
                *seq_id.data,
            ));
        }

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;
            let mut txn = TxnRequest::default();
            for (_, meta_seq, tag_id_ident, _, _) in &bindings {
                txn.condition.push(txn_cond_eq_seq(tag_id_ident, *meta_seq));
            }

            for (value, _, _, tag_ref_ident, tag_id) in &bindings {
                let tag_ref_value = TagRefValue {
                    tag_id: *tag_id,
                    value: value.clone(),
                    created_on: Utc::now(),
                };
                txn.if_then
                    .push(txn_op_put_pb(tag_ref_ident, &tag_ref_value, None)?);
            }

            let (succ, _) = send_txn(self, txn).await?;
            if succ {
                return Ok(Ok(()));
            }
        }
    }

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

        let mut bindings = Vec::with_capacity(req.tags.len());
        for tag_name in &req.tags {
            let tag_ident = TagNameIdent::new(&req.tenant, tag_name);
            let Some((seq_id, seq_meta)) = self.get_id_value(&tag_ident).await? else {
                return Ok(Err(TagError::not_found(
                    tag_name.clone(),
                    "unset_object_tags",
                )));
            };
            let tag_id_ident = seq_id.data.into_t_ident(tag_ident.tenant());
            let tag_ref_ident = TagRefIdent::new_generic(
                req.tenant.clone(),
                TagRefName::new(req.object.clone(), tag_name.clone()),
            );
            bindings.push((seq_meta.seq, tag_id_ident, tag_ref_ident));
        }

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;
            let mut txn = TxnRequest::default();
            for (meta_seq, tag_id_ident, _) in &bindings {
                txn.condition.push(txn_cond_eq_seq(tag_id_ident, *meta_seq));
            }

            for (_, _, tag_ref_ident) in &bindings {
                txn.if_then.push(txn_op_del(tag_ref_ident));
            }

            let (succ, _) = send_txn(self, txn).await?;
            if succ {
                return Ok(Ok(()));
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_object_tags(&self, req: GetObjectTagsReq) -> Result<GetObjectTagsReply, TagError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let dir = object_dir(&req.tenant, &req.object);
        let strm = self.list_pb(&dir).await?;
        let items = strm.try_collect::<Vec<_>>().await?;

        let mut tags = Vec::with_capacity(items.len());
        for item in items {
            let name = item.key.name();
            tags.push(ObjectTagValue {
                tag_name: name.tag_name.clone(),
                tag_id: item.seqv.data.tag_id,
                tag_value: item.seqv.data.value.clone(),
                created_on: item.seqv.data.created_on,
            });
        }

        Ok(GetObjectTagsReply { tags })
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_tag_references(
        &self,
        req: ListTagReferencesReq,
    ) -> Result<ListTagReferencesReply, TagError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let references = collect_tag_references(self, &req).await?;
        Ok(ListTagReferencesReply { references })
    }
}

impl<KV> TagApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError>,
    KV: NameIdValueApi<TagNameIdent, TagIdResource>,
{
}

const ALL_OBJECT_TYPES: [TagObjectType; 4] = [
    TagObjectType::Database,
    TagObjectType::Table,
    TagObjectType::Stage,
    TagObjectType::Connection,
];

fn object_dir(tenant: &Tenant, object: &TaggableObject) -> DirName<TagRefIdent> {
    let ident = TagRefIdent::new_generic(tenant.clone(), TagRefName::new(object.clone(), "dummy"));
    DirName::new(ident)
}

fn object_type_dir(tenant: &Tenant, object_type: TagObjectType) -> DirName<TagRefIdent> {
    let dummy_object = match object_type {
        TagObjectType::Database => TaggableObject::Database { db_id: 0 },
        TagObjectType::Table => TaggableObject::Table {
            db_id: 0,
            table_id: 0,
        },
        TagObjectType::Stage => TaggableObject::Stage {
            name: "dummy".to_string(),
        },
        TagObjectType::Connection => TaggableObject::Connection {
            name: "dummy".to_string(),
        },
    };

    let ident = TagRefIdent::new_generic(tenant.clone(), TagRefName::new(dummy_object, "dummy"));
    let mut dir = DirName::new(ident);
    let level = match object_type {
        TagObjectType::Database => 2,
        TagObjectType::Table => 3,
        TagObjectType::Stage => 2,
        TagObjectType::Connection => 2,
    };
    dir.with_level(level);
    dir
}

async fn collect_tag_references<T>(
    api: &T,
    req: &ListTagReferencesReq,
) -> Result<Vec<TagReferenceInfo>, TagError>
where
    T: kvapi::KVApi<Error = MetaError> + KVPbApi + ?Sized,
{
    let types = match req.object_type {
        Some(t) => vec![t],
        None => ALL_OBJECT_TYPES.to_vec(),
    };

    let mut refs = Vec::new();
    for object_type in types {
        let dir = object_type_dir(&req.tenant, object_type);
        let strm = api.list_pb(&dir).await?;
        let items = strm.try_collect::<Vec<_>>().await?;

        for item in items {
            let name = item.key.name();
            if let Some(filter_tag) = &req.tag_name {
                if name.tag_name != *filter_tag {
                    continue;
                }
            }

            refs.push(TagReferenceInfo {
                object_type: name.object.object_type(),
                object_id: name.object.object_id(),
                tag_name: name.tag_name.clone(),
                tag_value: item.seqv.data.value.clone(),
                created_on: item.seqv.data.created_on,
            });
        }
    }
    Ok(refs)
}
