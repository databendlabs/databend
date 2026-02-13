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

use std::sync::Arc;

use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::schema::CatalogIdToNameIdent;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::ListCatalogReq;
use databend_common_meta_app::schema::catalog_id_ident::CatalogId;
use databend_common_meta_app::schema::catalog_name_ident::CatalogNameIdentRaw;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use fastrace::func_name;
use log::debug;

use super::name_id_value_api::NameIdValueApi;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::serialize_struct;

/// CatalogApi defines APIs for catalog management.
///
/// This trait handles:
/// - Catalog creation and deletion
/// - Catalog metadata queries and listing
/// - Catalog lifecycle operations
#[async_trait::async_trait]
pub trait CatalogApi
where
    Self: Send + Sync,
    Self: kvapi::KVApi<Error = MetaError>,
{
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_catalog(
        &self,
        name_ident: &CatalogNameIdent,
        meta: &CatalogMeta,
    ) -> Result<Result<CatalogId, SeqV<CatalogId>>, KVAppError> {
        debug!(name_ident :? =(&name_ident), meta :? = meta; "SchemaApi: {}", func_name!());

        let name_ident_raw = serialize_struct(&CatalogNameIdentRaw::from(name_ident))?;

        let res = self
            .create_id_value(
                name_ident,
                meta,
                false,
                |id| {
                    vec![(
                        CatalogIdToNameIdent::new_generic(name_ident.tenant(), id).to_string_key(),
                        name_ident_raw.clone(),
                    )]
                },
                |_, _| Ok(vec![]),
                |_, _| {},
            )
            .await?;

        Ok(res)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_catalog(
        &self,
        name_ident: &CatalogNameIdent,
    ) -> Result<Arc<CatalogInfo>, KVAppError> {
        debug!(req :? =name_ident; "SchemaApi: {}", func_name!());

        let (seq_id, seq_meta) = self
            .get_id_and_value(name_ident)
            .await?
            .ok_or_else(|| AppError::unknown(name_ident, func_name!()))?;

        let catalog = CatalogInfo::new(name_ident.clone(), seq_id.data, seq_meta.data);

        Ok(Arc::new(catalog))
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_catalog(
        &self,
        name_ident: &CatalogNameIdent,
    ) -> Result<Option<(SeqV<CatalogId>, SeqV<CatalogMeta>)>, KVAppError> {
        debug!(req :? =(&name_ident); "SchemaApi: {}", func_name!());

        let removed = self
            .remove_id_value(name_ident, |id| {
                vec![CatalogIdToNameIdent::new_generic(name_ident.tenant(), id).to_string_key()]
            })
            .await?;

        Ok(removed)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_catalogs(
        &self,
        req: ListCatalogReq,
    ) -> Result<Vec<Arc<CatalogInfo>>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant = req.tenant;
        let name_key = CatalogNameIdent::new(&tenant, "dummy");
        let dir = DirName::new(name_key);

        let name_id_values = self.list_id_value(&dir).await?;

        let catalog_infos = name_id_values
            .map(|(name, id_seqv, seq_meta)| {
                Arc::new(CatalogInfo::new(name, id_seqv.data, seq_meta.data))
            })
            .collect::<Vec<_>>();

        Ok(catalog_infos)
    }
}

#[async_trait::async_trait]
impl<KV> CatalogApi for KV
where
    KV: Send + Sync,
    KV: kvapi::KVApi<Error = MetaError> + ?Sized,
{
}
