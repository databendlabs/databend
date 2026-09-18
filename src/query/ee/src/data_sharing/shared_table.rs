// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::TableApi;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_enterprise_data_sharing::ShareMetaStore;
use databend_enterprise_data_sharing::ShareTableContext;

use super::storage::resolve_share_storage_params;
use crate::meta_service_error;

async fn table_info(
    meta: &ShareMetaStore,
    database: &str,
    context: &ShareTableContext,
) -> Result<TableInfo> {
    let table_name = meta
        .get_pb(&TableIdToName {
            table_id: context.provider_table_id,
        })
        .await
        .map_err(meta_service_error)?
        .map(|name| name.data.table_name)
        .unwrap_or_else(|| context.provider_table.clone());
    let name_ident = DBIdTableName::new(context.binding.provider_database_id, &table_name);
    let table_niv = meta
        .get_table_in_db(&name_ident)
        .await
        .map_err(meta_service_error)?;

    let Some(table_niv) = table_niv else {
        return Err(AppError::from(UnknownTable::new(
            &table_name,
            format!(
                "shared table id {} in provider database {}",
                context.provider_table_id, context.binding.provider_database_id
            ),
        ))
        .into());
    };

    let (_name, id, seq_meta) = table_niv.unpack();
    if id.table_id != context.provider_table_id {
        return Err(ErrorCode::InvalidOperation(format!(
            "Shared table binding is stale: expected provider table id {}, got {}",
            context.provider_table_id, id.table_id
        )));
    }

    Ok(TableInfo {
        ident: TableIdent {
            table_id: id.table_id,
            seq: seq_meta.seq,
        },
        desc: format!("'{}'.'{}'", database, table_name),
        name: table_name,
        meta: seq_meta.data,
        db_type: DatabaseType::SharedDB,
        catalog_info: Default::default(),
    })
}

pub(super) async fn get_shared_table_info(
    meta: ShareMetaStore,
    database: &str,
    context: ShareTableContext,
) -> Result<TableInfo> {
    let mut table_info = table_info(&meta, database, &context).await?;
    let provider_storage = context
        .storage_params
        .or_else(|| table_info.meta.storage_params.take())
        .unwrap_or_else(|| GlobalConfig::instance().storage.params.clone());
    let storage = resolve_share_storage_params(
        &Tenant::new_literal(&context.binding.provider_tenant),
        &context.connection,
        provider_storage,
    )
    .await?;
    table_info.name = context.provider_table;
    table_info.desc = format!("'{}'.'{}'", database, table_info.name);
    table_info.meta.storage_params = Some(storage);
    Ok(table_info)
}
