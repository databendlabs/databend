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

use arrow_array::builder::StringBuilder;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_flight::utils::batches_to_flight_data;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use futures_util::stream;
use log::warn;
use tonic::Status;

use crate::servers::flight_sql::flight_sql_service::DoGetStream;

pub(super) struct CatalogInfoProvider {}

impl CatalogInfoProvider {
    fn batch_to_get_stream(batch: RecordBatch) -> Result<DoGetStream, Status> {
        let schema = (*batch.schema()).clone();
        let batches = vec![batch];
        let flight_data = batches_to_flight_data(&schema, batches)
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .into_iter()
            .map(Ok);
        let stream = stream::iter(flight_data);
        Ok(Box::pin(stream))
    }

    async fn get_tables_internal(
        ctx: Arc<dyn TableContext>,
        catalog_name: Option<String>,
        database_name: Option<String>,
    ) -> databend_common_exception::Result<(Vec<String>, Vec<String>, Vec<String>, Vec<String>)>
    {
        let tenant = ctx.get_tenant();
        let catalog_mgr = CatalogManager::instance();
        let catalogs: Vec<(String, Arc<dyn Catalog>)> = if let Some(catalog_name) = catalog_name {
            vec![(
                catalog_name.clone(),
                catalog_mgr
                    .get_catalog(tenant.tenant_name(), &catalog_name, ctx.txn_mgr())
                    .await?,
            )]
        } else {
            catalog_mgr
                .list_catalogs(&tenant, ctx.txn_mgr())
                .await?
                .iter()
                .map(|r| (r.name(), r.clone()))
                .collect()
        };

        let mut catalog_names = vec![];
        let mut database_names = vec![];
        let mut table_names = vec![];
        let mut table_types = vec![];
        let table_type = "table".to_string();
        for (catalog_name, catalog) in catalogs.into_iter() {
            let dbs = if let Some(database_name) = &database_name {
                vec![catalog.get_database(&tenant, database_name).await?]
            } else {
                catalog.list_databases(&tenant).await?
            };
            for db in dbs {
                let db_name = db.name().to_string().into_boxed_str();
                let db_name: &str = Box::leak(db_name);
                let tables = match catalog.list_tables(&tenant, db_name).await {
                    Ok(tables) => tables,
                    Err(err) if err.code() == ErrorCode::EMPTY_SHARE_ENDPOINT_CONFIG => {
                        warn!("list tables failed on db {}: {}", db.name(), err);
                        continue;
                    }
                    Err(err) => return Err(err),
                };
                for table in tables {
                    catalog_names.push(catalog_name.clone());
                    database_names.push(db_name.to_string());
                    table_names.push(table.name().to_string());
                    table_types.push(table_type.clone());
                }
            }
        }
        Ok((catalog_names, database_names, table_names, table_types))
    }

    pub(crate) async fn get_tables(
        ctx: Arc<dyn TableContext>,
        catalog_name: Option<String>,
        database_name: Option<String>,
    ) -> Result<DoGetStream, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("db_schema_name", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));
        let (catalog_name, db_schema_name, table_name, table_type) =
            Self::get_tables_internal(ctx.clone(), catalog_name, database_name)
                .await
                .map_err(|e| Status::internal(format!("{e:?}")))?;
        let batch = RecordBatch::try_new(schema, vec![
            Self::string_array(catalog_name),
            Self::string_array(db_schema_name),
            Self::string_array(table_name),
            Self::string_array(table_type),
        ])
        .map_err(|e| Status::internal(format!("RecordBatch::try_new fail {:?}", e)))?;
        Self::batch_to_get_stream(batch)
    }

    fn string_array(values: Vec<String>) -> ArrayRef {
        let mut builder = StringBuilder::new();
        for v in &values {
            builder.append_value(v);
        }
        Arc::new(builder.finish())
    }
}
