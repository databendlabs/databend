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

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_storage::check_operator;
use databend_common_storage::init_operator;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::operations::load_last_snapshot_hint;
use databend_common_storages_fuse::FUSE_OPT_KEY_ATTACH_COLUMN_IDS;
use databend_enterprise_attach_table::AttachTableHandler;
use databend_enterprise_attach_table::AttachTableHandlerWrapper;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

pub struct RealAttachTableHandler {}
#[async_trait::async_trait]
impl AttachTableHandler for RealAttachTableHandler {
    #[async_backtrace::framed]
    async fn build_attach_table_request(
        &self,
        storage_prefix: &str,
        plan: &CreateTablePlan,
    ) -> databend_common_exception::Result<CreateTableReq> {
        // Safe to unwrap here, as attach table must have storage params.
        let sp = plan.storage_params.as_ref().unwrap();
        let operator = init_operator(sp)?;
        check_operator(&operator, sp).await?;

        let snapshot_hint = load_last_snapshot_hint(storage_prefix, &operator)
            .await?
            .ok_or_else(|| {
                ErrorCode::StorageOther(format!(
                    "hint file of table {}.{} does not exist",
                    &plan.database, &plan.table
                ))
            })?;

        let reader = MetaReaders::table_snapshot_reader(operator.clone());

        // TODO duplicated code
        let snapshot_full_path = snapshot_hint.snapshot_full_path;
        let info = operator.info();
        let root = info.root();
        let snapshot_loc = snapshot_full_path[root.len()..].to_string();
        let mut options = plan.options.clone();
        options.insert(OPT_KEY_SNAPSHOT_LOCATION.to_string(), snapshot_loc.clone());

        let params = LoadParams {
            location: snapshot_loc.clone(),
            len_hint: None,
            ver: TableSnapshot::VERSION,
            put_cache: true,
        };

        let snapshot = reader.read(&params).await?;
        let stat = TableStatistics {
            number_of_rows: snapshot.summary.row_count,
            data_bytes: snapshot.summary.uncompressed_byte_size,
            compressed_data_bytes: snapshot.summary.compressed_byte_size,
            index_data_bytes: snapshot.summary.index_size,
            bloom_index_size: snapshot.summary.bloom_index_size,
            ngram_index_size: snapshot.summary.ngram_index_size,
            inverted_index_size: snapshot.summary.inverted_index_size,
            vector_index_size: snapshot.summary.vector_index_size,
            virtual_column_size: snapshot.summary.virtual_column_size,
            number_of_segments: Some(snapshot.segments.len() as u64),
            number_of_blocks: Some(snapshot.summary.block_count),
        };

        let attach_table_schema = Self::gen_schema(&plan, &snapshot)?;

        // let field_comments = vec!["".to_string(); snapshot.schema.num_fields()];
        let field_comments = snapshot_hint.entity_comments.field_comments;
        let comment = snapshot_hint.entity_comments.table_comment;
        let table_meta = TableMeta {
            schema: Arc::new(attach_table_schema),
            engine: plan.engine.to_string(),
            storage_params: plan.storage_params.clone(),
            options,
            cluster_key: None,
            comment,
            field_comments,
            drop_on: None,
            statistics: stat,
            indexes: snapshot_hint.indexes,
            ..Default::default()
        };
        let req = CreateTableReq {
            create_option: plan.create_option,
            name_ident: TableNameIdent {
                tenant: plan.tenant.clone(),
                db_name: plan.database.to_string(),
                table_name: plan.table.to_string(),
            },
            table_meta,
            as_dropped: false,
            auto_increments: plan.auto_increments.clone(),
            table_properties: None,
            table_partition: None,
        };

        Ok(req)
    }
}

impl RealAttachTableHandler {
    pub fn init() -> databend_common_exception::Result<()> {
        let rm = RealAttachTableHandler {};
        let wrapper = AttachTableHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }

    fn gen_schema(
        plan: &&CreateTablePlan,
        base_table_snapshot: &Arc<TableSnapshot>,
    ) -> Result<TableSchema> {
        let schema = if let Some(attached_columns) = &plan.attached_columns {
            // Columns to include are specified, let's check them
            let base_table_schema = &base_table_snapshot.schema;
            let mut fields_to_attach = Vec::with_capacity(attached_columns.len());

            // The ids of columns being included
            let mut field_ids_to_include = Vec::with_capacity(attached_columns.len());

            // Columns that do not exist in the table being attached to, if any
            let mut invalid_cols = vec![];
            for field in attached_columns {
                match base_table_schema.field_with_name(&field.name) {
                    Ok(f) => {
                        field_ids_to_include.push(f.column_id);
                        fields_to_attach.push(f.clone())
                    }
                    Err(_) => invalid_cols.push(field.name.as_str()),
                }
            }
            if !invalid_cols.is_empty() {
                return Err(ErrorCode::InvalidArgument(format!(
                    "Columns [{}] do not exist in the table being attached to",
                    invalid_cols.join(",")
                )));
            }

            let new_table_schema_metadata = if !field_ids_to_include.is_empty() {
                // If columns to include are specified explicitly, their ids should
                // be kept in the metadata of TableSchema.
                let ids = field_ids_to_include
                    .iter()
                    .map(|id| format!("{id}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let mut v = base_table_schema.metadata.clone();
                v.insert(FUSE_OPT_KEY_ATTACH_COLUMN_IDS.to_owned(), ids);
                v
            } else {
                base_table_schema.metadata.clone()
            };

            TableSchema {
                fields: fields_to_attach,
                metadata: new_table_schema_metadata,
                next_column_id: base_table_schema.next_column_id,
            }
        } else {
            // If columns are not specified, use all the fields of table being attached to,
            // in this case, no schema meta of key FUSE_OPT_KEY_ATTACH_COLUMN_IDS will be kept.
            base_table_snapshot.schema.clone()
        };

        Ok(schema)
    }
}
