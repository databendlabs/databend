//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;
use std::fs::File;
use std::sync::Arc;

use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Utc;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet;
use common_arrow::arrow::io::parquet::read::schema::parquet_to_arrow_schema;
use common_arrow::parquet::metadata::FileMetaData;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_function::TableFunction;
use common_datavalues::DataSchema;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::Pipeline;
use common_storages_fuse::TableContext;
use opendal::Operator;

use crate::storages::Table;
use crate::table_functions::TableArgs;

pub struct ParquetFileMeta {
    pub location: String,
    pub file_meta: FileMetaData,
}

pub struct ParquetTable {
    table_args: Vec<DataValue>,

    pub(super) table_info: TableInfo,
    pub(super) file_metas: Vec<ParquetFileMeta>,
    pub(super) operator: Operator,
}

impl ParquetTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        if table_args.is_none() || table_args.as_ref().unwrap().is_empty() {
            return Err(ErrorCode::BadArguments(
                "read_parquet needs at least one argument",
            ));
        }

        let table_args = table_args.unwrap();

        // TODO: support glob pattern
        let file_metas = table_args
            .iter()
            .map(|arg| {
                let location = match arg {
                    DataValue::String(s) => String::from_utf8_lossy(s).to_string(),
                    _ => {
                        return Err(ErrorCode::BadArguments(
                            "read_parquet only accepts string arguments",
                        ));
                    }
                };
                let file_meta = read_parquet_meta(&location)?;
                Ok(ParquetFileMeta {
                    location,
                    file_meta,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Infer schema from the first parquet file.
        // Assume all parquet files have the same schema.
        // If not, throw error during reading.
        let schema = infer_schema(&file_metas[0].file_meta);

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Arc::new(schema),
                engine: "SystemReadParquet".to_string(),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp(0, 0)),
                updated_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp(0, 0)),
                ..Default::default()
            },
            ..Default::default()
        };

        let mut builder = opendal::services::fs::Builder::default();
        builder.root("/");
        let operator = Operator::new(builder.build()?);

        Ok(Arc::new(ParquetTable {
            table_args,
            table_info,
            file_metas,
            operator,
        }))
    }
}

#[async_trait::async_trait]
impl Table for ParquetTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn benefit_column_prune(&self) -> bool {
        true
    }

    fn support_prewhere(&self) -> bool {
        true
    }

    fn has_exact_total_row_count(&self) -> bool {
        true
    }

    fn table_args(&self) -> Option<Vec<DataValue>> {
        Some(self.table_args.clone())
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(push_down)
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.do_read_data(ctx, plan, pipeline)
    }
}

impl TableFunction for ParquetTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

fn read_parquet_meta(file: &str) -> Result<FileMetaData> {
    let mut file = File::open(file)
        .map_err(|e| ErrorCode::Internal(format!("Failed to open file {}: {}", file, e)))?;
    parquet::read::read_metadata(&mut file)
        .map_err(|e| ErrorCode::Internal(format!("Read parquet file meta error: {}", e)))
}

/// Infer [`DataSchema`] from [`FileMetaData`]
fn infer_schema(metas: &FileMetaData) -> DataSchema {
    assert!(!metas.row_groups.is_empty());

    let column_metas = metas.row_groups[0].columns();
    let parquet_fields = column_metas
        .iter()
        .map(|col_meta| col_meta.descriptor().base_type.clone())
        .collect::<Vec<_>>();
    let arrow_fields = ArrowSchema::from(parquet_to_arrow_schema(&parquet_fields));

    DataSchema::from(&arrow_fields)
}
