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

use std::any::Any;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use chrono::Utc;
use common_arrow::arrow::datatypes::Field as Arrow2Field;
use common_arrow::arrow::datatypes::Schema as Arrow2Schema;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::ParquetReadOptions;
use common_catalog::plan::PartInfo;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SourcePipeBuilder;
use common_storage::DataOperator;
use common_storages_parquet::ParquetPart;
use common_storages_parquet::ParquetPartitionPruner;
use common_storages_parquet::ParquetRSReader;
use common_storages_parquet::ParquetTable;
use tokio::sync::OnceCell;

use crate::partition::IcebergPartInfo;
use crate::table_source::IcebergTableSource;

/// accessor wrapper as a table
///
/// TODO: we should use icelake Table instead.
pub struct IcebergTable {
    info: TableInfo,
    op: opendal::Operator,

    table: OnceCell<icelake::Table>,
}

impl IcebergTable {
    /// create a new table on the table directory
    #[async_backtrace::framed]
    pub fn try_new(dop: DataOperator, info: TableInfo) -> Result<IcebergTable> {
        Ok(Self {
            info,
            op: dop.operator(),
            table: OnceCell::new(),
        })
    }

    /// create a new table on the table directory
    #[async_backtrace::framed]
    pub async fn try_create(
        catalog: &str,
        database: &str,
        table_name: &str,
        dop: DataOperator,
    ) -> Result<IcebergTable> {
        let op = dop.operator();
        let table = icelake::Table::open_with_op(op.clone())
            .await
            .map_err(|e| ErrorCode::ReadTableDataError(format!("Cannot load metadata: {e:?}")))?;

        let meta = table.current_table_metadata();

        // Build arrow schema from iceberg metadata.
        let arrow_schema: ArrowSchema = meta
            .schemas
            .last()
            .ok_or_else(|| {
                ErrorCode::ReadTableDataError("Iceberg table schema is empty".to_string())
            })?
            .clone()
            .try_into()
            .map_err(|e| {
                ErrorCode::ReadTableDataError(format!("Cannot convert table metadata: {e:?}"))
            })?;

        // Build arrow2 schema from arrow schema.
        let fields: Vec<Arrow2Field> = arrow_schema
            .fields()
            .into_iter()
            .map(|f| f.into())
            .collect();
        let arrow2_schema = Arrow2Schema::from(fields);

        let table_schema = TableSchema::from(&arrow2_schema);

        // construct table info
        let info = TableInfo {
            ident: TableIdent::new(0, 0),
            desc: format!("{database}.{table_name}"),
            name: table_name.to_string(),
            meta: TableMeta {
                schema: Arc::new(table_schema),
                catalog: catalog.to_string(),
                engine: "iceberg".to_string(),
                created_on: Utc::now(),
                storage_params: Some(dop.params()),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Self {
            info,
            op,
            table: OnceCell::new_with(Some(table)),
        })
    }

    async fn table(&self) -> Result<&icelake::Table> {
        let op = self.op.clone();

        self.table
            .get_or_try_init(|| async {
                icelake::Table::open_with_op(op).await.map_err(|e| {
                    ErrorCode::ReadTableDataError(format!("Cannot load metadata: {e:?}"))
                })
            })
            .await
    }

    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let parts_len = plan.parts.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(parts_len, max_threads);

        let table_schema: TableSchemaRef = self.info.schema();
        let projection = PushDownInfo::projection_of_push_downs(&table_schema, &plan.push_downs);

        let arrow_schema = table_schema.to_arrow();
        let arrow_fields = arrow_schema
            .fields
            .into_iter()
            .map(|f| f.into())
            .collect::<Vec<arrow_schema::Field>>();
        let arrow_schema = arrow_schema::Schema::new(arrow_fields);
        let parquet_schema =
            parquet::arrow::arrow_to_parquet_schema(&arrow_schema).map_err(|e| {
                ErrorCode::TableInfoError(format!(
                    "Convert table schema to parquet schema failed: {}",
                    e
                ))
            })?;

        let praquet_reader =
            ParquetRSReader::create(self.op.clone(), &arrow_schema, &parquet_schema, projection)?;

        // TODO: we need to support top_k.
        // TODO: we need to support prewhere.

        let mut source_builder = SourcePipeBuilder::create();
        for _ in 0..std::cmp::max(1, max_threads) {
            let output = OutputPort::create();
            source_builder.add_source(
                output.clone(),
                IcebergTableSource::create(ctx.clone(), output, praquet_reader.clone())?,
            );
        }

        pipeline.add_pipe(source_builder.finalize());
        Ok(())
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let table = self.table().await?;

        let data_files = table.current_data_files().await.map_err(|e| {
            ErrorCode::ReadTableDataError(format!("Cannot get current data files: {e:?}"))
        })?;

        // TODO: support other file formats. We only support parquet files now.
        let parquet_files = data_files
            .into_iter()
            .map(|v: icelake::types::DataFile| match v.file_format {
                icelake::types::DataFileFormat::Parquet => {
                    let path = table
                        .rel_path(&v.file_path)
                        .expect("file path must be rel to table");
                    Ok((path, v.file_size_in_bytes as u64))
                }
                _ => Err(ErrorCode::Unimplemented(
                    "Only parquet format is supported for iceberg table",
                )),
            })
            .collect::<Result<Vec<_>>>()?;

        let (stats, parts) = self
            .prune_parquet_partitions(ctx.clone(), &parquet_files, &push_downs)
            .await?;

        let parts = parts
            .into_iter()
            .map(|p| Arc::new(Box::new(IcebergPartInfo::Parquet(p)) as Box<dyn PartInfo>))
            .collect::<Vec<_>>();

        Ok((
            stats,
            Partitions::create_nolazy(PartitionsShuffleKind::Mod, parts),
        ))
    }

    async fn prune_parquet_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        files: &[(String, u64)],
        push_downs: &Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Vec<ParquetPart>)> {
        if files.is_empty() {
            return Ok((PartStatistics::default_exact(), vec![]));
        }
        let (first_file, _) = files[0].clone();
        let (arrow_schema, schema_descr, compression_ratio) =
            ParquetTable::prepare_metas(&first_file, self.op.clone()).await?;
        let pruner = ParquetPartitionPruner::try_create(
            ctx,
            &arrow_schema,
            schema_descr,
            &first_file,
            push_downs,
            ParquetReadOptions::new()
                .with_prune_row_groups(true)
                .with_do_prewhere(false)
                .with_prune_pages(false)
                .with_push_down_bitmap(false),
            compression_ratio,
            true, // Currently, don't generate small file parts.
        )?;

        pruner
            .read_and_prune_partitions(self.op.clone(), files)
            .await
    }
}

#[async_trait]
impl Table for IcebergTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_local(&self) -> bool {
        false
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.info
    }

    fn name(&self) -> &str {
        &self.get_table_info().name
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        // TODO: we will support dry run later.
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(ctx, push_downs).await
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.do_read_data(ctx, plan, pipeline)
    }

    fn table_args(&self) -> Option<TableArgs> {
        None
    }

    fn support_column_projection(&self) -> bool {
        true
    }
}
