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
use common_expression::DataSchema;
use common_expression::TableSchema;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::Pipeline;
use common_storage::DataOperator;
use common_storages_parquet::ParquetFilesPart;
use common_storages_parquet::ParquetPart;
use common_storages_parquet::ParquetRSPruner;
use common_storages_parquet::ParquetRSReaderBuilder;
use icelake::catalog::Catalog;
use opendal::Operator;
use storages_common_pruner::RangePrunerCreator;
use tokio::sync::OnceCell;

use crate::partition::IcebergPartInfo;
use crate::stats::get_stats_of_data_file;
use crate::table_source::IcebergTableSource;

/// accessor wrapper as a table
///
/// TODO: we should use icelake Table instead.
pub struct IcebergTable {
    info: TableInfo,
    op: DataOperator,

    table: OnceCell<icelake::Table>,
}

impl IcebergTable {
    /// create a new table on the table directory
    #[async_backtrace::framed]
    pub fn try_new(dop: DataOperator, info: TableInfo) -> Result<IcebergTable> {
        Ok(Self {
            info,
            op: dop,
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
        // FIXME: we should implement catalog for icelake.
        let icelake_catalog = Arc::new(icelake::catalog::StorageCatalog::new(
            "databend",
            OperatorCreatorWrapper(dop.clone()),
        ));

        let table_id = icelake::TableIdentifier::new(vec![""]).unwrap();
        let table = icelake_catalog.load_table(&table_id).await.map_err(|err| {
            ErrorCode::ReadTableDataError(format!("Iceberg catalog load failed: {err:?}"))
        })?;

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
            op: dop,
            table: OnceCell::new_with(Some(table)),
        })
    }

    async fn table(&self) -> Result<&icelake::Table> {
        self.table
            .get_or_try_init(|| async {
                // FIXME: we should implement catalog for icelake.
                let icelake_catalog = Arc::new(icelake::catalog::StorageCatalog::new(
                    "databend",
                    OperatorCreatorWrapper(self.op.clone()),
                ));

                let table_id = icelake::TableIdentifier::new(vec![""]).unwrap();
                let table = icelake_catalog.load_table(&table_id).await.map_err(|err| {
                    ErrorCode::ReadTableDataError(format!("Iceberg catalog load failed: {err:?}"))
                })?;

                Ok(table)
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

        let table_schema = self.schema();
        let arrow_schema = table_schema.to_arrow();
        let arrow_fields = arrow_schema
            .fields
            .into_iter()
            .map(|f| f.into())
            .collect::<Vec<arrow_schema::Field>>();
        let arrow_schema = arrow_schema::Schema::new(arrow_fields);
        let leaf_fields = Arc::new(table_schema.leaf_fields());

        let mut read_options = ParquetReadOptions::default();

        if !ctx.get_settings().get_enable_parquet_page_index()? {
            read_options = read_options.with_prune_pages(false);
        }

        if !ctx.get_settings().get_enable_parquet_rowgroup_pruning()? {
            read_options = read_options.with_prune_row_groups(false);
        }

        if !ctx.get_settings().get_enable_parquet_prewhere()? {
            read_options = read_options.with_do_prewhere(false);
        }

        let pruner = ParquetRSPruner::try_create(
            ctx.get_function_context()?,
            table_schema.clone(),
            leaf_fields,
            &plan.push_downs,
            read_options,
        )?;

        let mut builder = ParquetRSReaderBuilder::create(
            ctx.clone(),
            self.op.operator(),
            table_schema,
            &arrow_schema,
        )?
        .with_options(read_options)
        .with_push_downs(plan.push_downs.as_ref())
        .with_pruner(Some(pruner));

        let praquet_reader = Arc::new(builder.build_full_reader()?);

        // TODO: we need to support top_k.
        let output_schema = Arc::new(DataSchema::from(plan.schema()));
        pipeline.add_source(
            |output| {
                IcebergTableSource::create(
                    ctx.clone(),
                    output,
                    output_schema.clone(),
                    praquet_reader.clone(),
                )
            },
            max_threads.max(1),
        )
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

        let filter = push_downs.as_ref().and_then(|extra| {
            extra
                .filters
                .as_ref()
                .map(|f| f.filter.as_expr(&BUILTIN_FUNCTIONS))
        });

        let schema = self.schema();

        let pruner =
            RangePrunerCreator::try_create(ctx.get_function_context()?, &schema, filter.as_ref())?;

        // TODO: support other file formats. We only support parquet files now.
        let mut read_rows = 0;
        let mut read_bytes = 0;
        let total_files = data_files.len();
        let parts = data_files
            .into_iter()
            .filter(|df| {
                if let Some(stats) = get_stats_of_data_file(&schema, df) {
                    pruner.should_keep(&stats, None)
                } else {
                    true
                }
            })
            .map(|v: icelake::types::DataFile| {
                read_rows += v.record_count as usize;
                read_bytes += v.file_size_in_bytes as usize;
                match v.file_format {
                    icelake::types::DataFileFormat::Parquet => {
                        let location = table
                            .rel_path(&v.file_path)
                            .expect("file path must be rel to table");
                        Ok(Arc::new(
                            Box::new(IcebergPartInfo::Parquet(ParquetPart::ParquetFiles(
                                ParquetFilesPart {
                                    files: vec![(location, v.file_size_in_bytes as u64)],
                                    estimated_uncompressed_size: v.file_size_in_bytes as u64, // This field is not used here.
                                },
                            ))) as Box<dyn PartInfo>,
                        ))
                    }
                    _ => Err(ErrorCode::Unimplemented(
                        "Only parquet format is supported for iceberg table",
                    )),
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // TODO: more precise pruning.

        Ok((
            PartStatistics::new_estimated(None, read_rows, read_bytes, parts.len(), total_files),
            Partitions::create_nolazy(PartitionsShuffleKind::Mod, parts),
        ))
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

    fn support_prewhere(&self) -> bool {
        true
    }
}

struct OperatorCreatorWrapper(DataOperator);

impl icelake::catalog::OperatorCreator for OperatorCreatorWrapper {
    fn create(&self) -> icelake::Result<Operator> {
        Ok(self.0.operator())
    }

    fn create_with_subdir(&self, path: &str) -> icelake::Result<Operator> {
        let params = self.0.params().map_root(|v| format!("{}/{}", v, path));

        // The operator used to be built successfully, change root should never returns error.
        Ok(DataOperator::try_new(&params)
            .expect("invalid params")
            .operator())
    }
}
