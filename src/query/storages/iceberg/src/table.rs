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
use databend_common_arrow::arrow::datatypes::Field as Arrow2Field;
use databend_common_arrow::arrow::datatypes::Schema as Arrow2Schema;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchema;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::storage::StorageParams;
use databend_common_pipeline_core::Pipeline;
use databend_common_storage::init_operator;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ParquetFilesPart;
use databend_common_storages_parquet::ParquetPart;
use databend_common_storages_parquet::ParquetRSPruner;
use databend_common_storages_parquet::ParquetRSReaderBuilder;
use databend_storages_common_pruner::RangePrunerCreator;
use icelake::catalog::Catalog;
use opendal::Operator;
use tokio::sync::OnceCell;

use crate::partition::IcebergPartInfo;
use crate::stats::get_stats_of_data_file;
use crate::table_source::IcebergTableSource;

pub const ICEBERG_ENGINE: &str = "ICEBERG";

/// accessor wrapper as a table
///
/// TODO: we should use icelake Table instead.
pub struct IcebergTable {
    info: TableInfo,
    table: OnceCell<icelake::Table>,
}

impl IcebergTable {
    /// create a new table on the table directory
    #[async_backtrace::framed]
    pub fn try_create(info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Box::new(Self {
            info,
            table: OnceCell::new(),
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: ICEBERG_ENGINE.to_string(),
            comment: "ICEBERG Storage Engine".to_string(),
            support_cluster_key: false,
        }
    }

    fn get_storage_params(&self) -> Result<&StorageParams> {
        self.info.meta.storage_params.as_ref().ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "Iceberg table {} must have storage parameters",
                self.info.name
            ))
        })
    }

    pub async fn load_iceberg_table(dop: DataOperator) -> Result<icelake::Table> {
        // FIXME: we should implement catalog for icelake.
        let icelake_catalog = Arc::new(icelake::catalog::StorageCatalog::new(
            OperatorCreatorWrapper(dop.clone()),
        ));

        let table_id = icelake::TableIdentifier::new(vec![""]).unwrap();
        icelake_catalog.load_table(&table_id).await.map_err(|err| {
            ErrorCode::ReadTableDataError(format!("Iceberg catalog load failed: {err:?}"))
        })
    }

    pub async fn get_schema(table: &icelake::Table) -> Result<TableSchema> {
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

        TableSchema::try_from(&arrow2_schema)
    }

    /// create a new table on the table directory
    #[async_backtrace::framed]
    pub async fn try_create_from_iceberg_catalog(
        catalog: &str,
        database: &str,
        table_name: &str,
        dop: DataOperator,
    ) -> Result<IcebergTable> {
        let table = Self::load_iceberg_table(dop.clone()).await?;
        let table_schema = Self::get_schema(&table).await?;

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
            table: OnceCell::new_with(Some(table)),
        })
    }

    async fn table(&self) -> Result<&icelake::Table> {
        self.table
            .get_or_try_init(|| async {
                let sp = self.get_storage_params()?;
                let op = DataOperator::try_new(sp)?;
                // FIXME: we should implement catalog for icelake.
                let icelake_catalog = Arc::new(icelake::catalog::StorageCatalog::new(
                    OperatorCreatorWrapper(op),
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
        let arrow_schema = table_schema.as_ref().into();
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
            vec![],
        )?;

        let sp = self.get_storage_params()?;
        let op = init_operator(sp)?;
        let mut builder =
            ParquetRSReaderBuilder::create(ctx.clone(), op, table_schema, &arrow_schema)?
                .with_options(read_options)
                .with_push_downs(plan.push_downs.as_ref())
                .with_pruner(Some(pruner));

        let parquet_reader = Arc::new(builder.build_full_reader()?);

        // TODO: we need to support top_k.
        let output_schema = Arc::new(DataSchema::from(plan.schema()));
        pipeline.add_source(
            |output| {
                IcebergTableSource::create(
                    ctx.clone(),
                    output,
                    output_schema.clone(),
                    parquet_reader.clone(),
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
            Partitions::create(PartitionsShuffleKind::Mod, parts),
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
        _put_cache: bool,
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
