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
use databend_common_catalog::catalog::Catalog;
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
use databend_common_meta_app::schema::CatalogInfo;
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
use opendal::Operator;
use tokio::sync::OnceCell;

use crate::partition::IcebergPartInfo;
use crate::stats::get_stats_of_data_file;
use crate::table_source::IcebergTableSource;
use crate::IcebergCatalog;

pub const ICEBERG_ENGINE: &str = "ICEBERG";

/// accessor wrapper as a table
pub struct IcebergTable {
    info: TableInfo,
    ctl: IcebergCatalog,
    database_name: String,
    table_name: String,

    table: OnceCell<iceberg::table::Table>,
}

impl IcebergTable {
    /// create a new table on the table directory
    #[async_backtrace::framed]
    pub fn try_create(info: TableInfo) -> Result<Box<dyn Table>> {
        let ctl = IcebergCatalog::try_create(info.catalog_info.clone())?;
        let (db_name, table_name) = info.desc.split_once(",").ok_or(|| {
            ErrorCode::BadArguments(format!("Iceberg table desc {} is invalid", info.desc))
        })?;
        Ok(Box::new(Self {
            info,
            ctl,
            database_name: db_name.to_string(),
            table_name: table_name.to_string(),
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

    pub async fn load_iceberg_table(
        ctl: &IcebergCatalog,
        database: &str,
        table_name: &str,
    ) -> Result<iceberg::table::Table> {
        let db_ident = iceberg::NamespaceIdent::new(database.to_string());
        let table = ctl
            .iceberg_catalog()
            .load_table(&iceberg::TableIdent::new(db_ident, table_name.to_string()))
            .await
            .map_err(|err| {
                ErrorCode::ReadTableDataError(format!("Iceberg catalog load failed: {err:?}"))
            })?;
        Ok(table)
    }

    pub fn get_schema(table: &iceberg::table::Table) -> Result<TableSchema> {
        let meta = table.metadata();

        // Build arrow schema from iceberg metadata.
        let arrow_schema: ArrowSchema = meta.current_schema().try_into().map_err(|e| {
            ErrorCode::ReadTableDataError(format!("Cannot convert table metadata: {e:?}"))
        })?;
        TableSchema::try_from(&arrow_schema)
    }

    /// create a new table on the table directory
    #[async_backtrace::framed]
    pub async fn try_create_from_iceberg_catalog(
        ctl: IcebergCatalog,
        database_name: &str,
        table_name: &str,
    ) -> Result<IcebergTable> {
        let table = Self::load_iceberg_table(&ctl, database_name, table_name).await?;
        let table_schema = Self::get_schema(&table)?;

        // construct table info
        let info = TableInfo {
            ident: TableIdent::new(0, 0),
            desc: format!("{database_name}.{table_name}"),
            name: table_name.to_string(),
            meta: TableMeta {
                schema: Arc::new(table_schema),
                engine: "iceberg".to_string(),
                created_on: Utc::now(),
                ..Default::default()
            },
            catalog_info: ctl.info(),
            ..Default::default()
        };

        Ok(Self {
            info,
            ctl,
            database_name: database_name.to_string(),
            table_name: table_name.to_string(),
            table: OnceCell::new_with(Some(table)),
        })
    }

    async fn table(&self) -> Result<&iceberg::table::Table> {
        self.table
            .get_or_try_init(|| async {
                let table =
                    Self::load_iceberg_table(&self.ctl, &self.database_name, &self.table_name)
                        .await
                        .map_err(|err| {
                            ErrorCode::ReadTableDataError(format!(
                                "Iceberg catalog load failed: {err:?}"
                            ))
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

        table.metadata_ref().current_snapshot();

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
            .map(|v: iceberg::spec::DataFile| {
                read_rows += v.record_count() as usize;
                read_bytes += v.file_size_in_bytes() as usize;
                match v.file_format() {
                    iceberg::spec::DataFileFormat::Parquet => {
                        let location = table
                            .rel_path(&v.file_path())
                            .expect("file path must be rel to table");
                        Ok(Arc::new(
                            Box::new(IcebergPartInfo::Parquet(ParquetPart::ParquetFiles(
                                ParquetFilesPart {
                                    files: vec![(location, v.file_size_in_bytes() as u64)],
                                    estimated_uncompressed_size: v.file_size_in_bytes() as u64, // This field is not used here.
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
