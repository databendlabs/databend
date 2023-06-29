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
use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_arrow::parquet::metadata::SchemaDescriptor as Parquet2SchemaDescriptor;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfo;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataSchema;
use common_expression::DataSchemaRefExt;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::Pipeline;
use common_storage::DataOperator;
use common_storages_parquet::calc_parallelism;
use common_storages_parquet::AsyncParquetSource;
use common_storages_parquet::ParquetDeserializeTransform;
use common_storages_parquet::ParquetPart;
use common_storages_parquet::ParquetReader;
use common_storages_parquet::ParquetSmallFilesPart;
use common_storages_parquet::PartitionPruner;
use common_storages_parquet::SyncParquetSource;

/// accessor wrapper as a table
///
/// TODO: we should use icelake Table instead.
pub struct IcebergTable {
    info: TableInfo,
    op: opendal::Operator,

    /// REMOVE ME: we should remove this field after arrow2 migration.
    arrow2_schema: Arc<Arrow2Schema>,
    /// REMOVE ME: we should remove this field after arrow2 migration.
    parquet2_schema: Arc<Parquet2SchemaDescriptor>,
    table: icelake::Table,
}

impl IcebergTable {
    /// create a new table on the table directory
    ///
    /// TODO: we should use icelake Table instead.
    #[async_backtrace::framed]
    pub async fn try_create(
        catalog: &str,
        database: &str,
        table_name: &str,
        tbl_root: DataOperator,
    ) -> Result<IcebergTable> {
        let op = tbl_root.operator();
        let mut table = icelake::Table::new(op.clone());
        table
            .load()
            .await
            .map_err(|e| ErrorCode::ReadTableDataError(format!("Cannot load metadata: {e:?}")))?;

        let meta = table.current_table_metadata().map_err(|e| {
            ErrorCode::ReadTableDataError(format!("Cannot get current table metadata: {e:?}"))
        })?;

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
        let fileds: Vec<Arrow2Field> = arrow_schema
            .fields()
            .into_iter()
            .map(|f| f.into())
            .collect();
        let arrow2_schema = Arrow2Schema::from(fileds);

        // Build parquet schema from arrow2 schema.
        let parquet2_schema = to_parquet_schema(&arrow2_schema).map_err(|err| {
            ErrorCode::ReadTableDataError(format!(
                "Cannot convert iceberg metadata to data schema: {err:?}"
            ))
        })?;

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
                storage_params: Some(tbl_root.params()),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Self {
            info,
            op,
            arrow2_schema: Arc::new(arrow2_schema),
            parquet2_schema: Arc::new(parquet2_schema),
            table,
        })
    }

    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let table_schema: TableSchemaRef = self.info.schema();
        let source_projection =
            PushDownInfo::projection_of_push_downs(&table_schema, &plan.push_downs);

        // The front of the src_fields are prewhere columns (if exist).
        // The back of the src_fields are remain columns.
        let mut src_fields = Vec::with_capacity(source_projection.len());

        // The schema of the data block `read_data` output.
        let output_schema: Arc<DataSchema> = Arc::new(plan.schema().into());

        // Build the reader for parquet source.
        let source_reader = ParquetReader::create(
            self.op.clone(),
            &self.arrow2_schema,
            &self.parquet2_schema,
            source_projection,
        )?;

        // TODO: we need to support top_k.
        // TODO: we need to support prewhere.

        // Since we don't support prewhere, we can use the source_reader directly.
        src_fields.extend_from_slice(source_reader.output_schema.fields());
        let src_schema = DataSchemaRefExt::create(src_fields);
        let is_blocking = self.op.info().can_blocking();

        let (num_reader, num_deserializer) = calc_parallelism(&ctx, plan, is_blocking)?;
        if is_blocking {
            pipeline.add_source(
                |output| SyncParquetSource::create(ctx.clone(), output, source_reader.clone()),
                num_reader,
            )?;
        } else {
            pipeline.add_source(
                |output| AsyncParquetSource::create(ctx.clone(), output, source_reader.clone()),
                num_reader,
            )?;
        };

        pipeline.try_resize(num_deserializer)?;

        pipeline.add_transform(|input, output| {
            ParquetDeserializeTransform::create(
                ctx.clone(),
                input,
                output,
                src_schema.clone(),
                output_schema.clone(),
                None,
                source_reader.clone(),
                source_reader.clone(),
                self.create_pruner(ctx.clone(), plan.push_downs.clone(), true)?,
            )
        })
    }

    #[tracing::instrument(level = "info", skip(self, _ctx))]
    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
    ) -> Result<(PartStatistics, Partitions)> {
        let data_files = self.table.current_data_files().await.map_err(|e| {
            ErrorCode::ReadTableDataError(format!("Cannot get current data files: {e:?}"))
        })?;

        let partitions = data_files
            .into_iter()
            .map(|v: icelake::types::DataFile| match v.file_format {
                icelake::types::DataFileFormat::Parquet => {
                    // FIXME: this is wrong of course.
                    //
                    // We should return the real part while possible.
                    Arc::new(Box::new(ParquetPart::SmallFiles(ParquetSmallFilesPart {
                        files: vec![(
                            // FIXME: we need better API.
                            self.table
                                .rel_path(&v.file_path)
                                .expect("rel_path must be correct"),
                            v.file_size_in_bytes as u64,
                        )],
                    })) as Box<dyn PartInfo>)
                }
                _ => {
                    unimplemented!("Only parquet format is supported for iceberg table")
                }
            })
            .collect();

        Ok((
            PartStatistics::default(),
            Partitions::create_nolazy(PartitionsShuffleKind::Seq, partitions),
        ))
    }

    fn create_pruner(
        &self,
        ctx: Arc<dyn TableContext>,
        // TODO: we should support push down later.
        _push_down: Option<PushDownInfo>,
        is_small_file: bool,
    ) -> Result<PartitionPruner> {
        let parquet_fast_read_bytes = if is_small_file {
            0_usize
        } else {
            ctx.get_settings().get_parquet_fast_read_bytes()? as usize
        };

        let projection = {
            let indices = (0..self.arrow2_schema.fields.len()).collect::<Vec<usize>>();
            Projection::Columns(indices)
        };

        let (_, projected_column_nodes, _, columns_to_read) =
            ParquetReader::do_projection(&self.arrow2_schema, &self.parquet2_schema, &projection)?;

        Ok(PartitionPruner {
            schema: self.schema(),
            row_group_pruner: None,
            page_pruners: None,
            columns_to_read,
            column_nodes: projected_column_nodes,
            skip_pruning: true,
            top_k: None,
            parquet_fast_read_bytes,
        })
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
        // TODO: we will support push down later.
        _push_downs: Option<PushDownInfo>,
        // TODO: we will support dry run later.
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(ctx).await
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
}
