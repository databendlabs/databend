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
use std::io::Cursor;
use std::sync::Arc;

use arrow::datatypes::Schema;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ResultScanTableInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::EmptySource;
use databend_common_pipeline_sources::OneBlockSource;
use databend_common_storage::parquet_rs::infer_schema_with_extension;
use databend_common_storage::read_metadata_async;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ParquetFilesPart;
use databend_common_storages_parquet::ParquetPart;
use databend_common_storages_parquet::ParquetRSReaderBuilder;
use databend_common_storages_parquet::ParquetSource;
use parquet::file::metadata::ParquetMetaData;

const RESULT_SCAN: &str = "result_scan";

pub struct ResultScan {
    table_info: TableInfo,
    query_id: String,
    location: String,
    schema: Schema,
    file_size: u64,
}

impl ResultScan {
    pub async fn try_create(query_id: String, location: String) -> Result<Arc<dyn Table>> {
        let op = DataOperator::instance().operator();
        let file_size = op.stat(&location).await?.content_length();
        let metadata = read_metadata_async(&location, &op, Some(file_size)).await?;
        let schema = infer_schema_with_extension(metadata.file_metadata())?;
        let table_schema = TableSchema::try_from(&schema)?;

        let table_info = TableInfo {
            ident: TableIdent::new(0, 0),
            desc: format!("''.'{RESULT_SCAN}'"),
            name: String::from(RESULT_SCAN),
            meta: TableMeta {
                schema: Arc::new(table_schema),
                engine: String::from(RESULT_SCAN),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(ResultScan {
            table_info,
            query_id,
            schema,
            location,
            file_size,
        }))
    }

    pub fn from_info(info: &ResultScanTableInfo) -> Result<Arc<dyn Table>> {
        Ok(Arc::new(ResultScan {
            table_info: info.table_info.clone(),
            query_id: info.query_id.clone(),
            location: info.location.clone(),
            schema: info.schema.clone(),
            file_size: info.file_size,
        }))
    }
}

#[async_trait::async_trait]
impl Table for ResultScan {
    fn is_local(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_data_source_info(&self) -> DataSourceInfo {
        DataSourceInfo::ResultScanSource(ResultScanTableInfo {
            table_info: self.table_info.clone(),
            query_id: self.query_id.clone(),
            location: self.location.clone(),
            schema: self.schema.clone(),
            file_size: self.file_size,
        })
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let part = ParquetPart::ParquetFiles(ParquetFilesPart {
            files: vec![(self.location.clone(), self.file_size)],
            estimated_uncompressed_size: self.file_size,
        });

        let part_info: Box<dyn PartInfo> = Box::new(part);
        Ok((
            PartStatistics::default(),
            Partitions::create(PartitionsShuffleKind::Mod, vec![Arc::new(part_info) as _]),
        ))
    }

    fn table_args(&self) -> Option<TableArgs> {
        let args = vec![Scalar::String(self.query_id.clone())];

        Some(TableArgs::new_positioned(args))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        let read_options = ParquetReadOptions::default();
        let op = DataOperator::instance().operator();
        let mut builder = ParquetRSReaderBuilder::create(
            ctx.clone(),
            op,
            self.table_info.schema(),
            self.schema.clone(),
        )?
        .with_options(read_options);
        let row_group_reader = Arc::new(builder.build_row_group_reader()?);
        let full_file_reader = Some(Arc::new(builder.build_full_reader()?));

        pipeline.add_source(
            |output| {
                ParquetSource::create(
                    ctx.clone(),
                    output,
                    row_group_reader.clone(),
                    full_file_reader.clone(),
                    Arc::new(None),
                )
            },
            1,
        )
    }

    fn result_can_be_cached(&self) -> bool {
        true
    }
}
