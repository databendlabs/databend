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

use databend_common_arrow::arrow::io::parquet::read::infer_schema;
use databend_common_arrow::arrow::io::parquet::read::{self as pread};
use databend_common_arrow::parquet::read::read_metadata;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
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

const RESULT_SCAN: &str = "result_scan";

pub struct ResultScan {
    table_info: TableInfo,
    query_id: String,
    block_raw_data: Vec<u8>,
}

impl ResultScan {
    pub fn try_create(
        table_schema: TableSchema,
        query_id: String,
        block_raw_data: Vec<u8>,
    ) -> Result<Arc<dyn Table>> {
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
            block_raw_data,
        }))
    }

    pub fn from_info(info: &ResultScanTableInfo) -> Result<Arc<dyn Table>> {
        Ok(Arc::new(ResultScan {
            table_info: info.table_info.clone(),
            query_id: info.query_id.clone(),
            block_raw_data: info.block_raw_data.clone(),
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
            block_raw_data: self.block_raw_data.clone(),
        })
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        let args = vec![Scalar::String(self.query_id.as_bytes().to_vec())];

        Some(TableArgs::new_positioned(args))
    }

    fn read_data(
        &self,
        _ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        if self.block_raw_data.is_empty() {
            pipeline.add_source(EmptySource::create, 1)?;
        } else {
            let mut reader = Cursor::new(self.block_raw_data.clone());
            let meta = read_metadata(&mut reader)?;
            let arrow_schema = infer_schema(&meta)?;
            let table_schema = TableSchema::from(&arrow_schema);
            let schema = DataSchema::from(&table_schema);

            // Read the parquet file into one block.
            let chunks_iter =
                pread::FileReader::new(reader, meta.row_groups, arrow_schema, None, None, None);

            for chunk in chunks_iter {
                let block = DataBlock::from_arrow_chunk(&chunk?, &schema)?;
                pipeline.add_source(|output| OneBlockSource::create(output, block.clone()), 1)?;
            }
        }
        Ok(())
    }

    fn result_can_be_cached(&self) -> bool {
        true
    }
}
