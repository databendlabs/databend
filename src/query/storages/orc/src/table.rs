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
use chrono::DateTime;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::OrcTableInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::Pipeline;
use databend_common_storage::init_stage_operator;
use databend_common_storage::StageFileInfo;
use databend_storages_common_table_meta::table::ChangeType;
use opendal::Operator;
use orc_rust::ArrowReaderBuilder;

use crate::chunk_reader_impl::OrcChunkReader;
use crate::read_partition::read_partitions_simple;

pub struct OrcTable {
    pub(super) stage_table_info: StageTableInfo,
    pub(super) arrow_schema: arrow_schema::SchemaRef,
    pub(super) schema_from: String,
    pub(super) table_info: TableInfo,
}

impl OrcTable {
    pub fn from_info(info: &OrcTableInfo) -> Result<Arc<dyn Table>> {
        let table_info = create_orc_table_info(
            info.stage_table_info.schema.clone(),
            &info.stage_table_info.stage_info,
        )?;
        Ok(Arc::new(OrcTable {
            stage_table_info: info.stage_table_info.clone(),
            arrow_schema: info.arrow_schema.clone(),
            schema_from: info.schema_from.clone(),
            table_info,
        }))
    }

    #[async_backtrace::framed]
    pub async fn try_create(mut stage_table_info: StageTableInfo) -> Result<Arc<dyn Table>> {
        let stage_info = &stage_table_info.stage_info;
        let files_to_read = &stage_table_info.files_to_copy;
        let operator = init_stage_operator(stage_info)?;
        let first_file = match &files_to_read {
            Some(files) => files[0].clone(),
            None => stage_table_info
                .files_info
                .first_file(&operator)
                .await?
                .clone(),
        };
        let schema_from = first_file.path.clone();

        let arrow_schema = Self::prepare_metas(first_file, operator.clone()).await?;

        let table_schema = Arc::new(
            TableSchema::try_from(arrow_schema.as_ref()).map_err(ErrorCode::from_std_error)?,
        );

        let table_info = create_orc_table_info(table_schema.clone(), stage_info)?;

        stage_table_info.schema = table_schema;

        Ok(Arc::new(OrcTable {
            table_info,
            stage_table_info,
            arrow_schema,
            schema_from,
        }))
    }

    #[async_backtrace::framed]
    async fn prepare_metas(
        file_info: StageFileInfo,
        operator: Operator,
    ) -> Result<Arc<ArrowSchema>> {
        let file = OrcChunkReader {
            operator,
            size: file_info.size,
            path: file_info.path,
        };
        let builder = ArrowReaderBuilder::try_new_async(file)
            .await
            .map_err(|e| ErrorCode::StorageOther(e.to_string()))?;
        let arrow_schema = builder.build_async().schema();
        Ok(arrow_schema)
    }
}

#[async_trait::async_trait]
impl Table for OrcTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_local(&self) -> bool {
        false
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn support_column_projection(&self) -> bool {
        false
    }

    fn support_prewhere(&self) -> bool {
        false
    }

    fn has_exact_total_row_count(&self) -> bool {
        false
    }

    fn get_data_source_info(&self) -> DataSourceInfo {
        DataSourceInfo::ORCSource(OrcTableInfo {
            stage_table_info: self.stage_table_info.clone(),
            arrow_schema: self.arrow_schema.clone(),
            schema_from: self.schema_from.clone(),
        })
    }

    /// The returned partitions only record the locations of files to read.
    /// So they don't have any real statistics.
    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        read_partitions_simple(ctx, &self.stage_table_info).await
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

    fn is_stage_table(&self) -> bool {
        true
    }

    async fn table_statistics(
        &self,
        _ctx: Arc<dyn TableContext>,
        _change_type: Option<ChangeType>,
    ) -> Result<Option<TableStatistics>> {
        Ok(None)
    }
}

fn create_orc_table_info(schema: Arc<TableSchema>, stage_info: &StageInfo) -> Result<TableInfo> {
    Ok(TableInfo {
        ident: TableIdent::new(0, 0),
        desc: "''.'orc_table'".to_string(),
        name: format!("orc_table({})", stage_info.stage_name),
        meta: TableMeta {
            schema,
            engine: "ORCTable".to_string(),
            created_on: DateTime::from_timestamp(0, 0).unwrap(),
            updated_on: DateTime::from_timestamp(0, 0).unwrap(),
            ..Default::default()
        },
        ..Default::default()
    })
}
