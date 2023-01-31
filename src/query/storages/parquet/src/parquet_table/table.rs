//  Copyright 2023 Datafuse Labs.
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
use std::sync::Arc;

use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Utc;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::Scalar;
use common_expression::TableSchema;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::Pipeline;
use opendal::Operator;

use crate::ReadOptions;

pub struct ParquetTable {
    pub(super) table_args: TableArgs,

    pub(super) file_locations: Vec<String>,
    pub(super) table_info: TableInfo,
    pub(super) arrow_schema: ArrowSchema,
    pub(super) operator: Operator,
    pub(super) read_options: ReadOptions,
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
        self.read_options.do_prewhere()
    }

    fn has_exact_total_row_count(&self) -> bool {
        true
    }

    fn table_args(&self) -> Option<Vec<Scalar>> {
        self.table_args.clone()
    }

    /// The returned partitions only record the locations of files to read.
    /// So they don't have any real statistics.
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(ctx, push_down).await
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

fn lower_field_name(field: &mut ArrowField) {
    field.name = field.name.to_lowercase();
    match &mut field.data_type {
        ArrowDataType::List(f)
        | ArrowDataType::LargeList(f)
        | ArrowDataType::FixedSizeList(f, _) => {
            lower_field_name(f.as_mut());
        }
        ArrowDataType::Struct(ref mut fields) => {
            for f in fields {
                lower_field_name(f);
            }
        }
        _ => {}
    }
}

pub(crate) fn arrow_to_table_schema(mut schema: ArrowSchema) -> TableSchema {
    schema.fields.iter_mut().for_each(|f| {
        lower_field_name(f);
    });
    TableSchema::from(&schema)
}

pub(super) fn create_parquet_table_info(table_id: u64, schema: ArrowSchema) -> TableInfo {
    TableInfo {
        ident: TableIdent::new(table_id, 0),
        desc: "''.'read_parquet'".to_string(),
        name: "read_parquet".to_string(),
        meta: TableMeta {
            schema: arrow_to_table_schema(schema).into(),
            engine: "SystemReadParquet".to_string(),
            created_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
            updated_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
            ..Default::default()
        },
        ..Default::default()
    }
}
