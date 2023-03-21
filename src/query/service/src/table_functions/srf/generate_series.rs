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
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_args::TableArgs;
use common_catalog::table_function::TableFunction;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_schema_type;
use common_expression::types::NumberDataType;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::Scalar::Number;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::OneBlockSource;
use common_sql::validate_function_arg;
use common_storages_factory::Table;
use common_storages_fuse::TableContext;

pub struct GenerateSeriesTable {
    table_info: TableInfo,
    output: Column,
}

impl GenerateSeriesTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        // Check args.
        validate_function_arg(
            table_func_name,
            table_args.positioned.len(),
            Some((2, 3)),
            2,
        )?;

        let start = table_args.positioned[0]
            .clone()
            .into_number()
            .map_err(|_| ErrorCode::BadArguments("Expected number argument."))?;
        let end = table_args.positioned[1]
            .clone()
            .into_number()
            .map_err(|_| ErrorCode::BadArguments("Expected number argument."))?;
        let mut step = 1;
        if table_args.positioned.len() == 3 {
            step = table_args.positioned[2]
                .clone()
                .into_number()
                .map_err(|_| ErrorCode::BadArguments("Expected number argument."))?.as_int32().unwrap().clone();
        }
        println!("start: {}, end: {}, step: {}", start, end, step);

        let column = Column::Null { len: 0 };

        let schema = TableSchema::new(vec![TableField::new(
            "generate_series",
            TableDataType::Number(NumberDataType::UInt64),
        )]);

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from(table_func_name),
            meta: TableMeta {
                schema: Arc::new(schema),
                engine: String::from(table_func_name),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: Utc
                    .from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
                updated_on: Utc
                    .from_utc_datetime(&NaiveDateTime::from_timestamp_opt(0, 0).unwrap()),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(GenerateSeriesTable {
            table_info,
            output: column,
        }))
    }
}

impl TableFunction for GenerateSeriesTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[async_trait::async_trait]
impl Table for GenerateSeriesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        _: Arc<dyn TableContext>,
        _: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        // dummy statistics
        Ok((
            PartStatistics::new_exact(self.output.len(), self.output.memory_size(), 1, 1),
            Partitions::default(),
        ))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(TableArgs::new_positioned(vec![Scalar::Array(
            self.output.clone(),
        )]))
    }

    fn read_data(
        &self,
        _ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                OneBlockSource::create(
                    output,
                    DataBlock::new_from_columns(vec![self.output.clone()]),
                )
            },
            1,
        )?;

        Ok(())
    }
}
