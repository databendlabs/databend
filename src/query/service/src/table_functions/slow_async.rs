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
use std::time::Duration;

use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Utc;
use common_base::base::tokio;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_context::TableContext;
use common_catalog::table_function::TableFunction;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::types::UInt64Type;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;

pub struct SlowAsyncTable {
    table_info: TableInfo,
    delay_seconds: usize,
    read_blocks_num: usize,
}

impl SlowAsyncTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        // let mut panic_message = None;
        let args = table_args.expect_all_positioned(table_func_name, None)?;

        if args.len() != 1 && args.len() != 2 {
            return Err(ErrorCode::BadArguments(
                "unexpected arguments for slow_async(delay_seconds, [block_num])",
            ));
        }

        let mut read_blocks_num = 1;
        println!("{:?}", args[0].to_string());

        let delay_seconds = match args[0].to_string().parse::<usize>() {
            Ok(x) => Ok(x),
            Err(_) => Err(ErrorCode::BadArguments("Expected uint64 argument")),
        }?;

        if args.len() == 2 {
            read_blocks_num = match args[1].to_string().parse::<usize>() {
                Ok(x) => Ok(x),
                Err(_) => Err(ErrorCode::BadArguments("Expected uint64 argument")),
            }?;
        }

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("slow_async"),
            meta: TableMeta {
                schema: TableSchemaRefExt::create(vec![TableField::new(
                    "number",
                    TableDataType::Number(NumberDataType::UInt64),
                )]),
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

        Ok(Arc::new(SlowAsyncTable {
            table_info,
            delay_seconds,
            read_blocks_num,
        }))
    }
}

#[async_trait::async_trait]
impl Table for SlowAsyncTable {
    fn is_local(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        // dummy statistics
        Ok((
            PartStatistics::new_exact(
                self.read_blocks_num,
                self.read_blocks_num * 8,
                self.read_blocks_num,
                self.read_blocks_num,
            ),
            Partitions::default(),
        ))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(TableArgs::new_positioned(vec![
            Scalar::Number(NumberScalar::UInt64(self.delay_seconds as u64)),
            Scalar::Number(NumberScalar::UInt64(self.read_blocks_num as u64)),
        ]))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                SlowAsyncDataSource::create(
                    ctx.clone(),
                    output,
                    self.delay_seconds,
                    self.read_blocks_num,
                )
            },
            1,
        )?;

        Ok(())
    }
}

struct SlowAsyncDataSource {
    delay_seconds: usize,
    read_blocks_num: usize,
}

impl SlowAsyncDataSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        delay_seconds: usize,
        read_blocks_num: usize,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, output, SlowAsyncDataSource {
            delay_seconds,
            read_blocks_num,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for SlowAsyncDataSource {
    const NAME: &'static str = "slow_async";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.read_blocks_num {
            0 => Ok(None),
            _ => {
                self.read_blocks_num -= 1;
                tokio::time::sleep(Duration::from_secs(self.delay_seconds as u64)).await;
                Ok(Some(DataBlock::new_from_columns(vec![
                    UInt64Type::from_data(vec![1]),
                ])))
            }
        }
    }
}

impl TableFunction for SlowAsyncTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
