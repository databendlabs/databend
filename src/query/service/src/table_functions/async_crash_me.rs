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
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use chrono::DateTime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use futures::Stream;

use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::ProcessorPtr;
use crate::sessions::TableContext;
use crate::storages::Table;

pub struct AsyncCrashMeTable {
    table_info: TableInfo,
    panic_message: Option<String>,
}

impl AsyncCrashMeTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let mut panic_message = None;
        let args = table_args.expect_all_positioned(table_func_name, None)?;
        if args.len() == 1 {
            let arg = args[0].clone();
            panic_message = Some(
                arg.into_string()
                    .map_err(|_| ErrorCode::BadArguments("Expected string argument"))?,
            );
        }

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("async_crash_me"),
            meta: TableMeta {
                schema: TableSchemaRefExt::create_dummy(),
                engine: String::from(table_func_name),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(AsyncCrashMeTable {
            table_info,
            panic_message,
        }))
    }
}

#[async_trait::async_trait]
impl Table for AsyncCrashMeTable {
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
        Ok((PartStatistics::new_exact(1, 1, 1, 1), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        let args = match &self.panic_message {
            Some(s) => vec![Scalar::String(s.clone())],
            None => vec![],
        };
        Some(TableArgs::new_positioned(args))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| AsyncCrashMeSource::create(ctx.clone(), output, self.panic_message.clone()),
            1,
        )?;

        Ok(())
    }
}

struct AsyncCrashMeSource {
    message: Option<String>,
}

impl AsyncCrashMeSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        message: Option<String>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, output, AsyncCrashMeSource { message })
    }
}

#[async_trait::async_trait]
impl AsyncSource for AsyncCrashMeSource {
    const NAME: &'static str = "async_crash_me";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        match &self.message {
            None => panic!("async crash me panic"),
            Some(message) => panic!("{}", message),
        }
    }
}

impl TableFunction for AsyncCrashMeTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct AsyncCrashMeStream {
    message: Option<String>,
}

impl Stream for AsyncCrashMeStream {
    type Item = Result<DataBlock>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &self.message {
            None => panic!("async crash me panic"),
            Some(message) => panic!("{}", message),
        }
    }
}
