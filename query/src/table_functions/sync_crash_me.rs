//  Copyright 2022 Datafuse Labs.
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
//

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use chrono::NaiveDateTime;
use common_datablocks::DataBlock;
use common_datavalues::chrono::TimeZone;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::SendableDataBlockStream;
use futures::Stream;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::{AsyncSource, SyncSource, SyncSourcer};
use crate::pipelines::new::processors::AsyncSourcer;
use crate::pipelines::new::NewPipe;
use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;
use crate::storages::Table;
use crate::table_functions::table_function_factory::TableArgs;
use crate::table_functions::TableFunction;

pub struct SyncCrashMeTable {
    table_info: TableInfo,
    panic_message: Option<String>,
}

impl SyncCrashMeTable {
    pub fn create(
        database_name: &str,
        _table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let mut panic_message = None;
        if let Some(args) = &table_args {
            if args.len() == 1 {
                let arg = &args[0];
                if let Expression::Literal { value, .. } = arg {
                    panic_message = Some(String::from_utf8(value.as_string()?)?);
                }
            }
        }

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, "async_crash_me"),
            name: String::from("async_crash_me"),
            meta: TableMeta {
                schema: Arc::new(DataSchema::empty()),
                engine: String::from("async_crash_me"),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp(0, 0)),
                ..Default::default()
            },
        };

        Ok(Arc::new(SyncCrashMeTable {
            table_info,
            panic_message,
        }))
    }
}

#[async_trait::async_trait]
impl Table for SyncCrashMeTable {
    fn is_local(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        _: Arc<QueryContext>,
        _: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        // dummy statistics
        Ok((Statistics::new_exact(1, 1, 1, 1), vec![]))
    }

    fn table_args(&self) -> Option<Vec<Expression>> {
        Some(vec![Expression::create_literal(DataValue::UInt64(0))])
    }

    async fn read(
        &self,
        _ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(SyncCrashMeStream {
            message: self.panic_message.clone(),
        }))
    }

    fn read2(
        &self,
        ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let output = OutputPort::create();
        pipeline.add_pipe(NewPipe::SimplePipe {
            inputs_port: vec![],
            outputs_port: vec![output.clone()],
            processors: vec![SyncCrashMeSource::create(
                ctx,
                output,
                self.panic_message.clone(),
            )?],
        });

        Ok(())
    }
}

struct SyncCrashMeSource {
    message: Option<String>,
}

impl SyncCrashMeSource {
    pub fn create(
        ctx: Arc<QueryContext>,
        output: Arc<OutputPort>,
        message: Option<String>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, SyncCrashMeSource { message })
    }
}

impl SyncSource for SyncCrashMeSource {
    const NAME: &'static str = "sync_crash_me";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match &self.message {
            None => panic!("sync crash me panic"),
            Some(message) => panic!("{}", message),
        }
    }
}

impl TableFunction for SyncCrashMeTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
        where Self: 'a {
        self
    }
}

struct SyncCrashMeStream {
    message: Option<String>,
}

impl Stream for SyncCrashMeStream {
    type Item = Result<DataBlock>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &self.message {
            None => panic!("sync crash me panic"),
            Some(message) => panic!("{}", message),
        }
    }
}
