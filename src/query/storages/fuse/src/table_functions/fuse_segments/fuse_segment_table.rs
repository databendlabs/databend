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

use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_exception::Result;
use common_expression::DataBlock;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use super::fuse_segment::FuseSegment;
use super::table_args::parse_func_history_args;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::AsyncSource;
use crate::pipelines::processors::AsyncSourcer;
use crate::pipelines::Pipeline;
use crate::sessions::TableContext;
use crate::table_functions::string_literal;
use crate::table_functions::TableArgs;
use crate::table_functions::TableFunction;
use crate::FuseTable;
use crate::Table;

const FUSE_FUNC_SEGMENT: &str = "fuse_segment";

pub struct FuseSegmentTable {
    table_info: TableInfo,
    arg_database_name: String,
    arg_table_name: String,
    arg_snapshot_id: String,
}

impl FuseSegmentTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let (arg_database_name, arg_table_name, arg_snapshot_id) =
            parse_func_history_args(&table_args)?;

        let engine = FUSE_FUNC_SEGMENT.to_owned();

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: FuseSegment::schema(),
                engine,
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(FuseSegmentTable {
            table_info,
            arg_database_name,
            arg_table_name,
            arg_snapshot_id,
        }))
    }
}

#[async_trait::async_trait]
impl Table for FuseSegmentTable {
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
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(TableArgs::new_positioned(vec![
            string_literal(self.arg_database_name.as_str()),
            string_literal(self.arg_table_name.as_str()),
            string_literal(self.arg_snapshot_id.as_str()),
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
                FuseSegmentSource::create(
                    ctx.clone(),
                    output,
                    self.arg_database_name.to_owned(),
                    self.arg_table_name.to_owned(),
                    self.arg_snapshot_id.to_owned(),
                )
            },
            1,
        )?;

        Ok(())
    }
}

struct FuseSegmentSource {
    finish: bool,
    ctx: Arc<dyn TableContext>,
    arg_database_name: String,
    arg_table_name: String,
    arg_snapshot_id: String,
}

impl FuseSegmentSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        arg_database_name: String,
        arg_table_name: String,
        arg_snapshot_id: String,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, FuseSegmentSource {
            ctx,
            finish: false,
            arg_table_name,
            arg_database_name,
            arg_snapshot_id,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for FuseSegmentSource {
    const NAME: &'static str = "fuse_segment";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finish {
            return Ok(None);
        }

        self.finish = true;
        let tenant_id = self.ctx.get_tenant();
        let tbl = self
            .ctx
            .get_catalog(CATALOG_DEFAULT)?
            .get_table(
                tenant_id.as_str(),
                self.arg_database_name.as_str(),
                self.arg_table_name.as_str(),
            )
            .await?;

        let tbl = FuseTable::try_from_table(tbl.as_ref())?;
        Ok(Some(
            FuseSegment::new(self.ctx.clone(), tbl, self.arg_snapshot_id.clone())
                .get_segments()
                .await?,
        ))
    }
}

impl TableFunction for FuseSegmentTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
