//  Copyright 2021 Datafuse Labs.
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

use common_catalog::catalog::CATALOG_DEFAULT;
use common_exception::Result;
use common_legacy_planners::Expression;
use common_legacy_planners::Extras;
use common_legacy_planners::Partitions;
use common_legacy_planners::ReadDataSourcePlan;
use common_legacy_planners::Statistics;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_sources::processors::sources::StreamSourceNoSkipEmpty;

use super::fuse_snapshot::FuseSnapshot;
use super::table_args::parse_func_history_args;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::Pipe;
use crate::pipelines::Pipeline;
use crate::sessions::TableContext;
use crate::table_functions::string_literal;
use crate::table_functions::TableArgs;
use crate::table_functions::TableFunction;
use crate::Table;

const FUSE_FUNC_SNAPSHOT: &str = "fuse_snapshot";

pub struct FuseSnapshotTable {
    table_info: TableInfo,
    arg_database_name: String,
    arg_table_name: String,
}

impl FuseSnapshotTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let (arg_database_name, arg_table_name) = parse_func_history_args(&table_args)?;

        let engine = FUSE_FUNC_SNAPSHOT.to_owned();

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: FuseSnapshot::schema(),
                engine,
                ..Default::default()
            },
            share_name: None,
        };

        Ok(Arc::new(FuseSnapshotTable {
            table_info,
            arg_database_name,
            arg_table_name,
        }))
    }

    fn get_limit(plan: &ReadDataSourcePlan) -> Option<usize> {
        plan.push_downs.as_ref().and_then(|extras| extras.limit)
    }
}

#[async_trait::async_trait]
impl Table for FuseSnapshotTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        Ok((Statistics::default(), vec![]))
    }

    fn table_args(&self) -> Option<Vec<Expression>> {
        Some(vec![
            string_literal(self.arg_database_name.as_str()),
            string_literal(self.arg_table_name.as_str()),
        ])
    }

    fn read2(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &ReadDataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let output = OutputPort::create();
        let limit = Self::get_limit(plan);
        let database_name = self.arg_database_name.to_owned();
        let table_name = self.arg_table_name.to_owned();
        let catalog_name = CATALOG_DEFAULT.to_owned();
        let snapshot_stream = FuseSnapshot::new_snapshot_history_stream(
            ctx.clone(),
            database_name,
            table_name,
            catalog_name,
            limit,
        );

        // the underlying stream may returns a single empty block, which carries the schema
        let source = StreamSourceNoSkipEmpty::create(ctx, Some(snapshot_stream), output.clone())?;

        pipeline.add_pipe(Pipe::SimplePipe {
            inputs_port: vec![],
            outputs_port: vec![output],
            processors: vec![source],
        });

        Ok(())
    }
}

impl TableFunction for FuseSnapshotTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
