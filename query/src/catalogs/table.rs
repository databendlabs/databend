// Copyright 2020 Datafuse Labs.
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

use common_context::TableIOContext;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::MetaId;
use common_meta_types::TableInfo;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::InsertIntoPlan;
use common_planners::Part;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;

#[async_trait::async_trait]
pub trait Table: Sync + Send {
    fn name(&self) -> &str {
        &self.get_table_info().name
    }

    fn engine(&self) -> &str {
        self.get_table_info().engine()
    }

    fn schema(&self) -> DataSchemaRef {
        self.get_table_info().schema()
    }

    fn get_id(&self) -> MetaId {
        self.get_table_info().ident.table_id
    }

    fn is_local(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any;

    fn get_table_info(&self) -> &TableInfo;

    // defaults to generate one single part and empty statistics
    fn read_partitions(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<(Statistics, Partitions)> {
        Ok((Statistics::default(), vec![Part {
            name: "".to_string(),
            version: 0,
        }]))
    }

    fn table_args(&self) -> Option<Vec<Expression>> {
        None
    }

    // Read block data from the underling.
    async fn read(
        &self,
        io_ctx: Arc<TableIOContext>,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream>;

    // temporary added, pls feel free to rm it
    async fn append_data(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "append data for local table {} is not implemented",
            self.name()
        )))
    }

    async fn truncate(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "truncate for local table {} is not implemented",
            self.name()
        )))
    }
}

pub type TablePtr = Arc<dyn Table>;

pub trait ToReadDataSourcePlan {
    fn read_plan(
        &self,
        io_ctx: Arc<TableIOContext>,
        push_downs: Option<Extras>,
        partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan>;
}

impl ToReadDataSourcePlan for dyn Table {
    fn read_plan(
        &self,
        io_ctx: Arc<TableIOContext>,
        push_downs: Option<Extras>,
        partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan> {
        let (statistics, parts) =
            self.read_partitions(io_ctx, push_downs.clone(), partition_num_hint)?;
        let table_info = self.get_table_info();

        let description = if statistics.read_rows > 0 {
            format!(
                "(Read from {} table, {} Read Rows:{}, Read Bytes:{})",
                table_info.desc,
                if statistics.is_exact {
                    "Exactly"
                } else {
                    "Approximately"
                },
                statistics.read_rows,
                statistics.read_bytes,
            )
        } else {
            format!("(Read from {} table)", table_info.desc)
        };

        Ok(ReadDataSourcePlan {
            table_info: table_info.clone(),

            scan_fields: None,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs,
        })
    }
}
