// Copyright 2021 Datafuse Labs.
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
use std::collections::BTreeMap;
use std::sync::Arc;

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

use crate::sessions::QueryContext;

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

    /// whether column prune(projection) can help in table read
    fn benefit_column_prune(&self) -> bool {
        false
    }

    // defaults to generate one single part and empty statistics
    async fn read_partitions(
        &self,
        _ctx: Arc<QueryContext>,
        _push_downs: Option<Extras>,
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
        _ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream>;

    // temporary added, pls feel free to rm it
    async fn append_data(
        &self,
        _ctx: Arc<QueryContext>,
        _insert_plan: InsertIntoPlan,
        _stream: SendableDataBlockStream,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "append data for local table {} is not implemented",
            self.name()
        )))
    }

    async fn truncate(
        &self,
        _ctx: Arc<QueryContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "truncate for local table {} is not implemented",
            self.name()
        )))
    }
}

pub type TablePtr = Arc<dyn Table>;

#[async_trait::async_trait]
pub trait ToReadDataSourcePlan {
    /// Real read_plan to access partitions/push_downs
    async fn read_plan(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: Option<Extras>,
    ) -> Result<ReadDataSourcePlan>;
}

#[async_trait::async_trait]
impl ToReadDataSourcePlan for dyn Table {
    async fn read_plan(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: Option<Extras>,
    ) -> Result<ReadDataSourcePlan> {
        let (statistics, parts) = self.read_partitions(ctx, push_downs.clone()).await?;
        let table_info = self.get_table_info();
        let description = get_description(table_info, &statistics);

        let scan_fields = match (self.benefit_column_prune(), &push_downs) {
            (true, Some(push_downs)) => match &push_downs.projection {
                Some(projection) if projection.len() < table_info.schema().fields().len() => {
                    let fields = projection
                        .iter()
                        .map(|i| table_info.schema().field(*i).clone());

                    Some((projection.iter().cloned().zip(fields)).collect::<BTreeMap<_, _>>())
                }
                _ => None,
            },
            _ => None,
        };

        Ok(ReadDataSourcePlan {
            table_info: table_info.clone(),
            scan_fields,
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs,
        })
    }
}

fn get_description(table_info: &TableInfo, statistics: &Statistics) -> String {
    if statistics.read_rows > 0 {
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
    }
}
