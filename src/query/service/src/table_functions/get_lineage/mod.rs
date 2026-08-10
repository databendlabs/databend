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

mod edge_reader;
mod resolver;
mod traversal;

use std::any::Any;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_args::i64_value;
use databend_common_catalog::table_args::string_value;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::processor::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_meta_client::types::MetaId;

use crate::sessions::TableContext;

const GET_LINEAGE_FUNC: &str = "get_lineage";
const GET_LINEAGE_ENGINE: &str = "GET_LINEAGE";
const DEFAULT_DISTANCE: u8 = 5;
const MAX_DISTANCE: u8 = 5;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ObjectDomain {
    Table,
    View,
    Stage,
    Column,
}

impl ObjectDomain {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            "TABLE" => Ok(Self::Table),
            "VIEW" => Ok(Self::View),
            "STAGE" => Ok(Self::Stage),
            "COLUMN" => Ok(Self::Column),
            other => Err(ErrorCode::BadArguments(format!(
                "unsupported object_domain '{other}', expected TABLE, VIEW, STAGE, or COLUMN"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum QueryDirection {
    /// Walk from a target object toward the source objects that feed it.
    Upstream,
    /// Walk from a source object toward the target objects that consume it.
    Downstream,
}

impl QueryDirection {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            "UPSTREAM" => Ok(Self::Upstream),
            "DOWNSTREAM" => Ok(Self::Downstream),
            other => Err(ErrorCode::BadArguments(format!(
                "unsupported direction '{other}', expected UPSTREAM or DOWNSTREAM"
            ))),
        }
    }

    fn match_column(self) -> &'static str {
        match self {
            Self::Upstream => "target_lineage_key",
            Self::Downstream => "source_lineage_key",
        }
    }
}

#[derive(Clone, Debug)]
struct GetLineageArgs {
    object_name: String,
    object_domain: ObjectDomain,
    direction: QueryDirection,
    distance: u8,
}

impl GetLineageArgs {
    fn parse(table_args: &TableArgs) -> Result<Self> {
        let args = table_args.expect_all_positioned(GET_LINEAGE_FUNC, None)?;
        if !(args.len() == 3 || args.len() == 4) {
            return Err(ErrorCode::BadArguments(format!(
                "{GET_LINEAGE_FUNC} requires 3 or 4 positioned arguments: object_name, object_domain, direction[, distance]"
            )));
        }

        let distance = if args.len() == 4 {
            let distance = i64_value(&args[3])?;
            if !(1..=MAX_DISTANCE as i64).contains(&distance) {
                return Err(ErrorCode::BadArguments(format!(
                    "distance must be an integer in the range [1, {MAX_DISTANCE}]"
                )));
            }
            distance as u8
        } else {
            DEFAULT_DISTANCE
        };

        Ok(Self {
            object_name: string_value(&args[0])?,
            object_domain: ObjectDomain::parse(&string_value(&args[1])?)?,
            direction: QueryDirection::parse(&string_value(&args[2])?)?,
            distance,
        })
    }
}

#[derive(Clone, Debug)]
struct LineageResultRow {
    distance: i32,
    source_object_domain: Option<String>,
    source_object_name: Option<String>,
    source_column_name: Option<String>,
    target_object_domain: Option<String>,
    target_object_name: Option<String>,
    target_column_name: Option<String>,
    target_status: String,
    process: Option<String>,
}

pub struct GetLineageTable {
    table_info: TableInfo,
    table_args: TableArgs,
    args: GetLineageArgs,
}

impl GetLineageTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: MetaId,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = GetLineageArgs::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Self::schema(),
                engine: GET_LINEAGE_ENGINE.to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        Ok(Arc::new(Self {
            table_info,
            table_args,
            args,
        }))
    }

    fn schema() -> Arc<TableSchema> {
        let nullable_string = || TableDataType::Nullable(Box::new(TableDataType::String));
        TableSchemaRefExt::create(vec![
            TableField::new(
                "distance",
                TableDataType::Number(databend_common_expression::types::NumberDataType::Int32),
            ),
            TableField::new("source_object_domain", nullable_string()),
            TableField::new("source_object_name", nullable_string()),
            TableField::new("source_column_name", nullable_string()),
            TableField::new("target_object_domain", nullable_string()),
            TableField::new("target_object_name", nullable_string()),
            TableField::new("target_column_name", nullable_string()),
            TableField::new("target_status", TableDataType::String),
            TableField::new("process", nullable_string()),
        ])
    }
}

#[async_trait::async_trait]
impl Table for GetLineageTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(self.table_args.clone())
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        let args = self.args.clone();
        let schema = self.table_info.meta.schema.clone();
        pipeline.add_source(
            |output| GetLineageSource::create(ctx.clone(), output, args.clone(), schema.clone()),
            1,
        )?;
        Ok(())
    }

    // The result depends on a history-table snapshot acquired inside this opaque source.
    fn result_can_be_cached(&self) -> bool {
        false
    }
}

impl TableFunction for GetLineageTable {
    fn function_name(&self) -> &str {
        GET_LINEAGE_FUNC
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct GetLineageSource {
    ctx: Arc<dyn TableContext>,
    args: GetLineageArgs,
    schema: DataSchemaRef,
    finished: bool,
}

impl GetLineageSource {
    fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args: GetLineageArgs,
        schema: Arc<TableSchema>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, Self {
            ctx,
            args,
            schema: Arc::new(DataSchema::from(schema.as_ref())),
            finished: false,
        })
    }

    fn build_block(&self, rows: Vec<LineageResultRow>) -> DataBlock {
        if rows.is_empty() {
            return DataBlock::empty_with_schema(&self.schema);
        }

        let mut distances = Vec::with_capacity(rows.len());
        let mut source_domains = Vec::with_capacity(rows.len());
        let mut source_names = Vec::with_capacity(rows.len());
        let mut source_columns = Vec::with_capacity(rows.len());
        let mut target_domains = Vec::with_capacity(rows.len());
        let mut target_names = Vec::with_capacity(rows.len());
        let mut target_columns = Vec::with_capacity(rows.len());
        let mut target_statuses = Vec::with_capacity(rows.len());
        let mut processes = Vec::with_capacity(rows.len());

        for row in rows {
            distances.push(row.distance);
            source_domains.push(row.source_object_domain);
            source_names.push(row.source_object_name);
            source_columns.push(row.source_column_name);
            target_domains.push(row.target_object_domain);
            target_names.push(row.target_object_name);
            target_columns.push(row.target_column_name);
            target_statuses.push(row.target_status);
            processes.push(row.process);
        }

        DataBlock::new_from_columns(vec![
            Int32Type::from_data(distances),
            StringType::from_opt_data(source_domains),
            StringType::from_opt_data(source_names),
            StringType::from_opt_data(source_columns),
            StringType::from_opt_data(target_domains),
            StringType::from_opt_data(target_names),
            StringType::from_opt_data(target_columns),
            StringType::from_data(target_statuses),
            StringType::from_opt_data(processes),
        ])
    }
}

#[async_trait::async_trait]
impl AsyncSource for GetLineageSource {
    const NAME: &'static str = "GetLineageSource";

    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }
        let rows = traversal::traverse(self.ctx.clone(), self.args.clone()).await?;
        self.finished = true;
        Ok(Some(self.build_block(rows)))
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::NumberScalar;

    use super::*;

    #[test]
    fn test_get_lineage_args() -> Result<()> {
        let default_distance = TableArgs::new_positioned(vec![
            Scalar::String("db.table".to_string()),
            Scalar::String("TABLE".to_string()),
            Scalar::String("UPSTREAM".to_string()),
        ]);
        let parsed = GetLineageArgs::parse(&default_distance)?;
        assert_eq!(parsed.object_domain, ObjectDomain::Table);
        assert_eq!(parsed.direction, QueryDirection::Upstream);
        assert_eq!(parsed.distance, DEFAULT_DISTANCE);

        let explicit_distance = TableArgs::new_positioned(vec![
            Scalar::String("db.table.col".to_string()),
            Scalar::String("COLUMN".to_string()),
            Scalar::String("DOWNSTREAM".to_string()),
            Scalar::Number(NumberScalar::Int64(2)),
        ]);
        let parsed = GetLineageArgs::parse(&explicit_distance)?;
        assert_eq!(parsed.object_domain, ObjectDomain::Column);
        assert_eq!(parsed.direction, QueryDirection::Downstream);
        assert_eq!(parsed.distance, 2);
        Ok(())
    }

    #[test]
    fn test_get_lineage_rejects_invalid_distance() {
        let args = TableArgs::new_positioned(vec![
            Scalar::String("db.table".to_string()),
            Scalar::String("TABLE".to_string()),
            Scalar::String("UPSTREAM".to_string()),
            Scalar::Number(NumberScalar::Int64(6)),
        ]);
        assert!(GetLineageArgs::parse(&args).is_err());
    }
}
