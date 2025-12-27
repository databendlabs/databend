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

use std::sync::Arc;

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VariantType;

use crate::FuseTable;
use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::io::TableMetaLocationGenerator;
use crate::sessions::TableContext;
use crate::table_functions::SimpleTableFunc;
use crate::table_functions::parse_db_tb_args;
use crate::table_functions::string_literal;

pub struct FuseDumpSnapshotsArgs {
    database_name: String,
    table_name: String,
}

const DEFAULT_SNAPSHOT_LIMIT: usize = 1;

pub struct FuseDumpSnapshotsFunc {
    args: FuseDumpSnapshotsArgs,
}

impl From<&FuseDumpSnapshotsArgs> for TableArgs {
    fn from(args: &FuseDumpSnapshotsArgs) -> Self {
        TableArgs::new_positioned(vec![
            string_literal(args.database_name.as_str()),
            string_literal(args.table_name.as_str()),
        ])
    }
}

#[async_trait::async_trait]
impl SimpleTableFunc for FuseDumpSnapshotsFunc {
    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("snapshot_id", TableDataType::String),
            TableField::new("snapshot", TableDataType::Variant),
        ])
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_table(
                &tenant_id,
                self.args.database_name.as_str(),
                self.args.table_name.as_str(),
            )
            .await?;

        let table = FuseTable::try_from_table(tbl.as_ref()).map_err(|_| {
            ErrorCode::StorageOther(
                "Invalid table engine, only FUSE table supports fuse_dump_snapshots",
            )
        })?;

        let meta_location_generator = table.meta_location_generator.clone();
        let snapshot_location = table.snapshot_loc();
        if let Some(snapshot_location) = snapshot_location {
            let limit = plan
                .push_downs
                .as_ref()
                .and_then(|v| v.limit)
                .unwrap_or(DEFAULT_SNAPSHOT_LIMIT);

            let table_snapshot_reader = MetaReaders::table_snapshot_reader(table.operator.clone());
            let format_version =
                TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());

            let lite_snapshot_stream = table_snapshot_reader.snapshot_history(
                snapshot_location,
                format_version,
                meta_location_generator.clone(),
                table.get_branch_id(),
            );

            let mut snapshot_ids: Vec<String> = Vec::with_capacity(limit);
            let mut content: Vec<_> = Vec::with_capacity(limit);

            use futures::stream::StreamExt;
            let mut stream = lite_snapshot_stream.take(limit);

            use jsonb::Value as JsonbValue;
            while let Some(s) = stream.next().await {
                let (s, _v) = s?;
                snapshot_ids.push(s.snapshot_id.simple().to_string());
                content.push(JsonbValue::from(serde_json::to_value(s)?).to_vec());
            }

            let block = DataBlock::new_from_columns(vec![
                StringType::from_data(snapshot_ids),
                VariantType::from_data(content),
            ]);

            return Ok(Some(block));
        }
        Ok(Some(DataBlock::empty_with_schema(Arc::new(
            self.schema().into(),
        ))))
    }

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let (arg_database_name, arg_table_name) = parse_db_tb_args(&table_args, func_name)?;
        Ok(Self {
            args: FuseDumpSnapshotsArgs {
                database_name: arg_database_name,
                table_name: arg_table_name,
            },
        })
    }
}
