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

use std::sync::Arc;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planner::PhysicalScalar;
use common_sql::PhysicalScalarParser;

use crate::table_functions::string_value;
use crate::table_functions::TableArgs;
use crate::FuseTable;

pub fn parse_func_table_args(table_args: &TableArgs) -> Result<(String, String)> {
    match table_args {
        // Todo(zhyass): support 3 arguments.
        Some(args) if args.len() == 2 => {
            let db = string_value(&args[0])?;
            let tbl = string_value(&args[1])?;
            Ok((db, tbl))
        }
        _ => Err(ErrorCode::BadArguments(format!(
            "expecting database and table name (as two string literals), but got {:?}",
            table_args
        ))),
    }
}

pub async fn get_cluster_keys(
    ctx: Arc<dyn TableContext>,
    table: &FuseTable,
    definition: &str,
) -> Result<Vec<PhysicalScalar>> {
    let cluster_keys = if !definition.is_empty() {
        let physical_scalars =
            PhysicalScalarParser::parse_exprs(ctx, table.schema(), definition).await?;
        physical_scalars
    } else {
        table.cluster_keys()
    };

    if cluster_keys.is_empty() {
        return Err(ErrorCode::InvalidClusterKeys(format!(
            "Invalid clustering keys or table {} is not clustered",
            table.name()
        )));
    }

    Ok(cluster_keys)
}
