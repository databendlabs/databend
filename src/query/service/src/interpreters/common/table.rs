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

use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataSchemaRef;
use databend_common_sql::parse_computed_expr;
use databend_common_storages_fuse::FuseTable;

use crate::sessions::TableContext;

pub fn check_referenced_computed_columns(
    ctx: Arc<dyn TableContext>,
    schema: DataSchemaRef,
    column: &str,
) -> Result<()> {
    for f in schema.fields() {
        if let Some(computed_expr) = f.computed_expr() {
            let expr = match computed_expr {
                ComputedExpr::Stored(expr) => expr,
                ComputedExpr::Virtual(expr) => expr,
            };
            match parse_computed_expr(ctx.clone(), schema.clone(), expr) {
                Ok(expr) => {
                    if expr.data_type() != f.data_type() {
                        return Err(ErrorCode::ColumnReferencedByComputedColumn(format!(
                            "expected computed column expression have type {}, but got type {}, may caused by modify column `{}`.",
                            f.data_type(),
                            expr.data_type(),
                            column,
                        )));
                    }
                }
                Err(_) => {
                    return Err(ErrorCode::ColumnReferencedByComputedColumn(format!(
                        "column `{}` is referenced by computed column `{}`",
                        column,
                        &f.name()
                    )));
                }
            }
        }
    }
    Ok(())
}

pub fn stored_computed_column_references(
    ctx: Arc<dyn TableContext>,
    schema: DataSchemaRef,
    column: &str,
) -> Result<bool> {
    for field in schema.fields() {
        let Some(ComputedExpr::Stored(sql)) = field.computed_expr() else {
            continue;
        };
        let expr = parse_computed_expr(ctx.clone(), schema.clone(), sql)?;
        if expr
            .column_refs()
            .keys()
            .any(|binding| binding.column_name == column)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Reject the table kinds that cannot honor a row-level TTL.
///
/// TTL is applied asynchronously by a background task that reads committed
/// table meta, so it only makes sense for a durable, mutable FUSE table:
///
/// - read-only tables (ATTACH, materialized views) cannot be mutated at all;
/// - TRANSIENT tables do not retain historical data the task would rely on;
/// - TEMPORARY tables live only for the current session, so a background task
///   in another session would never see them.
pub fn check_ttl_supported_table(table: &dyn Table) -> Result<()> {
    // Rejects any non-FUSE engine: TTL is applied by the FUSE mutation path.
    let fuse_table = FuseTable::try_from_table(table)?;

    // Covers ATTACH and materialized views with an explicit READ ONLY message.
    table.check_mutable()?;

    let desc = &table.get_table_info().desc;
    if fuse_table.is_transient() {
        return Err(ErrorCode::BadArguments(format!(
            "The table {desc} is transient, TTL is not supported"
        )));
    }
    if table.is_temp() {
        return Err(ErrorCode::BadArguments(format!(
            "The table {desc} is temporary, TTL is not supported"
        )));
    }
    Ok(())
}
