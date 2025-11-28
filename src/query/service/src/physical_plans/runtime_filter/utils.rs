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

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::RemoteExpr;
use databend_common_sql::plans::JoinType;
use databend_common_sql::ColumnEntry;
use databend_common_sql::IndexType;
use databend_common_sql::MetadataRef;
use databend_common_sql::ScalarExpr;
use databend_common_sql::TypeCheck;

/// Check if a data type is supported for bloom filter
///
/// Currently supports: numbers and strings
pub fn is_type_supported_for_bloom_filter(data_type: &DataType) -> bool {
    data_type.is_number() || data_type.is_string()
}

/// Check if a data type is supported for min-max filter
///
/// Currently supports: numbers, dates, and strings
pub fn is_type_supported_for_min_max_filter(data_type: &DataType) -> bool {
    data_type.is_number() || data_type.is_date() || data_type.is_string()
}

/// Check if the join type is supported for runtime filter
///
/// Runtime filters are only applicable to certain join types where
/// filtering the probe side can reduce processing
pub fn supported_join_type_for_runtime_filter(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::LeftMark
    )
}

pub fn scalar_to_remote_expr(
    metadata: &MetadataRef,
    scalar: &ScalarExpr,
) -> Result<Option<(RemoteExpr<String>, usize, IndexType)>> {
    if scalar.used_columns().iter().all(|idx| {
        matches!(
            metadata.read().column(*idx),
            ColumnEntry::BaseTableColumn(_)
        )
    }) {
        if let Some(column_idx) = scalar.used_columns().iter().next() {
            let scan_id = metadata.read().base_column_scan_id(*column_idx);

            if let Some(scan_id) = scan_id {
                let remote_expr = scalar
                    .as_raw_expr()
                    .type_check(&*metadata.read())?
                    .project_column_ref(|col| Ok(col.column_name.clone()))?
                    .as_remote_expr();

                if !is_valid_probe_key(&remote_expr) {
                    return Ok(None);
                }

                return Ok(Some((remote_expr, scan_id, *column_idx)));
            }
        }
    }

    Ok(None)
}

pub fn is_valid_probe_key(probe_key: &RemoteExpr<String>) -> bool {
    match probe_key {
        RemoteExpr::ColumnRef { .. } => true,
        RemoteExpr::Cast {
            expr: box RemoteExpr::ColumnRef { data_type, .. },
            dest_type,
            ..
        } if &dest_type.remove_nullable() == data_type => true,
        _ => false,
    }
}
