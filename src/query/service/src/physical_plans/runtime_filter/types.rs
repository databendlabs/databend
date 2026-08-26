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
use databend_common_expression::Expr;
use databend_common_expression::RemoteExpr;
use databend_common_sql::ColumnEntry;
use databend_common_sql::MetadataRef;
use databend_common_sql::Symbol;
use databend_common_sql::TypeCheck;
use databend_common_sql::plans::ScalarExpr;

pub(crate) struct ResolvedRuntimeFilterProbeExpr {
    pub probe_key: Expr<String>,
    pub scan_id: usize,
    pub column_idx: Symbol,
}

pub(crate) fn resolve_runtime_filter_probe_expr(
    metadata: &MetadataRef,
    scalar: &ScalarExpr,
) -> Result<Option<ResolvedRuntimeFilterProbeExpr>> {
    let used_columns = scalar.used_columns();
    if used_columns.len() != 1 {
        return Ok(None);
    }

    let column_idx = *used_columns.iter().next().unwrap();
    if !matches!(
        metadata.read().column(column_idx),
        ColumnEntry::BaseTableColumn(_)
    ) {
        return Ok(None);
    }
    let Some(scan_id) = metadata.read().base_column_scan_id(column_idx) else {
        return Ok(None);
    };

    let metadata = metadata.read();
    let probe_key = scalar
        .as_raw_expr()
        .type_check(&*metadata)?
        .project_column_ref(|column| {
            let entry = metadata.column(column.index);
            if let ColumnEntry::BaseTableColumn(base_column) = entry {
                if base_column.path_indices.is_none() {
                    let table = metadata.table(base_column.table_index);
                    let schema = table.table().schema_with_stream();
                    if let Ok(field) = schema.field_of_column_id(base_column.column_id) {
                        return Ok(field.name().clone());
                    }
                }
                return Ok(base_column.column_name.clone());
            }
            Ok(column.column_name.clone())
        })?;

    Ok(Some(ResolvedRuntimeFilterProbeExpr {
        probe_key,
        scan_id,
        column_idx,
    }))
}

/// Probe-side information retained while a join condition is converted into a
/// physical runtime filter.
pub(crate) struct RuntimeFilterProbeKey<T> {
    pub probe_key: T,
    pub scan_id: usize,
    pub column_idx: Symbol,
    pub is_connector: bool,
    pub is_null_equal: bool,
}

impl<T> RuntimeFilterProbeKey<T> {
    pub fn map_probe_key<U>(self, map: impl FnOnce(T) -> U) -> RuntimeFilterProbeKey<U> {
        RuntimeFilterProbeKey {
            probe_key: map(self.probe_key),
            scan_id: self.scan_id,
            column_idx: self.column_idx,
            is_connector: self.is_connector,
            is_null_equal: self.is_null_equal,
        }
    }

    pub fn try_map_probe_key<U, E>(
        self,
        map: impl FnOnce(T) -> Result<U, E>,
    ) -> Result<RuntimeFilterProbeKey<U>, E> {
        Ok(RuntimeFilterProbeKey {
            probe_key: map(self.probe_key)?,
            scan_id: self.scan_id,
            column_idx: self.column_idx,
            is_connector: self.is_connector,
            is_null_equal: self.is_null_equal,
        })
    }
}

/// Only a direct column, optionally wrapped in a nullability-only cast, can
/// connect equivalence classes. Every value-changing expression is a leaf.
pub(crate) fn canonical_equivalence_connector(
    probe_key: RemoteExpr<String>,
) -> std::result::Result<RemoteExpr<String>, RemoteExpr<String>> {
    match probe_key {
        probe_key @ RemoteExpr::ColumnRef { .. } => Ok(probe_key),
        RemoteExpr::Cast {
            expr,
            dest_type,
            is_try: false,
            span,
        } => match expr.as_ref() {
            RemoteExpr::ColumnRef { data_type, .. }
                if dest_type.remove_nullable() == *data_type =>
            {
                Ok(*expr)
            }
            _ => Err(RemoteExpr::Cast {
                span,
                is_try: false,
                expr,
                dest_type,
            }),
        },
        probe_key => Err(probe_key),
    }
}

/// Collection of runtime filters for a join operation
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
pub struct PhysicalRuntimeFilters {
    pub filters: Vec<PhysicalRuntimeFilter>,
}

/// A runtime filter that is built once and applied to multiple probe targets
///
/// # Design
/// A single runtime filter is constructed once from the build side and then
/// pushed down to multiple table scans on the probe side. This is particularly
/// useful when join columns form equivalence classes (e.g., t1.c1 = t2.c1 = t3.c1),
/// allowing one filter to be applied to multiple tables.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PhysicalRuntimeFilter {
    /// Unique identifier for this runtime filter
    pub id: usize,

    /// The build key expression used to construct the filter
    pub build_key: RemoteExpr,

    /// List of (probe_key, scan_id) pairs that this filter should be applied to
    /// A single filter is built once and then pushed down to multiple scans
    /// Targets are equality-safe connector members or complete expression
    /// leaves attached to one of those members.
    pub probe_targets: Vec<(RemoteExpr<String>, usize)>,

    pub build_table_rows: Option<u64>,

    /// Enable bloom filter for this runtime filter
    pub enable_bloom_runtime_filter: bool,

    /// Enable inlist filter for this runtime filter
    pub enable_inlist_runtime_filter: bool,

    /// Enable min-max filter for this runtime filter
    pub enable_min_max_runtime_filter: bool,
}
