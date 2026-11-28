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

//! The boundaries the runtime TopN filters of fuse scans reached, by scan
//! (`enable_condition_cache`).
//!
//! A boundary is the first sort key of a row that ranks among the top rows of its stream, so at
//! least as many rows of the scan rank before it or with it as the TopN keeps: any row that ranks
//! after it is not among the top rows. This holds for every scan that reads the same rows, that
//! is, with the same filters over the same snapshot, and orders them by the same first key for
//! as many top rows. Such a scan starts with the boundary, and prunes by it from its start.

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Filters;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_cache::TopNBoundaryValue;
use databend_storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use sha2::Digest;
use sha2::Sha256;

use crate::io::cache::FuseCaches;

/// The key of the boundaries of a runtime TopN filter of `source`, ordered first by `asc` and
/// `nulls_first` for `candidate_count` top rows; `None` if they are not cached: a scan of a table
/// without a snapshot or with security policies, a sample, or with non-deterministic filters.
pub fn top_n_boundary_key(
    source: &DataSourcePlan,
    asc: bool,
    nulls_first: bool,
    candidate_count: usize,
    func_ctx: &FunctionContext,
) -> Option<String> {
    let DataSourceInfo::TableSource(table_info) = &source.source_info else {
        return None;
    };
    let meta = &table_info.meta;
    let options = &meta.options;
    let snapshot = options
        .get(OPT_KEY_SNAPSHOT_LOCATION)
        .or_else(|| options.get(OPT_KEY_LEGACY_SNAPSHOT_LOC))?;
    // Policies depend on the user.
    if meta.row_access_policy.is_some()
        || meta
            .column_mask_policy
            .as_ref()
            .is_some_and(|policies| !policies.is_empty())
        || !meta.column_mask_policy_columns_ids.is_empty()
    {
        return None;
    }
    let push_downs = source.push_downs.as_ref();
    if let Some(push_downs) = push_downs {
        let deterministic = |filters: &Option<Filters>| {
            filters.as_ref().is_none_or(|filters| {
                filters
                    .filter
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .is_deterministic(&BUILTIN_FUNCTIONS)
                    && filters
                        .inverted_filter
                        .as_expr(&BUILTIN_FUNCTIONS)
                        .is_deterministic(&BUILTIN_FUNCTIONS)
            })
        };
        if push_downs.sample.is_some()
            || !deterministic(&push_downs.filters)
            || !deterministic(&push_downs.secure_filters)
            || push_downs.prewhere.as_ref().is_some_and(|prewhere| {
                !prewhere
                    .filter
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .is_deterministic(&BUILTIN_FUNCTIONS)
            })
        {
            return None;
        }
    }

    // The rows: the table and its snapshot, its schema (the values of the columns a block does not
    // have), the filters and what else the scan pushes down, and the arguments of a table
    // function. The function context the filters are evaluated with.
    let func_ctx = format!("{:?}", FunctionContext {
        now: Default::default(),
        ..func_ctx.clone()
    });
    let parts = [
        table_info.ident.table_id.to_string(),
        snapshot.clone(),
        serde_json::to_string(&meta.schema).ok()?,
        serde_json::to_string(&push_downs).ok()?,
        serde_json::to_string(&source.tbl_args).ok()?,
        serde_json::to_string(&source.base_block_ids).ok()?,
        format!("{asc}:{nulls_first}:{candidate_count}"),
        func_ctx,
    ];
    let mut hasher = Sha256::new();
    for part in &parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    Some(format!("{:x}", hasher.finalize()))
}

/// The boundary of `key`, if any.
pub fn lookup_top_n_boundary(key: &str) -> Option<Scalar> {
    FuseCaches::instance()
        .top_n_boundaries
        .lookup(key)
        .map(|value| value.0.clone())
}

/// Records the boundary of `key`.
pub fn insert_top_n_boundary(key: &str, boundary: Scalar) {
    if matches!(boundary, Scalar::Null) {
        return;
    }
    let cache = &FuseCaches::instance().top_n_boundaries;
    if cache.is_enabled() {
        cache.fill(key, TopNBoundaryValue(boundary));
    }
}

#[cfg(test)]
mod tests {
    use databend_common_catalog::plan::PushDownInfo;
    use databend_common_expression::ColumnRef;
    use databend_common_expression::Constant;
    use databend_common_expression::Expr;
    use databend_common_expression::type_check::check_function;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_meta_app::schema::TableInfo;

    use super::*;

    fn filter(name: &str, value: i64) -> Filters {
        let column = Expr::ColumnRef(ColumnRef {
            span: None,
            id: "k".to_string(),
            data_type: DataType::Number(NumberDataType::Int64),
            display_name: "k".to_string(),
        });
        let filter = check_function(
            None,
            name,
            &[],
            &[
                column,
                Expr::Constant(Constant {
                    span: None,
                    scalar: Scalar::Number(value.into()),
                    data_type: DataType::Number(NumberDataType::Int64),
                }),
            ],
            &BUILTIN_FUNCTIONS,
        )
        .unwrap();
        Filters {
            filter: filter.as_remote_expr(),
            inverted_filter: filter.as_remote_expr(),
        }
    }

    fn source(snapshot: Option<&str>, push_downs: Option<PushDownInfo>) -> DataSourcePlan {
        let mut table_info = TableInfo::default();
        if let Some(snapshot) = snapshot {
            table_info
                .meta
                .options
                .insert(OPT_KEY_SNAPSHOT_LOCATION.to_string(), snapshot.to_string());
        }
        DataSourcePlan {
            source_info: DataSourceInfo::TableSource(table_info),
            output_schema: Default::default(),
            parts: Default::default(),
            statistics: Default::default(),
            description: String::new(),
            tbl_args: None,
            push_downs,
            internal_columns: None,
            base_block_ids: None,
            block_meta_options: Default::default(),
            table_index: 0,
            scan_id: 0,
        }
    }

    #[test]
    fn test_top_n_boundary_key() {
        let func_ctx = FunctionContext::default();
        let key = |source: &DataSourcePlan, asc, candidates| {
            top_n_boundary_key(source, asc, false, candidates, &func_ctx)
        };
        let filtered = |name, value| PushDownInfo {
            filters: Some(filter(name, value)),
            ..Default::default()
        };
        let plain = source(Some("s1"), None);
        let base = key(&plain, true, 10).unwrap();
        assert_eq!(key(&source(Some("s1"), None), true, 10).unwrap(), base);

        // The rows, the order and the number of top rows are in the key.
        let others = [
            key(&source(Some("s2"), None), true, 10),
            key(&source(Some("s1"), Some(filtered("gt", 1))), true, 10),
            key(&source(Some("s1"), Some(filtered("gt", 2))), true, 10),
            key(&plain, false, 10),
            key(&plain, true, 11),
        ];
        for (i, other) in others.iter().enumerate() {
            assert_ne!(other.as_ref().unwrap(), &base, "{i}");
        }

        // Not cached: no snapshot, a sample, or a policy.
        assert!(key(&source(None, None), true, 10).is_none());
        let sampled = PushDownInfo {
            sample: Some(Default::default()),
            ..Default::default()
        };
        assert!(key(&source(Some("s1"), Some(sampled)), true, 10).is_none());
        let mut policy = source(Some("s1"), None);
        let DataSourceInfo::TableSource(table_info) = &mut policy.source_info else {
            unreachable!()
        };
        table_info.meta.row_access_policy = Some("p".to_string());
        assert!(key(&policy, true, 10).is_none());
    }
}
