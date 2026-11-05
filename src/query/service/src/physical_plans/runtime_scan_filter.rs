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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::runtime_filter_info::RuntimeLimitFilter;
use databend_common_catalog::runtime_filter_info::RuntimeTopNBoundary;
use databend_common_catalog::runtime_filter_info::RuntimeTopNFilter;
use databend_common_catalog::table_context::TableContextRuntimeFilter;
use databend_common_expression::ColumnId;
use databend_common_expression::RemoteExpr;
use databend_common_expression::types::DataType;
use databend_common_sql::Symbol;
use databend_common_sql::executor::physical_plans::SortDesc;
use databend_storages_common_table_meta::meta::supported_stat_type;

use crate::physical_plans::EvalScalar;
use crate::physical_plans::Filter;
use crate::physical_plans::FuseBlockRead;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::TableScan;
use crate::physical_plans::UnionAll;
use crate::sessions::QueryContext;

/// Register a runtime TopN filter on every eligible scan feeding `input`, and
/// return the boundary they share. The TopN over `input` publishes it.
pub(super) fn register_runtime_top_n_filter(
    ctx: &QueryContext,
    input: &PhysicalPlan,
    order_by: &[SortDesc],
    candidate_count: usize,
) -> Option<Arc<RuntimeTopNBoundary>> {
    let (boundary, filters) = create_runtime_top_n_filters(input, order_by, candidate_count)?;
    for (scan_id, filter) in filters {
        ctx.register_runtime_scan_filter(scan_id, filter);
    }
    Some(boundary)
}

pub(super) fn register_runtime_limit_filter(
    ctx: &QueryContext,
    input: &PhysicalPlan,
) -> Option<Arc<RuntimeLimitFilter>> {
    let source = runtime_scan_data_source(input)?;
    let DataSourceInfo::TableSource(table_info) = &source.source_info else {
        return None;
    };
    if table_info.engine() != "FUSE" {
        return None;
    }

    let filter = Arc::new(RuntimeLimitFilter::new());
    ctx.register_runtime_scan_filter(source.scan_id, filter.clone());
    Some(filter)
}

type RuntimeTopNFilters = (
    Arc<RuntimeTopNBoundary>,
    Vec<(usize, Arc<RuntimeTopNFilter>)>,
);

fn create_runtime_top_n_filters(
    input: &PhysicalPlan,
    order_by: &[SortDesc],
    candidate_count: usize,
) -> Option<RuntimeTopNFilters> {
    if candidate_count == 0 || order_by.is_empty() {
        return None;
    }
    let desc = &order_by[0];

    let mut scans = Vec::new();
    collect_runtime_top_n_scans(input, desc.order_by, &mut scans);
    let targets = scans
        .iter()
        .filter_map(|scan| {
            let column_id = runtime_top_n_column_id(scan, order_by, candidate_count)?;
            Some((scan.source.scan_id, column_id))
        })
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return None;
    }

    let boundary = Arc::new(RuntimeTopNBoundary::new(desc.asc, desc.nulls_first));
    let filters = targets
        .into_iter()
        .map(|(scan_id, column_id)| {
            let filter = Arc::new(RuntimeTopNFilter::new(column_id, boundary.clone()));
            (scan_id, filter)
        })
        .collect();
    Some((boundary, filters))
}

/// A scan feeding a TopN, and the symbol under which the scan outputs the
/// TopN sort column.
struct RuntimeTopNScan<'a> {
    source: &'a DataSourcePlan,
    name_mapping: &'a BTreeMap<String, String>,
    sort_column: Symbol,
}

/// The statistics column id of the first sort column, if the scan supports
/// runtime TopN pruning for it.
fn runtime_top_n_column_id(
    scan: &RuntimeTopNScan,
    order_by: &[SortDesc],
    candidate_count: usize,
) -> Option<ColumnId> {
    let source = scan.source;
    let DataSourceInfo::TableSource(table_info) = &source.source_info else {
        return None;
    };
    if table_info.engine() != "FUSE" {
        return None;
    }

    if source.statistics.read_rows < candidate_count.saturating_mul(2) {
        return None;
    }

    // The boundary tracks only the first sort key. A row whose first key is
    // strictly worse than the first key of the N-th candidate ranks after all
    // N candidates whatever its later keys are, so pruning by the first key
    // also holds for several keys. The pushed-down order must list the same
    // keys, so that its first item is the first sort key.
    let push_down = source.push_downs.as_ref()?;
    if push_down.order_by.len() != order_by.len() {
        return None;
    }
    let (expr, asc, nulls_first) = &push_down.order_by[0];
    let desc = &order_by[0];
    if *asc != desc.asc || *nulls_first != desc.nulls_first {
        return None;
    }

    let RemoteExpr::ColumnRef { id, data_type, .. } = expr else {
        return None;
    };
    // The pushed-down ordering must start with the column the scan outputs as
    // the first TopN sort column.
    let sort_column = scan.sort_column.to_string();
    if scan.name_mapping.get(id) != Some(&sort_column) || !supported_stat_type(data_type) {
        return None;
    }

    let schema = source.source_info.schema();
    let column_id = if let Ok(field) = schema.field_with_name(id) {
        if DataType::from(field.data_type()) != *data_type {
            return None;
        }
        let column_ids = field.leaf_column_ids();
        let [column_id] = column_ids.as_slice() else {
            return None;
        };
        *column_id
    } else {
        let virtual_column = push_down.order_by_virtual_column()?;
        virtual_column.query_column_id
    };

    Some(column_id)
}

/// Collect the scans whose rows reach the output of `plan`, each with the
/// symbol under which it outputs `sort_column`.
///
/// A distributed Fuse metadata exchange is part of the scan itself. Above the
/// scan, the traversal crosses the row-preserving `Filter`/`EvalScalar` and
/// `UNION ALL`. Every output row of a union comes from one of its branches, so
/// a boundary over the union output is valid for each branch scan. Other
/// operators stop the traversal of their subtree only, so the other branches
/// of a union still get filters.
#[recursive::recursive]
fn collect_runtime_top_n_scans<'a>(
    plan: &'a PhysicalPlan,
    sort_column: Symbol,
    scans: &mut Vec<RuntimeTopNScan<'a>>,
) {
    if let Some(scan) = plan.as_any().downcast_ref::<FuseBlockRead>() {
        scans.push(RuntimeTopNScan {
            source: &scan.source,
            name_mapping: &scan.name_mapping,
            sort_column,
        });
        return;
    }

    if let Some(scan) = plan.as_any().downcast_ref::<TableScan>() {
        scans.push(RuntimeTopNScan {
            source: &scan.source,
            name_mapping: &scan.name_mapping,
            sort_column,
        });
        return;
    }

    if let Some(filter) = plan.as_any().downcast_ref::<Filter>() {
        return collect_runtime_top_n_scans(&filter.input, sort_column, scans);
    }

    if let Some(eval_scalar) = plan.as_any().downcast_ref::<EvalScalar>() {
        return collect_runtime_top_n_scans(&eval_scalar.input, sort_column, scans);
    }

    if let Some(union_all) = plan.as_any().downcast_ref::<UnionAll>() {
        // A recursive CTE union reads its branches through the CTE source.
        if !union_all.cte_scan_names.is_empty() {
            return;
        }
        let Ok(offset) = union_all.schema.index_of(&sort_column.to_string()) else {
            return;
        };
        let output_type = union_all.schema.field(offset).data_type();
        for (input, outputs) in [
            (&union_all.left, &union_all.left_outputs),
            (&union_all.right, &union_all.right_outputs),
        ] {
            let branch_column = outputs
                .get(offset)
                .and_then(|output| union_branch_column(input, output, output_type));
            if let Some(branch_column) = branch_column {
                collect_runtime_top_n_scans(input, branch_column, scans);
            }
        }
    }
}

/// The symbol under which a union branch outputs a union column of type
/// `output_type`, if the branch column has the same values, so boundary values
/// compare with its statistics. The binder coerces a branch column by casting
/// it to the union type; only a cast that adds nullability keeps the values.
fn union_branch_column(
    input: &PhysicalPlan,
    (symbol, coercion): &(Symbol, Option<RemoteExpr>),
    output_type: &DataType,
) -> Option<Symbol> {
    let schema = input.output_schema().ok()?;
    let offset = schema.index_of(&symbol.to_string()).ok()?;
    let branch_type = schema.field(offset).data_type();
    if branch_type.remove_nullable() != output_type.remove_nullable() {
        return None;
    }
    match coercion {
        None => Some(*symbol),
        Some(RemoteExpr::Cast {
            is_try: false,
            expr,
            dest_type,
            ..
        }) if matches!(dest_type, DataType::Nullable(_))
            && matches!(expr.as_ref(), RemoteExpr::ColumnRef { id, .. } if *id == offset) =>
        {
            Some(*symbol)
        }
        Some(_) => None,
    }
}

/// A distributed Fuse metadata exchange is part of the scan itself. Above the scan,
/// only `Filter`/`EvalScalar` may be crossed; other operators stop the traversal.
#[recursive::recursive]
fn runtime_scan_data_source(plan: &PhysicalPlan) -> Option<&DataSourcePlan> {
    if let Some(scan) = plan.as_any().downcast_ref::<FuseBlockRead>() {
        return Some(&scan.source);
    }

    if let Some(scan) = plan.as_any().downcast_ref::<TableScan>() {
        return Some(&scan.source);
    }

    if let Some(filter) = plan.as_any().downcast_ref::<Filter>() {
        return runtime_scan_data_source(&filter.input);
    }

    if let Some(eval_scalar) = plan.as_any().downcast_ref::<EvalScalar>() {
        return runtime_scan_data_source(&eval_scalar.input);
    }

    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_catalog::plan::PartStatistics;
    use databend_common_catalog::plan::Partitions;
    use databend_common_catalog::plan::PushDownInfo;
    use databend_common_catalog::plan::VirtualColumnField;
    use databend_common_catalog::plan::VirtualColumnInfo;
    use databend_common_catalog::runtime_filter_info::RuntimeScanFilter;
    use databend_common_catalog::runtime_filter_info::RuntimeScanStatistics;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchemaRefExt;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_meta_app::schema::TableInfo;
    use databend_storages_common_table_meta::meta::ColumnStatistics;
    use jsonb::keypath::OwnedKeyPaths;

    use super::*;
    use crate::physical_plans::PhysicalPlanMeta;
    use crate::physical_plans::Sort;
    use crate::physical_plans::WindowPartition;
    use crate::physical_plans::physical_sort::SortStep;

    /// A scan that outputs its pushed-down sort column as symbol #0, the
    /// symbol used by `order_by()`.
    fn table_scan_with(
        scan_id: usize,
        schema: Arc<TableSchema>,
        push_downs: Option<PushDownInfo>,
        read_rows: usize,
    ) -> PhysicalPlan {
        table_scan_outputting(scan_id, schema, push_downs, read_rows, 0)
    }

    /// A scan that outputs its pushed-down sort column as `sort_symbol`.
    fn table_scan_outputting(
        scan_id: usize,
        schema: Arc<TableSchema>,
        push_downs: Option<PushDownInfo>,
        read_rows: usize,
        sort_symbol: usize,
    ) -> PhysicalPlan {
        let mut name_mapping = BTreeMap::new();
        if let Some([(RemoteExpr::ColumnRef { id, .. }, _, _)]) = push_downs
            .as_ref()
            .map(|push_downs| push_downs.order_by.as_slice())
        {
            name_mapping.insert(id.clone(), sort_symbol.to_string());
        }
        table_scan_mapping(scan_id, schema, push_downs, read_rows, name_mapping)
    }

    /// A scan that outputs table columns under the symbols of `name_mapping`.
    fn table_scan_mapping(
        scan_id: usize,
        schema: Arc<TableSchema>,
        push_downs: Option<PushDownInfo>,
        read_rows: usize,
        name_mapping: BTreeMap<String, String>,
    ) -> PhysicalPlan {
        let mut table_info = TableInfo::simple("default", "top_n_source", schema.clone());
        table_info.meta.engine = "FUSE".to_string();
        let source = DataSourcePlan {
            source_info: DataSourceInfo::TableSource(table_info),
            output_schema: schema,
            parts: Partitions::default(),
            statistics: PartStatistics {
                read_rows,
                ..PartStatistics::default()
            },
            description: String::new(),
            tbl_args: None,
            push_downs,
            internal_columns: None,
            base_block_ids: None,
            block_meta_options: Default::default(),
            table_index: 0,
            scan_id,
        };
        TableScan::create(scan_id, name_mapping, Box::new(source), None, None, None)
    }

    /// The filter of a TopN fed by exactly one eligible scan.
    fn single_runtime_top_n_filter(
        input: &PhysicalPlan,
        order_by: &[SortDesc],
        candidate_count: usize,
    ) -> Option<(usize, Arc<RuntimeTopNFilter>)> {
        let (_, mut filters) = create_runtime_top_n_filters(input, order_by, candidate_count)?;
        assert_eq!(filters.len(), 1);
        filters.pop()
    }

    fn table_scan(scan_id: usize) -> PhysicalPlan {
        table_scan_with(scan_id, Arc::new(TableSchema::empty()), None, 0)
    }

    fn order_by() -> Vec<SortDesc> {
        vec![SortDesc {
            asc: false,
            nulls_first: false,
            order_by: databend_common_expression::Symbol::new(0),
            display_name: "a".to_string(),
        }]
    }

    fn nullable_int_pushdown() -> (Arc<TableSchema>, PushDownInfo) {
        int_pushdown(NumberDataType::Int32, true)
    }

    /// A table with one integer column `a` and a pushed-down `a DESC NULLS LAST`.
    fn int_pushdown(
        number_type: NumberDataType,
        nullable: bool,
    ) -> (Arc<TableSchema>, PushDownInfo) {
        let mut table_type = TableDataType::Number(number_type);
        if nullable {
            table_type = table_type.wrap_nullable();
        }
        let data_type = DataType::from(&table_type);
        let schema = Arc::new(TableSchema::new(vec![TableField::new("a", table_type)]));
        let push_downs = PushDownInfo {
            order_by: vec![(
                RemoteExpr::ColumnRef {
                    span: None,
                    id: "a".to_string(),
                    data_type,
                    display_name: "a".to_string(),
                },
                false,
                false,
            )],
            ..PushDownInfo::default()
        };
        (schema, push_downs)
    }

    #[test]
    fn runtime_top_n_filter_supports_nullable_keys_behind_a_cost_gate() {
        let (schema, push_downs) = nullable_int_pushdown();

        let scan = table_scan_with(3, schema.clone(), Some(push_downs.clone()), 1000);
        let (scan_id, filter) = single_runtime_top_n_filter(&scan, &order_by(), 5).unwrap();
        assert_eq!(scan_id, 3);
        let order = filter.preferred_order().unwrap();
        assert!(!order.asc);
        assert!(!order.nulls_first);

        let scan = table_scan_with(3, schema, Some(push_downs), 9);
        assert!(single_runtime_top_n_filter(&scan, &order_by(), 5).is_none());
    }

    #[test]
    fn runtime_top_n_filter_tracks_the_first_of_several_sort_keys() {
        let int = TableDataType::Number(NumberDataType::Int32);
        let schema = Arc::new(TableSchema::new(vec![
            TableField::new("a", int.clone()),
            TableField::new("b", int),
        ]));
        let column = |name: &str| RemoteExpr::ColumnRef {
            span: None,
            id: name.to_string(),
            data_type: DataType::Number(NumberDataType::Int32),
            display_name: name.to_string(),
        };
        let sort = |name: &str, index| SortDesc {
            asc: true,
            nulls_first: false,
            order_by: databend_common_expression::Symbol::new(index),
            display_name: name.to_string(),
        };
        let scan_with = |keys: &[&str]| {
            let push_downs = PushDownInfo {
                order_by: keys.iter().map(|k| (column(k), true, false)).collect(),
                ..PushDownInfo::default()
            };
            let name_mapping = BTreeMap::from([
                ("a".to_string(), "0".to_string()),
                ("b".to_string(), "1".to_string()),
            ]);
            table_scan_mapping(4, schema.clone(), Some(push_downs), 1000, name_mapping)
        };
        let order_by = vec![sort("b", 1), sort("a", 0)];

        let (scan_id, filter) =
            single_runtime_top_n_filter(&scan_with(&["b", "a"]), &order_by, 5).unwrap();
        assert_eq!(scan_id, 4);
        let column_id = schema.field_with_name("b").unwrap().leaf_column_ids()[0];
        assert_eq!(filter.preferred_order().unwrap().column_id, column_id);

        // The pushed-down order must list exactly the sort keys.
        assert!(single_runtime_top_n_filter(&scan_with(&["b"]), &order_by, 5).is_none());
        assert!(single_runtime_top_n_filter(&scan_with(&["a", "b"]), &order_by, 5).is_none());
    }

    #[test]
    fn runtime_scan_filters_support_distributed_fuse_block_read() {
        let (schema, push_downs) = nullable_int_pushdown();
        let scan = table_scan_with(7, schema, Some(push_downs), 1000);
        let scan =
            FuseBlockRead::create(scan.as_any().downcast_ref::<TableScan>().unwrap().clone());
        let block_read = scan.as_any().downcast_ref::<FuseBlockRead>().unwrap();

        // The metadata exchange below the reader is not a runtime scan filter target.
        assert!(runtime_scan_data_source(&block_read.input).is_none());
        assert_eq!(runtime_scan_data_source(&scan).unwrap().scan_id, 7);

        let filter = PhysicalPlan::new(Filter {
            meta: PhysicalPlanMeta::new("Filter"),
            projections: Default::default(),
            input: scan,
            predicates: vec![],
            stat_info: None,
            is_secure: false,
        });
        let eval_scalar =
            PhysicalPlan::new(EvalScalar::create(filter, vec![], Default::default(), None));
        assert_eq!(runtime_scan_data_source(&eval_scalar).unwrap().scan_id, 7);
        let (scan_id, _) = single_runtime_top_n_filter(&eval_scalar, &order_by(), 5).unwrap();
        assert_eq!(scan_id, 7);

        let exchange = PhysicalPlan::new(crate::physical_plans::Exchange {
            meta: PhysicalPlanMeta::new("Exchange"),
            input: eval_scalar,
            kind: databend_common_sql::executor::physical_plans::FragmentKind::Normal,
            keys: vec![],
            ignore_exchange: false,
            allow_adjust_parallelism: false,
        });
        assert!(runtime_scan_data_source(&exchange).is_none());
        assert!(single_runtime_top_n_filter(&exchange, &order_by(), 5).is_none());
    }

    #[test]
    fn runtime_top_n_filter_supports_typed_virtual_columns() {
        let query_column_id = 3_000_000_000;
        let name = "v['a']::Int64".to_string();
        let data_type = DataType::Number(NumberDataType::Int64).wrap_nullable();
        let virtual_field = VirtualColumnField {
            source_column_id: 1,
            source_name: "v".to_string(),
            query_column_id,
            name: name.clone(),
            key_paths: OwnedKeyPaths::from_canonical_path("a").unwrap(),
            data_type: Box::new(TableDataType::Number(NumberDataType::Int64).wrap_nullable()),
            is_try: false,
        };
        let push_downs = PushDownInfo {
            order_by: vec![(
                RemoteExpr::ColumnRef {
                    span: None,
                    id: name.clone(),
                    data_type: data_type.clone(),
                    display_name: name.clone(),
                },
                true,
                false,
            )],
            limit: Some(5),
            virtual_column: Some(VirtualColumnInfo {
                source_column_ids: [1].into_iter().collect(),
                ignorable_source_column_ids: [1].into_iter().collect(),
                virtual_column_fields: vec![virtual_field.clone()],
            }),
            ..PushDownInfo::default()
        };
        let order_by = vec![SortDesc {
            asc: true,
            nulls_first: false,
            order_by: databend_common_expression::Symbol::new(0),
            display_name: name.clone(),
        }];
        let scan = table_scan_with(
            9,
            Arc::new(TableSchema::empty()),
            Some(push_downs.clone()),
            1000,
        );
        let distributed_scan =
            FuseBlockRead::create(scan.as_any().downcast_ref::<TableScan>().unwrap().clone());
        for scan in [scan, distributed_scan] {
            let (scan_id, filter) = single_runtime_top_n_filter(&scan, &order_by, 5).unwrap();
            assert_eq!(scan_id, 9);
            assert_eq!(filter.preferred_order().unwrap().column_id, query_column_id);
        }

        let mut variant_field = virtual_field;
        variant_field.name = "v['a']".to_string();
        variant_field.data_type = Box::new(TableDataType::Variant.wrap_nullable());
        let variant_type = DataType::Variant.wrap_nullable();
        let variant_push_downs = PushDownInfo {
            order_by: vec![(
                RemoteExpr::ColumnRef {
                    span: None,
                    id: variant_field.name.clone(),
                    data_type: variant_type,
                    display_name: variant_field.name.clone(),
                },
                true,
                false,
            )],
            limit: Some(5),
            virtual_column: Some(VirtualColumnInfo {
                source_column_ids: [1].into_iter().collect(),
                ignorable_source_column_ids: [1].into_iter().collect(),
                virtual_column_fields: vec![variant_field.clone()],
            }),
            ..PushDownInfo::default()
        };
        let variant_order_by = vec![SortDesc {
            asc: true,
            nulls_first: false,
            order_by: databend_common_expression::Symbol::new(0),
            display_name: variant_field.name,
        }];
        let scan = table_scan_with(
            10,
            Arc::new(TableSchema::empty()),
            Some(variant_push_downs),
            1000,
        );
        let distributed_scan =
            FuseBlockRead::create(scan.as_any().downcast_ref::<TableScan>().unwrap().clone());
        for scan in [scan, distributed_scan] {
            assert!(single_runtime_top_n_filter(&scan, &variant_order_by, 5).is_none());
        }
    }

    #[test]
    fn runtime_scan_data_source_only_crosses_row_preserving_wrappers() {
        let scan = table_scan(7);
        let filter = PhysicalPlan::new(Filter {
            meta: PhysicalPlanMeta::new("Filter"),
            projections: Default::default(),
            input: scan.clone(),
            predicates: vec![],
            stat_info: None,
            is_secure: false,
        });
        let eval_scalar =
            PhysicalPlan::new(EvalScalar::create(filter, vec![], Default::default(), None));

        assert_eq!(
            runtime_scan_data_source(&eval_scalar).map(|source| source.scan_id),
            Some(7)
        );

        let window = PhysicalPlan::new(WindowPartition {
            meta: PhysicalPlanMeta::new("WindowPartition"),
            input: scan.clone(),
            partition_by: vec![],
            order_by: vec![],
            top_n: None,
            stat_info: None,
        });
        assert!(window.try_find_single_data_source().is_some());
        assert!(runtime_scan_data_source(&window).is_none());

        let sort = PhysicalPlan::new(Sort {
            meta: PhysicalPlanMeta::new("Sort"),
            input: scan,
            order_by: vec![],
            limit: None,
            step: SortStep::Single,
            pre_projection: None,
            broadcast_id: None,
            enable_fixed_rows: false,
            stat_info: None,
        });
        assert!(sort.try_find_single_data_source().is_some());
        assert!(runtime_scan_data_source(&sort).is_none());
    }

    /// A single-column union. Each branch is `(input, symbol, cast)`: the
    /// branch outputs the union column as `symbol`, coerced by `cast`.
    fn union_all(
        left: (PhysicalPlan, usize, Option<DataType>),
        right: (PhysicalPlan, usize, Option<DataType>),
        output_type: DataType,
    ) -> PhysicalPlan {
        PhysicalPlan::new(union_all_node(left, right, output_type))
    }

    fn union_all_node(
        left: (PhysicalPlan, usize, Option<DataType>),
        right: (PhysicalPlan, usize, Option<DataType>),
        output_type: DataType,
    ) -> UnionAll {
        // The physical plan builder projects a coerced branch column at offset 0
        // of the single-column branch schema.
        let output = |input: &PhysicalPlan, symbol: usize, cast: Option<DataType>| {
            let coercion = cast.map(|dest_type| RemoteExpr::Cast {
                span: None,
                is_try: false,
                expr: Box::new(RemoteExpr::ColumnRef {
                    span: None,
                    id: 0,
                    data_type: input.output_schema().unwrap().field(0).data_type().clone(),
                    display_name: symbol.to_string(),
                }),
                dest_type,
            });
            (Symbol::new(symbol), coercion)
        };
        UnionAll {
            meta: PhysicalPlanMeta::new("UnionAll"),
            left_outputs: vec![output(&left.0, left.1, left.2)],
            right_outputs: vec![output(&right.0, right.1, right.2)],
            left: left.0,
            right: right.0,
            schema: DataSchemaRefExt::create(vec![DataField::new("0", output_type)]),
            cte_scan_names: vec![],
            logical_recursive_cte_id: None,
            stat_info: None,
        }
    }

    fn int32(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }

    #[test]
    fn runtime_top_n_filter_spans_union_all_branches() {
        let (schema, push_downs) = nullable_int_pushdown();
        let scan = |scan_id, sort_symbol| {
            table_scan_outputting(
                scan_id,
                schema.clone(),
                Some(push_downs.clone()),
                1000,
                sort_symbol,
            )
        };
        let filtered_scan = PhysicalPlan::new(Filter {
            meta: PhysicalPlanMeta::new("Filter"),
            projections: [0].into(),
            input: scan(4, 2),
            predicates: vec![],
            stat_info: None,
            is_secure: false,
        });
        let output_type = DataType::Number(NumberDataType::Int32).wrap_nullable();
        let inner = union_all(
            (scan(3, 1), 1, None),
            (filtered_scan, 2, None),
            output_type.clone(),
        );
        // Each branch outputs the union column under its own symbol.
        let union_input = union_all((inner, 0, None), (scan(5, 6), 6, None), output_type);

        let (boundary, filters) =
            create_runtime_top_n_filters(&union_input, &order_by(), 5).unwrap();
        assert_eq!(
            filters
                .iter()
                .map(|(scan_id, _)| *scan_id)
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );

        // One boundary, published once by the TopN, prunes blocks of every branch.
        boundary.update(&int32(10));
        let worse = HashMap::from([(0, ColumnStatistics::new(int32(1), int32(9), 0, 0, None))]);
        let tied = HashMap::from([(0, ColumnStatistics::new(int32(1), int32(10), 0, 0, None))]);
        for (_, filter) in &filters {
            assert!(filter.should_prune(RuntimeScanStatistics::from_columns(Some(&worse))));
            assert!(!filter.should_prune(RuntimeScanStatistics::from_columns(Some(&tied))));
        }
    }

    #[test]
    fn runtime_top_n_filter_skips_ineligible_union_branches() {
        let (schema, push_downs) = nullable_int_pushdown();
        let nullable_int32 = DataType::Number(NumberDataType::Int32).wrap_nullable();
        let scan = |scan_id, sort_symbol| {
            table_scan_outputting(
                scan_id,
                schema.clone(),
                Some(push_downs.clone()),
                1000,
                sort_symbol,
            )
        };
        let scan_ids = |plan: &PhysicalPlan| {
            create_runtime_top_n_filters(plan, &order_by(), 5).map(|(_, filters)| {
                filters
                    .iter()
                    .map(|(scan_id, _)| *scan_id)
                    .collect::<Vec<_>>()
            })
        };

        // A row-changing branch stops the traversal of that branch only.
        let sort = PhysicalPlan::new(Sort {
            meta: PhysicalPlanMeta::new("Sort"),
            input: scan(4, 2),
            order_by: vec![],
            limit: None,
            step: SortStep::Single,
            pre_projection: None,
            broadcast_id: None,
            enable_fixed_rows: false,
            stat_info: None,
        });
        let plan = union_all(
            (scan(3, 1), 1, None),
            (sort, 2, None),
            nullable_int32.clone(),
        );
        assert_eq!(scan_ids(&plan), Some(vec![3]));

        // A cast that only adds nullability keeps the branch values.
        let (non_null_schema, non_null_push_downs) = int_pushdown(NumberDataType::Int32, false);
        let non_null_scan = || {
            table_scan_outputting(
                4,
                non_null_schema.clone(),
                Some(non_null_push_downs.clone()),
                1000,
                2,
            )
        };
        let plan = union_all(
            (scan(3, 1), 1, None),
            (non_null_scan(), 2, Some(nullable_int32.clone())),
            nullable_int32.clone(),
        );
        assert_eq!(scan_ids(&plan), Some(vec![3, 4]));

        // A cast that removes nullability drops or rejects null rows, so the
        // branch values are not the union values.
        let plan = union_all(
            (non_null_scan(), 2, None),
            (scan(3, 1), 1, Some(DataType::Number(NumberDataType::Int32))),
            DataType::Number(NumberDataType::Int32),
        );
        assert_eq!(scan_ids(&plan), Some(vec![4]));

        // A cast to a wider type changes the scalar domain of the boundary.
        let (int64_schema, int64_push_downs) = int_pushdown(NumberDataType::Int64, false);
        let int64_scan = table_scan_outputting(3, int64_schema, Some(int64_push_downs), 1000, 1);
        let plan = union_all(
            (int64_scan, 1, None),
            (
                non_null_scan(),
                2,
                Some(DataType::Number(NumberDataType::Int64)),
            ),
            DataType::Number(NumberDataType::Int64),
        );
        assert_eq!(scan_ids(&plan), Some(vec![3]));

        // A recursive CTE union reads its branches through the CTE source.
        let mut recursive_cte =
            union_all_node((scan(3, 1), 1, None), (scan(4, 2), 2, None), nullable_int32);
        recursive_cte.cte_scan_names = vec!["t".to_string()];
        assert_eq!(scan_ids(&PhysicalPlan::new(recursive_cte)), None);

        // The scan must output the pushed-down column as the TopN sort column.
        assert_eq!(scan_ids(&scan(3, 9)), None);
    }
}
