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

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::runtime_filter_info::RuntimeLimitFilter;
use databend_common_catalog::runtime_filter_info::RuntimeTopNFilter;
use databend_common_catalog::table_context::TableContextRuntimeFilter;
use databend_common_expression::RemoteExpr;
use databend_common_expression::types::DataType;
use databend_common_sql::executor::physical_plans::SortDesc;
use databend_storages_common_table_meta::meta::supported_stat_type;

use crate::physical_plans::EvalScalar;
use crate::physical_plans::Filter;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::TableScan;
use crate::sessions::QueryContext;

pub(super) fn register_runtime_top_n_filter(
    ctx: &QueryContext,
    input: &PhysicalPlan,
    order_by: &[SortDesc],
    candidate_count: usize,
) -> Option<Arc<RuntimeTopNFilter>> {
    let (scan_id, filter) = create_runtime_top_n_filter(input, order_by, candidate_count)?;
    ctx.register_runtime_scan_filter(scan_id, filter.clone());
    Some(filter)
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

fn create_runtime_top_n_filter(
    input: &PhysicalPlan,
    order_by: &[SortDesc],
    candidate_count: usize,
) -> Option<(usize, Arc<RuntimeTopNFilter>)> {
    if candidate_count == 0 || order_by.len() != 1 {
        return None;
    }

    let source = runtime_scan_data_source(input)?;
    let DataSourceInfo::TableSource(table_info) = &source.source_info else {
        return None;
    };
    if table_info.engine() != "FUSE" {
        return None;
    }

    if source.statistics.read_rows < candidate_count.saturating_mul(2) {
        return None;
    }

    let push_down = source.push_downs.as_ref()?;
    let [(expr, asc, nulls_first)] = push_down.order_by.as_slice() else {
        return None;
    };
    let desc = &order_by[0];
    if *asc != desc.asc || *nulls_first != desc.nulls_first {
        return None;
    }

    let RemoteExpr::ColumnRef { id, data_type, .. } = expr else {
        return None;
    };
    if id != &desc.display_name || !supported_stat_type(data_type) {
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

    Some((
        source.scan_id,
        Arc::new(RuntimeTopNFilter::new(
            column_id,
            desc.asc,
            desc.nulls_first,
        )),
    ))
}

#[recursive::recursive]
fn runtime_scan_data_source(plan: &PhysicalPlan) -> Option<&DataSourcePlan> {
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
    use databend_common_catalog::plan::PartStatistics;
    use databend_common_catalog::plan::Partitions;
    use databend_common_catalog::plan::PushDownInfo;
    use databend_common_catalog::plan::VirtualColumnField;
    use databend_common_catalog::plan::VirtualColumnInfo;
    use databend_common_catalog::runtime_filter_info::RuntimeScanFilter;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;
    use databend_common_meta_app::schema::TableInfo;
    use jsonb::keypath::OwnedKeyPaths;

    use super::*;
    use crate::physical_plans::PhysicalPlanMeta;
    use crate::physical_plans::Sort;
    use crate::physical_plans::WindowPartition;
    use crate::physical_plans::physical_sort::SortStep;

    fn table_scan_with(
        scan_id: usize,
        schema: Arc<TableSchema>,
        push_downs: Option<PushDownInfo>,
        read_rows: usize,
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
        TableScan::create(
            scan_id,
            Default::default(),
            Box::new(source),
            None,
            None,
            None,
        )
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
        let data_type = DataType::Number(NumberDataType::Int32).wrap_nullable();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "a",
            TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
        )]));
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
        let (scan_id, filter) = create_runtime_top_n_filter(&scan, &order_by(), 5).unwrap();
        assert_eq!(scan_id, 3);
        let order = filter.preferred_order().unwrap();
        assert!(!order.asc);
        assert!(!order.nulls_first);

        let scan = table_scan_with(3, schema, Some(push_downs), 9);
        assert!(create_runtime_top_n_filter(&scan, &order_by(), 5).is_none());
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
        let (scan_id, filter) = create_runtime_top_n_filter(&scan, &order_by, 5).unwrap();
        assert_eq!(scan_id, 9);
        assert_eq!(filter.preferred_order().unwrap().column_id, query_column_id);

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
        assert!(create_runtime_top_n_filter(&scan, &variant_order_by, 5).is_none());
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
}
