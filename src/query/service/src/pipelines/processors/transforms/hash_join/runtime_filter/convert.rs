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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::runtime_filter_info::RuntimeFilterBloom;
use databend_common_catalog::runtime_filter_info::RuntimeFilterEntry;
use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_catalog::runtime_filter_info::RuntimeFilterSpatial;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStats;
use databend_common_catalog::sbbf::Sbbf;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnRef;
use databend_common_expression::Constant;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::type_check;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDomain;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::builder::should_enable_runtime_filter;
use super::packet::JoinRuntimeFilterPacket;
use super::packet::SerializableDomain;
use super::spatial::rtree_bounds_from_bytes;
use crate::pipelines::processors::transforms::hash_join::desc::RuntimeFilterDesc;
use crate::pipelines::processors::transforms::hash_join::util::min_max_filter;

/// # Note
///
/// The key in the resulting [`HashMap`] is the scan_id, which identifies the scan operator
/// where the runtime filter will be applied. This is different from the runtime filter's own id,
///
/// Each runtime filter (identified by packet.id) is built once and then applied to multiple scans.
/// The probe_targets in RuntimeFilterDesc specify all (probe_key, scan_id) pairs where this filter should be applied.
pub async fn build_runtime_filter_infos(
    packet: JoinRuntimeFilterPacket,
    runtime_filter_descs: HashMap<usize, &RuntimeFilterDesc>,
    selectivity_threshold: u64,
    _max_threads: usize,
) -> Result<HashMap<usize, RuntimeFilterInfo>> {
    let total_build_rows = packet.build_rows;
    let Some(packets) = packet.packets else {
        return Ok(HashMap::new());
    };
    let mut filters: HashMap<usize, RuntimeFilterInfo> = HashMap::new();

    // Iterate over all runtime filter packets
    for packet in packets.into_values() {
        let desc = runtime_filter_descs.get(&packet.id).unwrap();
        let bloom_enabled =
            should_enable_runtime_filter(desc, total_build_rows, selectivity_threshold);

        // Apply this single runtime filter to all probe targets (scan_id, probe_key pairs)
        // This implements the design goal: "one runtime filter built once, pushed down to multiple scans"
        for (probe_key, scan_id) in &desc.probe_targets {
            let entry = filters.entry(*scan_id).or_default();

            let spatial = if let Some(ref spatial_packet) = packet.spatial {
                let spatial_valid = spatial_packet.valid;
                let spatial_srid = spatial_packet.srid;
                let spatial_rtrees = &spatial_packet.rtrees;
                if spatial_valid && !spatial_rtrees.is_empty() && spatial_srid.is_some() {
                    let rtree_bounds = rtree_bounds_from_bytes(spatial_rtrees)?;
                    let probe_column = resolve_probe_column_ref(probe_key);
                    let column_name = probe_column.id.to_string();
                    let spatial = RuntimeFilterSpatial {
                        column_name,
                        srid: spatial_srid.unwrap(),
                        rtrees: Arc::new(spatial_rtrees.clone()),
                        rtree_bounds,
                    };
                    Some(spatial)
                } else {
                    None
                }
            } else {
                None
            };

            let (inlist, inlist_value_count) = if let Some(ref inlist) = packet.inlist {
                let (expr, value_count) = build_inlist_filter(inlist.clone(), probe_key)?;
                (Some(expr), value_count)
            } else {
                (None, 0)
            };
            let bloom = if bloom_enabled {
                if let Some(ref bloom) = packet.bloom {
                    Some(build_bloom_filter(bloom.clone(), probe_key)?)
                } else {
                    None
                }
            } else {
                None
            };

            let min_max = if let Some(ref min_max) = packet.min_max {
                Some(build_min_max_filter(
                    min_max.clone(),
                    probe_key,
                    &desc.build_key,
                )?)
            } else {
                None
            };
            let enabled =
                bloom.is_some() || inlist.is_some() || min_max.is_some() || spatial.is_some();

            let runtime_entry = RuntimeFilterEntry {
                id: desc.id,
                probe_expr: probe_key.clone(),
                bloom,
                spatial,
                inlist,
                inlist_value_count,
                min_max,
                stats: Arc::new(RuntimeFilterStats::new()),
                build_rows: total_build_rows,
                build_table_rows: desc.build_table_rows,
                enabled,
            };

            entry.filters.push(runtime_entry);
        }
    }
    Ok(filters)
}

fn build_inlist_filter(inlist: Column, probe_key: &Expr<String>) -> Result<(Expr<String>, usize)> {
    let inlist_value_count = inlist.len();
    if inlist.len() == 0 {
        return Ok((
            Expr::Constant(Constant {
                span: None,
                scalar: Scalar::Boolean(false),
                data_type: DataType::Boolean,
            }),
            0,
        ));
    }
    let probe_key = resolve_probe_column_ref(probe_key);

    let probe_data_type = probe_key.data_type.clone();
    let raw_probe_key = RawExpr::ColumnRef {
        span: probe_key.span,
        id: probe_key.id.to_string(),
        data_type: probe_key.data_type.clone(),
        display_name: probe_key.display_name.clone(),
    };

    let eq_exprs: Vec<RawExpr<String>> = inlist
        .iter()
        .map(|scalar_ref| RawExpr::FunctionCall {
            span: None,
            name: "eq".to_string(),
            params: vec![],
            args: vec![raw_probe_key.clone(), RawExpr::Constant {
                span: None,
                scalar: scalar_ref.to_owned(),
                data_type: Some(probe_data_type.clone()),
            }],
        })
        .collect();

    let or_filters_expr = if eq_exprs.len() == 1 {
        eq_exprs[0].clone()
    } else {
        RawExpr::FunctionCall {
            span: None,
            name: "or_filters".to_string(),
            params: vec![],
            args: eq_exprs,
        }
    };

    let expr = type_check::check(&or_filters_expr, &BUILTIN_FUNCTIONS)?;
    Ok((expr, inlist_value_count))
}

fn build_min_max_filter(
    min_max: SerializableDomain,
    probe_key: &Expr<String>,
    build_key: &Expr,
) -> Result<Expr<String>> {
    let min_max = Domain::from_min_max(
        min_max.min,
        min_max.max,
        &build_key.data_type().remove_nullable(),
    );
    let min_max_filter = match min_max {
        Domain::Number(domain) => match domain {
            NumberDomain::UInt8(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::UInt16(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::UInt32(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::UInt64(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::Int8(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::Int16(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::Int32(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::Int64(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::Float32(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::Float64(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
        },
        Domain::String(domain) => {
            let min = Scalar::String(domain.min);
            let max = Scalar::String(domain.max.unwrap());
            min_max_filter(min, max, probe_key)?
        }
        Domain::Date(date_domain) => {
            let min = Scalar::Date(date_domain.min);
            let max = Scalar::Date(date_domain.max);
            min_max_filter(min, max, probe_key)?
        }
        _ => {
            return Err(ErrorCode::UnsupportedDataType(format!(
                "Unsupported domain {:?} for runtime filter",
                min_max,
            )));
        }
    };
    Ok(min_max_filter)
}

fn build_bloom_filter(
    bloom_bytes: Vec<u8>,
    probe_key: &Expr<String>,
) -> Result<RuntimeFilterBloom> {
    let probe_column = resolve_probe_column_ref(probe_key);
    let column_name = probe_column.id.to_string();
    let filter = Sbbf::from_bytes(&bloom_bytes)
        .ok_or_else(|| ErrorCode::Internal("Invalid bloom filter bytes in runtime filter"))?;
    Ok(RuntimeFilterBloom {
        column_name,
        filter: Arc::new(filter),
    })
}

fn resolve_probe_column_ref(probe_key: &Expr<String>) -> &ColumnRef<String> {
    match probe_key {
        Expr::ColumnRef(col) => col,
        // Support simple cast that only changes nullability, e.g. CAST(col AS Nullable(T))
        Expr::Cast(cast) => match cast.expr.as_ref() {
            Expr::ColumnRef(col) => col,
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_catalog::sbbf::Sbbf;
    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::ColumnRef;
    use databend_common_expression::Constant;
    use databend_common_expression::ConstantFolder;
    use databend_common_expression::Domain;
    use databend_common_expression::Expr;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_functions::BUILTIN_FUNCTIONS;
    use geo_index::rtree::RTreeBuilder;
    use geo_index::rtree::sort::HilbertSort;

    use super::build_inlist_filter;
    use super::build_runtime_filter_infos;
    use crate::pipelines::processors::transforms::hash_join::desc::RuntimeFilterDesc;
    use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::JoinRuntimeFilterPacket;
    use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::RuntimeFilterPacket;
    use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::SerializableDomain;
    use crate::pipelines::processors::transforms::hash_join::runtime_filter::packet::SpatialPacket;

    #[tokio::test(flavor = "current_thread")]
    async fn test_build_runtime_filter_infos_selectivity_threshold_only_disables_bloom() {
        let data_type = DataType::Number(NumberDataType::Int32);
        let mut builder = ColumnBuilder::with_capacity(&data_type, 2);
        builder.push(Scalar::Number(1i32.into()).as_ref());
        builder.push(Scalar::Number(10i32.into()).as_ref());
        let inlist = builder.build();

        let build_key = Expr::ColumnRef(ColumnRef {
            span: None,
            id: 0,
            data_type: data_type.clone(),
            display_name: "build_key".to_string(),
        });
        let probe_key = Expr::ColumnRef(ColumnRef {
            span: None,
            id: "probe_key".to_string(),
            data_type: data_type.clone(),
            display_name: "probe_key".to_string(),
        });
        let desc = RuntimeFilterDesc {
            id: 0,
            build_key,
            probe_targets: vec![(probe_key, 7)],
            build_table_rows: Some(10),
            enable_bloom_runtime_filter: true,
            enable_inlist_runtime_filter: true,
            enable_min_max_runtime_filter: true,
            is_spatial: false,
        };

        let mut packets = HashMap::new();
        packets.insert(0, RuntimeFilterPacket {
            id: 0,
            inlist: Some(inlist),
            min_max: Some(SerializableDomain {
                min: Scalar::Number(1i32.into()),
                max: Scalar::Number(10i32.into()),
            }),
            bloom: Some({
                let mut f = Sbbf::new_with_ndv_fpp(100, 0.01).unwrap();
                f.insert_hash_batch(&[11, 22]);
                f.to_bytes()
            }),
            spatial: None,
        });

        let runtime_filter_infos = build_runtime_filter_infos(
            JoinRuntimeFilterPacket::complete(packets, 2),
            HashMap::from([(0, &desc)]),
            1,
            1,
        )
        .await
        .unwrap();

        let entry = &runtime_filter_infos.get(&7).unwrap().filters[0];
        assert!(entry.bloom.is_none());
        assert!(entry.inlist.is_some());
        assert_eq!(entry.inlist_value_count, 2);
        assert!(entry.min_max.is_some());
        assert!(entry.enabled);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_build_runtime_filter_infos_spatial() {
        let data_type = DataType::Number(NumberDataType::Int32);
        let build_key = Expr::ColumnRef(ColumnRef {
            span: None,
            id: 0,
            data_type: data_type.clone(),
            display_name: "build_key".to_string(),
        });
        let probe_key = Expr::ColumnRef(ColumnRef {
            span: None,
            id: "probe_key".to_string(),
            data_type: data_type.clone(),
            display_name: "probe_key".to_string(),
        });
        let desc = RuntimeFilterDesc {
            id: 0,
            build_key,
            probe_targets: vec![(probe_key, 42)],
            build_table_rows: Some(10),
            enable_bloom_runtime_filter: true,
            enable_inlist_runtime_filter: true,
            enable_min_max_runtime_filter: true,
            is_spatial: true,
        };

        let mut builder = RTreeBuilder::<f64>::new(1);
        builder.add(0.0, 0.0, 1.0, 1.0);
        let rtrees = builder.finish::<HilbertSort>().into_inner();

        let mut packets = HashMap::new();
        packets.insert(0, RuntimeFilterPacket {
            id: 0,
            inlist: None,
            min_max: None,
            bloom: None,
            spatial: Some(SpatialPacket {
                valid: true,
                srid: Some(4326),
                rtrees,
            }),
        });

        let runtime_filter_infos = build_runtime_filter_infos(
            JoinRuntimeFilterPacket::complete(packets, 1),
            HashMap::from([(0, &desc)]),
            1,
            1,
        )
        .await
        .unwrap();

        let entry = &runtime_filter_infos.get(&42).unwrap().filters[0];
        assert!(entry.bloom.is_none());
        assert!(entry.inlist.is_none());
        assert!(entry.min_max.is_none());
        assert!(entry.enabled);

        let spatial = entry.spatial.as_ref().unwrap();
        assert_eq!(spatial.column_name, "probe_key");
        assert_eq!(spatial.srid, 4326);
        assert_eq!(spatial.rtree_bounds, Some([0.0, 0.0, 1.0, 1.0]));
        assert!(!spatial.rtrees.is_empty());
    }

    #[test]
    fn test_build_inlist_filter() {
        let func_ctx = FunctionContext::default();

        // Create test column with values {1, 10}
        let data_type = DataType::Number(NumberDataType::Int32);
        let mut builder = ColumnBuilder::with_capacity(&data_type, 2);
        builder.push(Scalar::Number(1i32.into()).as_ref());
        builder.push(Scalar::Number(10i32.into()).as_ref());
        let inlist = builder.build();

        // Create probe key expression: column_a
        let probe_key = Expr::ColumnRef(ColumnRef {
            span: None,
            id: "column_a".to_string(),
            data_type: data_type.clone(),
            display_name: "column_a".to_string(),
        });

        // Build the filter expression
        let (filter_expr, inlist_value_count) = build_inlist_filter(inlist, &probe_key).unwrap();
        assert_eq!(inlist_value_count, 2);

        // Test with ConstantFolder - case where column_a in [2,10] (can be folded to constant)
        let mut input_domains = HashMap::new();
        let domain_value_2_10 = Domain::from_min_max(
            Scalar::Number(2i32.into()),
            Scalar::Number(10i32.into()),
            &data_type,
        );
        input_domains.insert("column_a".to_string(), domain_value_2_10);

        let (folded_expr, _) = ConstantFolder::fold_with_domain(
            &filter_expr,
            &input_domains,
            &func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Verify it's not folded to constant
        assert!(folded_expr.as_constant().is_none());

        // Test with ConstantFolder - case where column_a in [2,9] (should evaluate to false)
        let mut input_domains_false = HashMap::new();
        let domain_value_2_9 = Domain::from_min_max(
            Scalar::Number(2i32.into()),
            Scalar::Number(9i32.into()),
            &data_type,
        );
        input_domains_false.insert("column_a".to_string(), domain_value_2_9);

        let (folded_expr_false, _) = ConstantFolder::fold_with_domain(
            &filter_expr,
            &input_domains_false,
            &func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Range [2,9] does not intersect with {1, 10}, so it should fold to constant false
        match folded_expr_false {
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            }) => {
                println!("✓ Test passed: column_a in [2,9] correctly evaluated to false");
            }
            _ => {
                panic!("Expected constant false, got: {:?}", folded_expr_false);
            }
        }
    }

    fn collect_string_constant_types(expr: &Expr<String>, constant_types: &mut Vec<DataType>) {
        match expr {
            Expr::Constant(Constant {
                scalar: Scalar::String(_),
                data_type,
                ..
            }) => constant_types.push(data_type.clone()),
            Expr::Cast(cast) => collect_string_constant_types(&cast.expr, constant_types),
            Expr::FunctionCall(call) => {
                for arg in &call.args {
                    collect_string_constant_types(arg, constant_types);
                }
            }
            Expr::LambdaFunctionCall(call) => {
                for arg in &call.args {
                    collect_string_constant_types(arg, constant_types);
                }
            }
            Expr::Constant(_) | Expr::ColumnRef(_) => {}
        }
    }

    #[test]
    fn test_build_inlist_filter_nullable_string_preserves_constant_type() {
        let data_type = DataType::String;
        let probe_data_type = DataType::Nullable(Box::new(DataType::String));
        let mut builder = ColumnBuilder::with_capacity(&data_type, 2);
        builder.push(Scalar::String("a".to_string()).as_ref());
        builder.push(Scalar::String("b".to_string()).as_ref());
        let inlist = builder.build();

        let probe_key = Expr::ColumnRef(ColumnRef {
            span: None,
            id: "column_s".to_string(),
            data_type: probe_data_type.clone(),
            display_name: "column_s".to_string(),
        });

        let (filter_expr, inlist_value_count) = build_inlist_filter(inlist, &probe_key).unwrap();
        assert_eq!(inlist_value_count, 2);

        let mut constant_types = Vec::new();
        collect_string_constant_types(&filter_expr, &mut constant_types);
        assert_eq!(constant_types, vec![
            probe_data_type.clone(),
            probe_data_type
        ]);
    }

    #[test]
    fn test_build_inlist_filter_large() {
        let func_ctx = FunctionContext::default();

        // Create test column with 1024 elements: {0, 1, 2, ..., 1023}
        let data_type = DataType::Number(NumberDataType::Int32);
        let mut builder = ColumnBuilder::with_capacity(&data_type, 1024);
        for i in 0..1024 {
            builder.push(Scalar::Number((i).into()).as_ref());
        }
        let inlist = builder.build();

        // Create probe key expression: column_b
        let probe_key = Expr::ColumnRef(ColumnRef {
            span: None,
            id: "column_b".to_string(),
            data_type: data_type.clone(),
            display_name: "column_b".to_string(),
        });

        // Build the filter expression - this should create a balanced binary tree
        let (filter_expr, inlist_value_count) = build_inlist_filter(inlist, &probe_key).unwrap();
        assert_eq!(inlist_value_count, 1024);

        // Verify the expression was built successfully
        assert!(
            filter_expr.as_constant().is_none(),
            "Filter expression should not be a constant"
        );

        // Test with ConstantFolder - case where column_b in [500, 600]
        // (should intersect with our range [0, 1023])
        let mut input_domains = HashMap::new();
        let domain_value_500_600 = Domain::from_min_max(
            Scalar::Number(500i32.into()),
            Scalar::Number(600i32.into()),
            &data_type,
        );
        input_domains.insert("column_b".to_string(), domain_value_500_600);

        let (folded_expr, _) = ConstantFolder::fold_with_domain(
            &filter_expr,
            &input_domains,
            &func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Should not fold to constant since there's intersection
        assert!(
            folded_expr.as_constant().is_none(),
            "Expression should not fold to constant when there's intersection"
        );

        // Test with ConstantFolder - case where column_b in [2000, 3000]
        // (should NOT intersect with our range [0, 1023])
        let mut input_domains_no_intersect = HashMap::new();
        let domain_value_2000_3000 = Domain::from_min_max(
            Scalar::Number(2000i32.into()),
            Scalar::Number(3000i32.into()),
            &data_type,
        );
        input_domains_no_intersect.insert("column_b".to_string(), domain_value_2000_3000);

        let (folded_expr_false, _) = ConstantFolder::fold_with_domain(
            &filter_expr,
            &input_domains_no_intersect,
            &func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Range [2000, 3000] does not intersect with {0, 1, 2, ..., 1023},
        // so it should fold to constant false
        match folded_expr_false {
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            }) => {
                println!(
                    "✓ Test passed: column_b in [2000,3000] correctly evaluated to false for 1024 elements"
                );
            }
            _ => {
                panic!(
                    "Expected constant false for non-intersecting range, got: {:?}",
                    folded_expr_false
                );
            }
        }

        println!(
            "✓ Large inlist filter test (1024 elements) passed - balanced binary tree working correctly"
        );
    }
}
