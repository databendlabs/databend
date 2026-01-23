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

use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::F64;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::map::KvPair;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use h3o::CellIndex;
use h3o::DirectedEdgeIndex;
use h3o::LatLng;
use h3o::Resolution;

pub fn register(registry: &mut FunctionRegistry) {
    registry
        .scalar_builder("h3_to_geo")
        .function()
        .typed_1_arg::<UInt64Type, KvPair<Float64Type, Float64Type>>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<
            UInt64Type,
            KvPair<Float64Type, Float64Type>,
        >(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(h3_cell) => {
                    let coord: LatLng = h3_cell.into();
                    builder.push((coord.lng().into(), coord.lat().into()));
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e);
                    builder.push((F64::from(0.0), F64::from(0.0)))
                }
            }
        }))
        .register();

    registry
        .scalar_builder("h3_to_geo_boundary")
        .function()
        .typed_1_arg::<UInt64Type, ArrayType<KvPair<Float64Type, Float64Type>>>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<
            UInt64Type,
            ArrayType<KvPair<Float64Type, Float64Type>>,
        >(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(h3_cell) => {
                    let boundary = h3_cell.boundary();
                    let coord_list = boundary.iter().collect::<Vec<_>>();
                    for coord in coord_list {
                        builder.put_item((coord.lng().into(), coord.lat().into()));
                    }
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e);
                    builder.put_item((F64::from(0.0), F64::from(0.0)));
                }
            }
            builder.commit_row();
        }))
        .register();

    registry
        .register_passthrough_nullable_2_arg::<UInt64Type, UInt32Type, ArrayType<UInt64Type>, _, _>(
            "h3_k_ring",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<UInt64Type, UInt32Type, ArrayType<UInt64Type>>(
                |h3, k, builder, ctx| {
                    match CellIndex::try_from(h3) {
                        Ok(h3_cell) => {
                            let ring = h3_cell.grid_disk::<Vec<_>>(k);
                            for item in ring {
                                builder.put_item(item.into());
                            }
                        }
                        Err(e) => {
                            ctx.set_error(builder.len(), e);
                            builder.put_item(0);
                        }
                    }
                    builder.commit_row();
                },
            ),
        );

    registry
        .scalar_builder("h3_is_valid")
        .function()
        .typed_1_arg::<UInt64Type, BooleanType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<UInt64Type, BooleanType>(
            |h3, builder, _| {
                if CellIndex::try_from(h3).is_ok() {
                    builder.push(true);
                } else {
                    builder.push(false);
                }
            },
        ))
        .register();

    registry
        .scalar_builder("h3_get_resolution")
        .function()
        .typed_1_arg::<UInt64Type, UInt8Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<UInt64Type, UInt8Type>(
            |h3, builder, ctx| match CellIndex::try_from(h3) {
                Ok(index) => builder.push(index.resolution().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0);
                }
            },
        ))
        .register();

    registry
        .scalar_builder("h3_edge_length_m")
        .function()
        .typed_1_arg::<UInt8Type, Float64Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<UInt8Type, Float64Type>(
            |r, builder, ctx| match Resolution::try_from(r) {
                Ok(rr) => builder.push(rr.edge_length_m().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0.0.into());
                }
            },
        ))
        .register();

    registry
        .scalar_builder("h3_edge_length_km")
        .function()
        .typed_1_arg::<UInt8Type, Float64Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<UInt8Type, Float64Type>(
            |r, builder, ctx| match Resolution::try_from(r) {
                Ok(rr) => builder.push(rr.edge_length_km().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0.0.into());
                }
            },
        ))
        .register();

    registry
        .scalar_builder("h3_get_base_cell")
        .function()
        .typed_1_arg::<UInt64Type, UInt8Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<UInt64Type, UInt8Type>(
            |h3, builder, ctx| match CellIndex::try_from(h3) {
                Ok(index) => builder.push(index.base_cell().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0);
                }
            },
        ))
        .register();

    registry
        .scalar_builder("h3_hex_area_m2")
        .function()
        .typed_1_arg::<UInt8Type, Float64Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<UInt8Type, Float64Type>(
            |r, builder, ctx| match Resolution::try_from(r) {
                Ok(rr) => builder.push(rr.area_m2().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0.0.into());
                }
            },
        ))
        .register();

    registry
        .scalar_builder("h3_hex_area_km2")
        .function()
        .typed_1_arg::<UInt8Type, Float64Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<UInt8Type, Float64Type>(
            |r, builder, ctx| match Resolution::try_from(r) {
                Ok(rr) => builder.push(rr.area_km2().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0.0.into());
                }
            },
        ))
        .register();

    registry.register_passthrough_nullable_2_arg::<UInt64Type, UInt64Type, BooleanType, _, _>(
        "h3_indexes_are_neighbors",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<UInt64Type, UInt64Type, BooleanType>(
            |h3, a_h3, builder, ctx| match CellIndex::try_from(h3)
                .map_err(|e| e.to_string())
                .and_then(|index| {
                    CellIndex::try_from(a_h3)
                        .map_err(|e| e.to_string())
                        .map(|a_index| index.is_neighbor_with(a_index).unwrap_or(false))
                }) {
                Ok(b) => builder.push(b),
                Err(e) => {
                    ctx.set_error(builder.len(), e);
                    builder.push(false);
                }
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<UInt64Type, UInt8Type, ArrayType<UInt64Type>, _, _>(
            "h3_to_children",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<UInt64Type, UInt8Type, ArrayType<UInt64Type>>(
                |h3, r, builder, ctx| {
                    match CellIndex::try_from(h3)
                        .map_err(|e| e.to_string())
                        .and_then(|index| {
                            Resolution::try_from(r)
                                .map_err(|e| e.to_string())
                                .map(|rr| index.children(rr))
                        }) {
                        Ok(index_iter) => {
                            for child in index_iter {
                                builder.put_item(child.into());
                            }
                        }
                        Err(err) => {
                            ctx.set_error(builder.len(), err);
                            builder.put_item(0);
                        }
                    }
                    builder.commit_row();
                },
            ),
        );

    registry.register_passthrough_nullable_2_arg::<UInt64Type, UInt8Type, UInt64Type, _, _>(
        "h3_to_parent",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<UInt64Type, UInt8Type, UInt64Type>(|h3, r, builder, ctx| {
            match CellIndex::try_from(h3)
                .map_err(|e| e.to_string())
                .and_then(|index| {
                    Resolution::try_from(r)
                        .map_err(|e| e.to_string())
                        .map(|rr| index.parent(rr))
                }) {
                Ok(parent) => {
                    if let Some(p) = parent {
                        builder.push(p.into());
                    } else {
                        builder.push(0);
                    }
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0);
                }
            }
        }),
    );

    registry
        .scalar_builder("h3_to_string")
        .function()
        .typed_1_arg::<UInt64Type, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<UInt64Type, StringType>(
            |h3, builder, ctx| match CellIndex::try_from(h3) {
                Ok(index) => builder.put_and_commit(index.to_string()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.put_and_commit("");
                }
            },
        ))
        .register();

    registry
        .scalar_builder("string_to_h3")
        .function()
        .typed_1_arg::<StringType, UInt64Type>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<StringType, UInt64Type>(
            |h3_str, builder, ctx| match h3_str.parse::<CellIndex>() {
                Ok(index) => builder.push(index.into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0);
                }
            },
        ))
        .register();

    registry
        .scalar_builder("h3_is_res_class_iii")
        .function()
        .typed_1_arg::<UInt64Type, BooleanType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<UInt64Type, BooleanType>(
            |h3, builder, ctx| match CellIndex::try_from(h3) {
                Ok(index) => builder.push(index.resolution().is_class3()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(false);
                }
            },
        ))
        .register();

    registry
        .scalar_builder("h3_is_pentagon")
        .function()
        .typed_1_arg::<UInt64Type, BooleanType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<UInt64Type, BooleanType>(
            |h3, builder, ctx| match CellIndex::try_from(h3) {
                Ok(index) => builder.push(index.is_pentagon()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(false);
                }
            },
        ))
        .register();

    registry
        .scalar_builder("h3_get_faces")
        .function()
        .typed_1_arg::<UInt64Type, ArrayType<UInt8Type>>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<
            UInt64Type,
            ArrayType<UInt8Type>,
        >(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(index) => {
                    for f in index.icosahedron_faces().iter() {
                        builder.put_item(f.into());
                    }
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.put_item(0);
                }
            }
            builder.commit_row();
        }))
        .register();

    registry.register_passthrough_nullable_1_arg::<UInt64Type, Float64Type, _>(
        "h3_cell_area_m2",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, Float64Type>(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(index) => builder.push(index.area_m2().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0.0.into());
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, Float64Type, _>(
        "h3_cell_area_rads2",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, Float64Type>(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(index) => builder.push(index.area_rads2().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0.0.into());
                }
            }
        }),
    );

    registry.register_passthrough_nullable_2_arg::<UInt64Type, UInt8Type, UInt64Type, _, _>(
        "h3_to_center_child",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<UInt64Type, UInt8Type, UInt64Type>(|h3, r, builder, ctx| {
            match CellIndex::try_from(h3)
                .map_err(|e| e.to_string())
                .and_then(|index| {
                    Resolution::try_from(r)
                        .map_err(|e| e.to_string())
                        .map(|rr| index.center_child(rr))
                }) {
                Ok(child) => {
                    if let Some(p) = child {
                        builder.push(p.into());
                    } else {
                        builder.push(0);
                    }
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0);
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, Float64Type, _>(
        "h3_exact_edge_length_m",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, Float64Type>(|h3, builder, ctx| {
            match DirectedEdgeIndex::try_from(h3) {
                Ok(index) => builder.push(index.length_m().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0.0.into());
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, Float64Type, _>(
        "h3_exact_edge_length_km",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, Float64Type>(|h3, builder, ctx| {
            match DirectedEdgeIndex::try_from(h3) {
                Ok(index) => builder.push(index.length_km().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0.0.into());
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, Float64Type, _>(
        "h3_exact_edge_length_rads",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, Float64Type>(|h3, builder, ctx| {
            match DirectedEdgeIndex::try_from(h3) {
                Ok(index) => builder.push(index.length_rads().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0.0.into());
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt8Type, UInt64Type, _>(
        "h3_num_hexagons",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt8Type, UInt64Type>(|r, builder, ctx| {
            match Resolution::try_from(r) {
                Ok(rr) => builder.push(rr.cell_count()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0);
                }
            }
        }),
    );

    registry
        .register_passthrough_nullable_2_arg::<UInt64Type, UInt64Type, ArrayType<UInt64Type>, _, _>(
            "h3_line",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<UInt64Type, UInt64Type, ArrayType<UInt64Type>>(
                |h3, a_h3, builder, ctx| {
                    match CellIndex::try_from(h3)
                        .map_err(|e| e.to_string())
                        .and_then(|index| {
                            CellIndex::try_from(a_h3)
                                .map_err(|e| e.to_string())
                                .map(|a_index| {
                                    index.grid_path_cells(a_index).map_err(|e| e.to_string())
                                })
                        }) {
                        Ok(Ok(index_iter)) => {
                            for index in index_iter {
                                match index {
                                    Ok(i) => builder.put_item(i.into()),
                                    Err(e) => {
                                        ctx.set_error(builder.len(), e);
                                        builder.put_item(0);
                                        break;
                                    }
                                }
                            }
                        }
                        Ok(Err(err)) | Err(err) => {
                            ctx.set_error(builder.len(), err);
                            builder.put_item(0);
                        }
                    }
                    builder.commit_row();
                },
            ),
        );

    registry.register_passthrough_nullable_2_arg::<UInt64Type, UInt64Type, Int32Type, _, _>(
        "h3_distance",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<UInt64Type, UInt64Type, Int32Type>(
            |h3, a_h3, builder, ctx| match CellIndex::try_from(h3)
                .map_err(|e| e.to_string())
                .and_then(|index| {
                    CellIndex::try_from(a_h3)
                        .map_err(|e| e.to_string())
                        .map(|a_index| index.grid_distance(a_index).map_err(|e| e.to_string()))
                }) {
                Ok(Ok(dist)) => builder.push(dist),
                Ok(Err(err)) | Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0);
                }
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<UInt64Type, UInt32Type, ArrayType<UInt64Type>, _, _>(
            "h3_hex_ring",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<UInt64Type, UInt32Type, ArrayType<UInt64Type>>(
                |h3, k, builder, ctx| {
                    match CellIndex::try_from(h3)
                        .map_err(|e| e.to_string())
                        .map(|index| index.grid_ring_fast(k))
                    {
                        Ok(index_iter) => {
                            for index in index_iter {
                                if index.is_none() {
                                    // If index is none, it means that a pentagon (or a pentagon distortion) is encountered.
                                    // When this happen, the previously returned cells should be treated as invalid and discarded.
                                    // Ref: https://docs.rs/h3o/0.3.5/h3o/struct.CellIndex.html#method.grid_ring_fast
                                    ctx.set_error(
                                        builder.len(),
                                        "a pentagon (or a pentagon distortion) is encountered",
                                    );
                                    builder.put_item(0);
                                    break;
                                }
                                builder.put_item(index.unwrap().into());
                            }
                        }
                        Err(err) => {
                            ctx.set_error(builder.len(), err);
                            builder.put_item(0);
                        }
                    }
                    builder.commit_row();
                },
            ),
        );

    registry.register_passthrough_nullable_2_arg::<UInt64Type, UInt64Type, UInt64Type, _, _>(
        "h3_get_unidirectional_edge",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<UInt64Type, UInt64Type, UInt64Type>(
            |h3, a_h3, builder, ctx| match CellIndex::try_from(h3)
                .map_err(|e| e.to_string())
                .and_then(|index| {
                    CellIndex::try_from(a_h3)
                        .map_err(|e| e.to_string())
                        .map(|a_index| index.edge(a_index))
                }) {
                Ok(edge) => match edge {
                    Some(e) => builder.push(e.into()),
                    None => builder.push(0),
                },
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0);
                }
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, BooleanType, _>(
        "h3_unidirectional_edge_is_valid",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, BooleanType>(|h3, builder, _| {
            if DirectedEdgeIndex::try_from(h3).is_ok() {
                builder.push(true);
            } else {
                builder.push(false);
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, UInt64Type, _>(
        "h3_get_origin_index_from_unidirectional_edge",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, UInt64Type>(|h3, builder, ctx| {
            match DirectedEdgeIndex::try_from(h3) {
                Ok(edge) => builder.push(edge.origin().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(0u64);
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, UInt64Type, _>(
        "h3_get_destination_index_from_unidirectional_edge",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, UInt64Type>(|h3, builder, ctx| {
            match DirectedEdgeIndex::try_from(h3) {
                Ok(edge) => builder.push(edge.destination().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(0u64);
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, KvPair<UInt64Type, UInt64Type>, _>(
        "h3_get_indexes_from_unidirectional_edge",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, KvPair<UInt64Type, UInt64Type>>(
            |h3, builder, ctx| match DirectedEdgeIndex::try_from(h3) {
                Ok(edge) => {
                    let (origin, dest) = edge.cells();
                    builder.push((origin.into(), dest.into()));
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push((0u64, 0u64));
                }
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, ArrayType<UInt64Type>, _>(
        "h3_get_unidirectional_edges_from_hexagon",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, ArrayType<UInt64Type>>(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(index) => {
                    for e in index.edges() {
                        builder.put_item(e.into());
                    }
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.put_item(0u64);
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, ArrayType<KvPair<Float64Type, Float64Type>>, _>(
        "h3_get_unidirectional_edge_boundary",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, ArrayType<KvPair<Float64Type, Float64Type>>>(|h3, builder, ctx| {
            match DirectedEdgeIndex::try_from(h3) {
                Ok(index) => {
                    for coord in index.boundary().iter() {
                        builder.put_item((coord.lat().into(), coord.lng().into()));
                    }
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.put_item((0.0.into(), 0.0.into()));
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt8Type, Float64Type, _>(
        "h3_edge_angle",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt8Type, Float64Type>(|r, builder, ctx| {
            match Resolution::try_from(r) {
                Ok(rr) => {
                    // Numerical constant is 180 degrees / pi / Earth radius, Earth radius is from h3 sources
                    // Ref: https://github.com/ClickHouse/ClickHouse/blob/bc93254ca23835b9846ec1f9158a50f26694257f/src/Functions/h3EdgeAngle.cpp#L86
                    builder.push((8.99320592271288e-6 * rr.edge_length_m()).into())
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(0.0.into());
                }
            }
        }),
    );
}
