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

use std::str;

use common_expression::types::map::KvPair;
use common_expression::types::ArrayType;
use common_expression::types::BooleanType;
use common_expression::types::Float64Type;
use common_expression::types::StringType;
use common_expression::types::UInt32Type;
use common_expression::types::UInt64Type;
use common_expression::types::UInt8Type;
use common_expression::types::F64;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::vectorize_with_builder_2_arg;
use common_expression::FunctionDomain;
use common_expression::FunctionRegistry;
use h3o::CellIndex;
use h3o::LatLng;
use h3o::Resolution;

pub fn register(registry: &mut FunctionRegistry) {
    registry
        .register_passthrough_nullable_1_arg::<UInt64Type, KvPair<Float64Type, Float64Type>, _, _>(
            "h3_to_geo",
            |_, _| FunctionDomain::Full,
            vectorize_with_builder_1_arg::<UInt64Type, KvPair<Float64Type, Float64Type>>(
                |h3, builder, ctx| match CellIndex::try_from(h3) {
                    Ok(h3_cell) => {
                        let coord: LatLng = h3_cell.into();
                        builder.push((coord.lng().into(), coord.lat().into()));
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push((F64::from(0.0), F64::from(0.0)))
                    }
                },
            ),
        );

    registry
        .register_passthrough_nullable_1_arg::<UInt64Type, ArrayType<KvPair<Float64Type, Float64Type>>, _, _>(
            "h3_to_geo_boundary",
            |_, _| FunctionDomain::Full,
            vectorize_with_builder_1_arg::<UInt64Type, ArrayType<KvPair<Float64Type, Float64Type>>>(
                |h3, builder, ctx| {
                    match CellIndex::try_from(h3) {
                        Ok(h3_cell) => {
                            let boundary = h3_cell.boundary();
                            let coord_list = boundary.iter().collect::<Vec<_>>();
                            for coord in coord_list {
                                builder.put_item((coord.lng().into(), coord.lat().into()));
                            }
                        }
                        Err(e) => {
                            ctx.set_error(builder.len(), e.to_string());
                            builder.put_item((F64::from(0.0), F64::from(0.0)));
                        }
                    }
                    builder.commit_row();
                },
            ),
        );

    registry
        .register_passthrough_nullable_2_arg::<UInt64Type, UInt32Type, ArrayType<UInt64Type>, _, _>(
            "h3_k_ring",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<UInt64Type, UInt32Type, ArrayType<UInt64Type>>(
                |h3, k, builder, ctx| {
                    match CellIndex::try_from(h3) {
                        Ok(h3_cell) => {
                            let ring = h3_cell.grid_ring_fast(k);
                            for item in ring.flatten() {
                                builder.put_item(item.into());
                            }
                        }
                        Err(e) => {
                            ctx.set_error(builder.len(), e.to_string());
                            builder.put_item(0);
                        }
                    }
                    builder.commit_row();
                },
            ),
        );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, BooleanType, _, _>(
        "h3_is_valid",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, BooleanType>(|h3, builder, _| {
            if CellIndex::try_from(h3).is_ok() {
                builder.push(true);
            } else {
                builder.push(false);
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, UInt8Type, _, _>(
        "h3_get_resolution",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, UInt8Type>(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(index) => builder.push(index.resolution().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(0);
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt8Type, Float64Type, _, _>(
        "h3_edge_length_m",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt8Type, Float64Type>(|r, builder, ctx| {
            match Resolution::try_from(r) {
                Ok(rr) => builder.push(rr.edge_length_m().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(0.0.into());
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt8Type, Float64Type, _, _>(
        "h3_edge_length_km",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt8Type, Float64Type>(|r, builder, ctx| {
            match Resolution::try_from(r) {
                Ok(rr) => builder.push(rr.edge_length_km().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(0.0.into());
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, UInt8Type, _, _>(
        "h3_get_base_cell",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, UInt8Type>(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(index) => builder.push(index.base_cell().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(0);
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt8Type, Float64Type, _, _>(
        "h3_hex_area_m2",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt8Type, Float64Type>(|r, builder, ctx| {
            match Resolution::try_from(r) {
                Ok(rr) => builder.push(rr.area_m2().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(0.0.into());
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt8Type, Float64Type, _, _>(
        "h3_hex_area_km2",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt8Type, Float64Type>(|r, builder, ctx| {
            match Resolution::try_from(r) {
                Ok(rr) => builder.push(rr.area_km2().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(0.0.into());
                }
            }
        }),
    );

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

    registry.register_passthrough_nullable_1_arg::<UInt64Type, StringType, _, _>(
        "h3_to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, StringType>(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(index) => builder.put_str(&index.to_string()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.put_str("");
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, UInt64Type, _, _>(
        "string_to_h3",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, UInt64Type>(|h3_str, builder, ctx| {
            match str::from_utf8(h3_str)
                .map_err(|e| e.to_string())
                .and_then(|h3_str| str::parse::<CellIndex>(h3_str).map_err(|e| e.to_string()))
            {
                Ok(index) => builder.push(index.into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err);
                    builder.push(0);
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, BooleanType, _, _>(
        "h3_is_res_class_iii",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, BooleanType>(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(index) => builder.push(index.resolution().is_class3()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(false);
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, BooleanType, _, _>(
        "h3_is_pentagon",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, BooleanType>(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(index) => builder.push(index.is_pentagon()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(false);
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, ArrayType<UInt8Type>, _, _>(
        "h3_get_faces",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, ArrayType<UInt8Type>>(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(index) => {
                    for f in index.icosahedron_faces().iter() {
                        builder.put_item(f.into());
                    }
                }
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.put_item(0);
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, Float64Type, _, _>(
        "h3_cell_area_m2",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, Float64Type>(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(index) => builder.push(index.area_m2().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(0.0.into());
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<UInt64Type, Float64Type, _, _>(
        "h3_cell_area_rads2",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<UInt64Type, Float64Type>(|h3, builder, ctx| {
            match CellIndex::try_from(h3) {
                Ok(index) => builder.push(index.area_rads2().into()),
                Err(err) => {
                    ctx.set_error(builder.len(), err.to_string());
                    builder.push(0.0.into());
                }
            }
        }),
    );
}
