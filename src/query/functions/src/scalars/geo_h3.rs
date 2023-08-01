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

use common_expression::types::map::KvPair;
use common_expression::types::ArrayType;
use common_expression::types::BooleanType;
use common_expression::types::Float64Type;
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
}
