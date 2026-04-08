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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::GeographyType;
use databend_common_expression::types::GeometryType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NullableType;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_io::ewkb_to_geo;
use geo::Geometry;
use geozero::ToGeo;
use geozero::wkb::Ewkb;

pub(crate) fn geometry_unary_fn<O: ArgType>(
    name: &str,
    registry: &mut FunctionRegistry,
    op: fn(Geometry, Option<i32>) -> Result<O::Scalar>,
) {
    registry.register_passthrough_nullable_1_arg::<GeometryType, O, _>(
        name,
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, O>(move |ewkb, builder, ctx| {
            let row = O::builder_len(builder);
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(row) {
                    O::push_default(builder);
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, srid)| op(geo, srid)) {
                Ok(value) => {
                    O::push_item(builder, O::to_scalar_ref(&value));
                }
                Err(e) => {
                    ctx.set_error(row, e.to_string());
                    O::push_default(builder);
                }
            }
        }),
    );
}

pub(crate) fn geometry_unary_combine_fn<O: ArgType>(
    name: &str,
    registry: &mut FunctionRegistry,
    op: fn(Geometry, Option<i32>) -> Result<Option<O::Scalar>>,
) {
    registry.register_combine_nullable_1_arg::<GeometryType, O, _, _>(
        name,
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<O>>(move |ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push_null();
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, srid)| op(geo, srid)) {
                Ok(Some(value)) => builder.push(O::to_scalar_ref(&value)),
                Ok(None) => builder.push_null(),
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    builder.push_null();
                }
            }
        }),
    );
}

pub(crate) fn geometry_binary_fn<O: ArgType>(
    name: &str,
    registry: &mut FunctionRegistry,
    op: fn(Geometry, Geometry, Option<i32>) -> Result<O::Scalar>,
) {
    registry.register_passthrough_nullable_2_arg::<GeometryType, GeometryType, O, _, _>(
        name,
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, O>(
            move |l_ewkb, r_ewkb, builder, ctx| {
                let row = O::builder_len(builder);
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(row) {
                        O::push_default(builder);
                        return;
                    }
                }

                let result = match (
                    ewkb_to_geo(&mut Ewkb(l_ewkb)),
                    ewkb_to_geo(&mut Ewkb(r_ewkb)),
                ) {
                    (Ok((l_geo, l_srid)), Ok((r_geo, r_srid))) => {
                        if !check_incompatible_srid(l_srid, r_srid, row, ctx) {
                            O::push_default(builder);
                            return;
                        }
                        op(l_geo, r_geo, l_srid)
                    }
                    (Err(e), _) | (_, Err(e)) => Err(e),
                };

                match result {
                    Ok(result) => {
                        O::push_item(builder, O::to_scalar_ref(&result));
                    }
                    Err(e) => {
                        ctx.set_error(row, e.to_string());
                        O::push_default(builder);
                    }
                }
            },
        ),
    );
}

pub(crate) fn geometry_binary_combine_fn<O: ArgType>(
    name: &str,
    registry: &mut FunctionRegistry,
    op: fn(Geometry, Geometry, Option<i32>) -> Result<Option<O::Scalar>>,
) {
    registry.register_combine_nullable_2_arg::<GeometryType, GeometryType, O, _, _>(
        name,
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, NullableType<O>>(
            move |l_ewkb, r_ewkb, builder, ctx| {
                let row = builder.len();
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(row) {
                        builder.push_null();
                        return;
                    }
                }

                let result = match (
                    ewkb_to_geo(&mut Ewkb(l_ewkb)),
                    ewkb_to_geo(&mut Ewkb(r_ewkb)),
                ) {
                    (Ok((l_geo, l_srid)), Ok((r_geo, r_srid))) => {
                        if !check_incompatible_srid(l_srid, r_srid, row, ctx) {
                            builder.push_null();
                            return;
                        }
                        op(l_geo, r_geo, l_srid)
                    }
                    (Err(e), _) | (_, Err(e)) => Err(e),
                };

                match result {
                    Ok(Some(value)) => {
                        builder.push(O::to_scalar_ref(&value));
                    }
                    Ok(None) => {
                        builder.push_null();
                    }
                    Err(e) => {
                        ctx.set_error(row, e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );
}

pub(crate) fn geography_unary_fn<O: ArgType>(
    name: &str,
    registry: &mut FunctionRegistry,
    op: fn(Geometry) -> Result<O::Scalar>,
) {
    registry.register_passthrough_nullable_1_arg::<GeographyType, O, _>(
        name,
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, O>(move |ewkb, builder, ctx| {
            let row = O::builder_len(builder);
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(row) {
                    O::push_default(builder);
                    return;
                }
            }

            let result = Ewkb(ewkb)
                .to_geo()
                .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                .and_then(op);
            match result {
                Ok(value) => {
                    O::push_item(builder, O::to_scalar_ref(&value));
                }
                Err(e) => {
                    ctx.set_error(row, e.to_string());
                    O::push_default(builder);
                }
            }
        }),
    );
}

pub(crate) fn geography_unary_combine_fn<O: ArgType>(
    name: &str,
    registry: &mut FunctionRegistry,
    op: fn(Geometry) -> Result<Option<O::Scalar>>,
) {
    registry.register_combine_nullable_1_arg::<GeographyType, O, _, _>(
        name,
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, NullableType<O>>(
            move |ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                let result = Ewkb(ewkb)
                    .to_geo()
                    .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                    .and_then(op);
                match result {
                    Ok(Some(value)) => builder.push(O::to_scalar_ref(&value)),
                    Ok(None) => builder.push_null(),
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );
}

pub(crate) fn geography_binary_fn<O: ArgType>(
    name: &str,
    registry: &mut FunctionRegistry,
    op: fn(Geometry, Geometry) -> Result<O::Scalar>,
) {
    registry.register_passthrough_nullable_2_arg::<GeographyType, GeographyType, O, _, _>(
        name,
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeographyType, GeographyType, O>(
            move |l_ewkb, r_ewkb, builder, ctx| {
                let row = O::builder_len(builder);
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(row) {
                        O::push_default(builder);
                        return;
                    }
                }

                let result = Ewkb(l_ewkb)
                    .to_geo()
                    .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                    .and_then(|l_geo| {
                        Ewkb(r_ewkb)
                            .to_geo()
                            .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                            .and_then(|r_geo| op(l_geo, r_geo))
                    });

                match result {
                    Ok(result) => {
                        O::push_item(builder, O::to_scalar_ref(&result));
                    }
                    Err(e) => {
                        ctx.set_error(row, e.to_string());
                        O::push_default(builder);
                    }
                }
            },
        ),
    );
}

pub(crate) fn geo_convert_fn<I: ArgType, O: ArgType>(
    name: &str,
    registry: &mut FunctionRegistry,
    op: fn(I::ScalarRef<'_>) -> Result<O::Scalar>,
) {
    registry.register_passthrough_nullable_1_arg::<I, O, _>(
        name,
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<I, O>(move |input, builder, ctx| {
            let row = O::builder_len(builder);
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(row) {
                    O::push_default(builder);
                    return;
                }
            }

            match op(input) {
                Ok(value) => {
                    O::push_item(builder, O::to_scalar_ref(&value));
                }
                Err(e) => {
                    ctx.set_error(row, e.to_string());
                    O::push_default(builder);
                }
            }
        }),
    );
}

pub(crate) fn geo_convert_with_arg_fn<I: ArgType, O: ArgType>(
    name: &str,
    registry: &mut FunctionRegistry,
    op: fn(I::ScalarRef<'_>, i32) -> Result<O::Scalar>,
) {
    registry.register_passthrough_nullable_2_arg::<I, Int32Type, O, _, _>(
        name,
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<I, Int32Type, O>(move |input, srid, builder, ctx| {
            let row = O::builder_len(builder);
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(row) {
                    O::push_default(builder);
                    return;
                }
            }

            match op(input, srid) {
                Ok(value) => {
                    O::push_item(builder, O::to_scalar_ref(&value));
                }
                Err(e) => {
                    ctx.set_error(row, e.to_string());
                    O::push_default(builder);
                }
            }
        }),
    );
}

pub(crate) fn geo_try_convert_fn<I: ArgType, O: ArgType>(
    name: &str,
    registry: &mut FunctionRegistry,
    op: fn(I::ScalarRef<'_>) -> Result<O::Scalar>,
) {
    registry.register_combine_nullable_1_arg::<I, O, _, _>(
        name,
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<I, NullableType<O>>(move |input, builder, ctx| {
            let row = builder.len();
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(row) {
                    builder.push_null();
                    return;
                }
            }

            match op(input) {
                Ok(value) => {
                    builder.push(O::to_scalar_ref(&value));
                }
                Err(_) => {
                    builder.push_null();
                }
            }
        }),
    );
}

pub(crate) fn geo_try_convert_with_arg_fn<I: ArgType, O: ArgType>(
    name: &str,
    registry: &mut FunctionRegistry,
    op: fn(I::ScalarRef<'_>, i32) -> Result<O::Scalar>,
) {
    registry.register_combine_nullable_2_arg::<I, Int32Type, O, _, _>(
        name,
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<I, Int32Type, NullableType<O>>(
            move |input, srid, builder, ctx| {
                let row = builder.len();
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(row) {
                        builder.push_null();
                        return;
                    }
                }

                match op(input, srid) {
                    Ok(value) => {
                        builder.push(O::to_scalar_ref(&value));
                    }
                    Err(_) => {
                        builder.push_null();
                    }
                }
            },
        ),
    );
}

fn check_incompatible_srid(
    l_srid: Option<i32>,
    r_srid: Option<i32>,
    len: usize,
    ctx: &mut EvalContext,
) -> bool {
    let l_srid = l_srid.unwrap_or_default();
    let r_srid = r_srid.unwrap_or_default();
    if !l_srid.eq(&r_srid) {
        ctx.set_error(len, format!("Incompatible SRID: {} and {}", l_srid, r_srid));
        false
    } else {
        true
    }
}
