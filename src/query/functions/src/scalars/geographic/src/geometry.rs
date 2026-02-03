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
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::F64;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::geometry::GeometryType;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_expression::vectorize_with_builder_4_arg;
use databend_common_io::Axis;
use databend_common_io::Extremum;
use databend_common_io::ewkb_to_geo;
use databend_common_io::geo_to_ewkb;
use databend_common_io::geo_to_ewkt;
use databend_common_io::geo_to_json;
use databend_common_io::geo_to_wkb;
use databend_common_io::geo_to_wkt;
use databend_common_io::geometry::count_points;
use databend_common_io::geometry::geometry_from_str;
use databend_common_io::geometry::point_to_geohash;
use databend_common_io::geometry::st_extreme;
use databend_common_io::geometry_format;
use databend_common_io::geometry_from_ewkt;
use databend_common_io::geometry_type_name;
use databend_common_io::read_srid;
use geo::Area;
use geo::Contains;
use geo::ConvexHull;
use geo::Coord;
use geo::Distance;
use geo::Euclidean;
use geo::Geometry;
use geo::HasDimensions;
use geo::Haversine;
use geo::Intersects;
use geo::Length;
use geo::Line;
use geo::LineString;
use geo::MultiLineString;
use geo::MultiPolygon;
use geo::Point;
use geo::Polygon;
use geo::Rect;
use geo::ToDegrees;
use geo::ToRadians;
use geo::Triangle;
use geo::Within;
use geo::coord;
use geo::dimensions::Dimensions;
use geohash::decode_bbox;
use geozero::CoordDimensions;
use geozero::GeozeroGeometry;
use geozero::ToGeo;
use geozero::ToWkb;
use geozero::geojson::GeoJson;
use geozero::wkb::Ewkb;
use jsonb::RawJsonb;
use jsonb::parse_owned_jsonb_with_buf;
use num_traits::AsPrimitive;
use proj4rs::Proj;
use proj4rs::transform::transform;

pub fn register(registry: &mut FunctionRegistry) {
    // aliases
    registry.register_aliases("st_aswkb", &["st_asbinary"]);
    registry.register_aliases("st_aswkt", &["st_astext"]);
    registry.register_aliases("st_makegeompoint", &["st_geom_point"]);
    registry.register_aliases("st_makepolygon", &["st_polygon"]);
    registry.register_aliases("st_makeline", &["st_make_line"]);
    registry.register_aliases("st_npoints", &["st_numpoints"]);
    registry.register_aliases("st_geometryfromwkb", &[
        "st_geomfromwkb",
        "st_geometryfromewkb",
        "st_geomfromewkb",
    ]);
    registry.register_aliases("st_geometryfromwkt", &[
        "st_geomfromwkt",
        "st_geometryfromewkt",
        "st_geomfromewkt",
        "st_geometryfromtext",
        "st_geomfromtext",
    ]);

    // functions
    registry.register_passthrough_nullable_4_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>, _, _>(
        "haversine",
        |_, _, _, _, _| FunctionDomain::Full,
        vectorize_with_builder_4_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>,>(|lat1, lon1, lat2, lon2, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push(0_f64.into());
                    return;
                }
            }
            let p1 = Point::new(lon1, lat1);
            let p2 = Point::new(lon2, lat2);
            let distance = Haversine.distance(p1, p2) * 0.001;
            let distance = (distance * 1_000_000_000_f64).round() / 1_000_000_000_f64;
            builder.push(distance.into());
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, VariantType, _>(
        "st_asgeojson",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, VariantType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, _)| geo_to_json(geo)) {
                Ok(json) => {
                    if let Err(e) = parse_owned_jsonb_with_buf(json.as_bytes(), &mut builder.data) {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, BinaryType, _>(
        "st_asewkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, BinaryType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, srid)| geo_to_ewkb(geo, srid)) {
                Ok(ewkb) => {
                    builder.put_slice(ewkb.as_slice());
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, BinaryType, _>(
        "st_aswkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, BinaryType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, _)| geo_to_wkb(geo)) {
                Ok(wkb) => {
                    builder.put_slice(wkb.as_slice());
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _>(
        "st_asewkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, srid)| geo_to_ewkt(geo, srid)) {
                Ok(ewkt) => {
                    builder.put_str(&ewkt);
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _>(
        "st_aswkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, _)| geo_to_wkt(geo)) {
                Ok(wkt) => {
                    builder.put_str(&wkt);
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, GeometryType, BooleanType, _, _>(
        "st_contains",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, BooleanType>(
            |l_ewkb, r_ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push(false);
                        return;
                    }
                }

                match (
                    ewkb_to_geo(&mut Ewkb(l_ewkb)),
                    ewkb_to_geo(&mut Ewkb(r_ewkb)),
                ) {
                    (Ok((l_geo, l_srid)), Ok((r_geo, r_srid))) => {
                        if !check_incompatible_srid(l_srid, r_srid, builder.len(), ctx) {
                            builder.push(false);
                            return;
                        }
                        let is_contains = l_geo.contains(&r_geo);
                        builder.push(is_contains);
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(false);
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, GeometryType, BooleanType, _, _>(
        "st_intersects",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, BooleanType>(
            |l_ewkb, r_ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push(false);
                        return;
                    }
                }

                match (
                    ewkb_to_geo(&mut Ewkb(l_ewkb)),
                    ewkb_to_geo(&mut Ewkb(r_ewkb)),
                ) {
                    (Ok((l_geo, l_srid)), Ok((r_geo, r_srid))) => {
                        if !check_incompatible_srid(l_srid, r_srid, builder.len(), ctx) {
                            builder.push(false);
                            return;
                        }
                        let is_intersects = l_geo.intersects(&r_geo);
                        builder.push(is_intersects);
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(false);
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, GeometryType, BooleanType, _, _>(
        "st_disjoint",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, BooleanType>(
            |l_ewkb, r_ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push(false);
                        return;
                    }
                }

                match (
                    ewkb_to_geo(&mut Ewkb(l_ewkb)),
                    ewkb_to_geo(&mut Ewkb(r_ewkb)),
                ) {
                    (Ok((l_geo, l_srid)), Ok((r_geo, r_srid))) => {
                        if !check_incompatible_srid(l_srid, r_srid, builder.len(), ctx) {
                            builder.push(false);
                            return;
                        }
                        let is_disjoint = !l_geo.intersects(&r_geo);
                        builder.push(is_disjoint);
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(false);
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, GeometryType, BooleanType, _, _>(
        "st_within",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, BooleanType>(
            |l_ewkb, r_ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push(false);
                        return;
                    }
                }

                match (
                    ewkb_to_geo(&mut Ewkb(l_ewkb)),
                    ewkb_to_geo(&mut Ewkb(r_ewkb)),
                ) {
                    (Ok((l_geo, l_srid)), Ok((r_geo, r_srid))) => {
                        if !check_incompatible_srid(l_srid, r_srid, builder.len(), ctx) {
                            builder.push(false);
                            return;
                        }
                        let is_within = l_geo.is_within(&r_geo);
                        builder.push(is_within);
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(false);
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, GeometryType, BooleanType, _, _>(
        "st_equals",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, BooleanType>(
            |l_ewkb, r_ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push(false);
                        return;
                    }
                }

                match (
                    ewkb_to_geo(&mut Ewkb(l_ewkb)),
                    ewkb_to_geo(&mut Ewkb(r_ewkb)),
                ) {
                    (Ok((l_geo, l_srid)), Ok((r_geo, r_srid))) => {
                        if !check_incompatible_srid(l_srid, r_srid, builder.len(), ctx) {
                            builder.push(false);
                            return;
                        }
                        let is_equal = l_geo.is_within(&r_geo) && r_geo.is_within(&l_geo);
                        builder.push(is_equal);
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push(false);
                    }
                }
            },
        ),
    );

    registry
        .register_passthrough_nullable_2_arg::<GeometryType, GeometryType, NumberType<F64>, _, _>(
            "st_distance",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<GeometryType, GeometryType, NumberType<F64>>(
                |l_ewkb, r_ewkb, builder, ctx| {
                    if let Some(validity) = &ctx.validity {
                        if !validity.get_bit(builder.len()) {
                            builder.push(F64::from(0_f64));
                            return;
                        }
                    }

                    match (
                        ewkb_to_geo(&mut Ewkb(l_ewkb)),
                        ewkb_to_geo(&mut Ewkb(r_ewkb)),
                    ) {
                        (Ok((l_geo, l_srid)), Ok((r_geo, r_srid))) => {
                            if !check_incompatible_srid(l_srid, r_srid, builder.len(), ctx) {
                                builder.push(F64::from(0_f64));
                                return;
                            }
                            let distance = Euclidean.distance(&l_geo, &r_geo);
                            let distance =
                                (distance * 1_000_000_000_f64).round() / 1_000_000_000_f64;
                            builder.push(distance.into());
                        }
                        (Err(e), _) | (_, Err(e)) => {
                            ctx.set_error(builder.len(), e.to_string());
                            builder.push(F64::from(0_f64));
                        }
                    }
                },
            ),
        );

    registry.register_passthrough_nullable_1_arg::<GeometryType, NumberType<F64>, _>(
        "st_area",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NumberType<F64>>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push(F64::from(0_f64));
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(ewkb)) {
                Ok((geo, _)) => {
                    let area = geo.unsigned_area();
                    let area = (area * 1_000_000_000_f64).round() / 1_000_000_000_f64;
                    builder.push(area.into());
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    builder.push(F64::from(0_f64));
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, GeometryType, _>(
        "st_convexhull",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, GeometryType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, srid)| {
                let polygon = geo.convex_hull();
                geo_to_ewkb(Geometry::from(polygon), srid)
            }) {
                Ok(ewkb) => {
                    builder.put_slice(ewkb.as_slice());
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, GeometryType, _, _>(
        "st_endpoint",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<GeometryType>>(
            |ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, srid)| match geo {
                    Geometry::LineString(line_string) => {
                        let mut points = line_string.points();
                        if let Some(point) = points.next_back() {
                            geo_to_ewkb(Geometry::from(point), srid).map(Some)
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Err(ErrorCode::GeometryError(format!(
                        "Type {} is not supported as argument to ST_ENDPOINT",
                        geometry_type_name(&geo)
                    ))),
                }) {
                    Ok(Some(binary)) => {
                        builder.push(binary.as_slice());
                    }
                    Ok(None) => {
                        builder.push_null();
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, GeometryType, _, _>(
        "st_startpoint",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<GeometryType>>(
            |ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, srid)| match geo {
                    Geometry::LineString(line_string) => {
                        let mut points = line_string.points();
                        if let Some(point) = points.next() {
                            geo_to_ewkb(Geometry::from(point), srid).map(Some)
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Err(ErrorCode::GeometryError(format!(
                        "Type {} is not supported as argument to ST_STARTPOINT",
                        geometry_type_name(&geo)
                    ))),
                }) {
                    Ok(Some(binary)) => {
                        builder.push(binary.as_slice());
                    }
                    Ok(None) => {
                        builder.push_null();
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<GeometryType, Int32Type, GeometryType, _, _>(
        "st_pointn",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, Int32Type, NullableType<GeometryType>>(
            |ewkb, index, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, srid)| match geo {
                    Geometry::LineString(line_string) => {
                        let mut points = line_string.points();
                        let len = points.len() as i32;
                        let idx = if index < 0 { len + index } else { index - 1 };
                        if let Some(point) = points.nth(idx as usize) {
                            geo_to_ewkb(Geometry::from(point), srid).map(Some)
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Err(ErrorCode::GeometryError(format!(
                        "Type {} is not supported as argument to ST_POINTN",
                        geometry_type_name(&geo)
                    ))),
                }) {
                    Ok(Some(binary)) => {
                        builder.push(binary.as_slice());
                    }
                    Ok(None) => {
                        builder.push_null();
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, Int32Type, _, _>(
        "st_dimension",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<Int32Type>>(
            |ewkb, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }

                match Ewkb(ewkb).to_geo() {
                    Ok(geo) => {
                        let dimension: Option<i32> = match geo.dimensions() {
                            Dimensions::Empty => None,
                            Dimensions::ZeroDimensional => Some(0),
                            Dimensions::OneDimensional => Some(1),
                            Dimensions::TwoDimensional => Some(2),
                        };
                        match dimension {
                            Some(dimension) => output.push(dimension),
                            None => output.push_null(),
                        }
                    }
                    Err(e) => {
                        ctx.set_error(output.len(), e.to_string());
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _>(
        "st_geomfromgeohash",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|geohash, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            if geohash.len() > 12 {
                ctx.set_error(
                    builder.len(),
                    "Currently the precision only implement within 12 digits!".to_string(),
                );
                builder.commit_row();
                return;
            }

            match decode_bbox(geohash) {
                Ok(rect) => {
                    let geo: Geometry = rect.into();
                    match geo_to_ewkb(geo, None) {
                        Ok(binary) => builder.put_slice(binary.as_slice()),
                        Err(e) => ctx.set_error(builder.len(), e.to_string()),
                    }
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _>(
        "st_geompointfromgeohash",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|geohash, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            if geohash.len() > 12 {
                ctx.set_error(
                    builder.len(),
                    "Currently the precision only implement within 12 digits!".to_string(),
                );
                builder.commit_row();
                return;
            }

            match decode_bbox(geohash) {
                Ok(rect) => {
                    let geo: Geometry = Point::from(rect.center()).into();
                    match geo_to_ewkb(geo, None) {
                        Ok(binary) => builder.put_slice(binary.as_slice()),
                        Err(e) => ctx.set_error(builder.len(), e.to_string()),
                    }
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            };
            builder.commit_row();
        }),
    );

    registry
        .register_passthrough_nullable_2_arg::<NumberType<F64>, NumberType<F64>, GeometryType, _, _>(
            "st_makegeompoint",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<NumberType<F64>, NumberType<F64>, GeometryType>(
                |longitude, latitude, builder, ctx| {
                    if let Some(validity) = &ctx.validity {
                        if !validity.get_bit(builder.len()) {
                            builder.commit_row();
                            return;
                        }
                    }
                    let geo = Geometry::from(Point::new(longitude.0, latitude.0));
                    match geo_to_ewkb(geo, None) {
                        Ok(binary) => builder.put_slice(binary.as_slice()),
                        Err(e) => ctx.set_error(builder.len(), e.to_string()),
                    }
                    builder.commit_row();
                },
            ),
        );

    registry.register_passthrough_nullable_1_arg::<GeometryType, GeometryType, _>(
        "st_makepolygon",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, GeometryType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, srid)| match geo {
                Geometry::LineString(line_string) => {
                    let points = line_string.into_points();
                    if points.len() < 4 {
                        Err(ErrorCode::GeometryError(format!(
                            "Input lines must have at least 4 points, but got {}",
                            points.len()
                        )))
                    } else if points.last() != points.first() {
                        Err(ErrorCode::GeometryError(
                            "The first point and last point are not equal".to_string(),
                        ))
                    } else {
                        let polygon = Polygon::new(LineString::from(points), vec![]);
                        let geo: Geometry = polygon.into();
                        geo_to_ewkb(geo, srid)
                    }
                }
                _ => Err(ErrorCode::GeometryError(format!(
                    "Type {} is not supported as argument to ST_MAKEPOLYGON",
                    geometry_type_name(&geo)
                ))),
            }) {
                Ok(binary) => {
                    builder.put_slice(binary.as_slice());
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, GeometryType, GeometryType, _, _>(
        "st_makeline",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, GeometryType>(
            |l_ewkb, r_ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                match (ewkb_to_geo(&mut Ewkb(l_ewkb)), ewkb_to_geo(&mut Ewkb(r_ewkb))) {
                    (Ok((l_geo, l_srid)), Ok((r_geo, r_srid))) => {
                        if !check_incompatible_srid(l_srid, r_srid, builder.len(), ctx) {
                            builder.commit_row();
                            return;
                        }
                        let geos = [l_geo, r_geo];
                        let mut coords: Vec<Coord> = vec![];
                        for geo in geos.iter() {
                            match geo {
                                Geometry::Point(point) => {
                                    coords.push(point.0);
                                },
                                Geometry::LineString(line)=> {
                                    coords.append(&mut line.clone().into_inner());
                                },
                                Geometry::MultiPoint(multi_point)=> {
                                    for point in multi_point.into_iter() {
                                        coords.push(point.0);
                                    }
                                },
                                _ => {
                                    ctx.set_error(
                                        builder.len(),
                                        format!("Geometry expression must be a Point, MultiPoint, or LineString, but got {}",
                                            geometry_type_name(geo)
                                        )
                                    );
                                    builder.commit_row();
                                    return;
                                }
                            }
                        }

                        let geo = Geometry::from(LineString::new(coords));
                        match geo_to_ewkb(geo, l_srid) {
                            Ok(data) => builder.put_slice(data.as_slice()),
                            Err(e) => ctx.set_error(builder.len(), e.to_string()),
                        }
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        ctx.set_error(
                            builder.len(),
                            e.to_string(),
                        );
                    }
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _>(
        "st_geohash",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match point_to_geohash(ewkb, None) {
                Ok(hash) => builder.put_str(&hash),
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, Int32Type, StringType, _, _>(
        "st_geohash",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, Int32Type, StringType>(
            |geometry, precision, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                if precision > 12 {
                    ctx.set_error(
                        builder.len(),
                        "Currently the precision only implement within 12 digits!",
                    );
                    builder.commit_row();
                    return;
                }

                match point_to_geohash(geometry, Some(precision)) {
                    Ok(hash) => builder.put_str(&hash),
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                };
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _>(
        "st_geometryfromwkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|s, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let binary = match hex::decode(s) {
                Ok(binary) => binary,
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    builder.commit_row();
                    return;
                }
            };

            match ewkb_to_geo(&mut Ewkb(binary)).and_then(|(geo, srid)| geo_to_ewkb(geo, srid)) {
                Ok(ewkb) => builder.put_slice(ewkb.as_slice()),
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, GeometryType, _>(
        "st_geometryfromwkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BinaryType, GeometryType>(|binary, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(binary)).and_then(|(geo, srid)| geo_to_ewkb(geo, srid)) {
                Ok(ewkb) => builder.put_slice(ewkb.as_slice()),
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, Int32Type, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, Int32Type, GeometryType>(
            |s, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                let binary = match hex::decode(s) {
                    Ok(binary) => binary,
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.commit_row();
                        return;
                    }
                };

                match ewkb_to_geo(&mut Ewkb(binary))
                    .and_then(|(geo, _)| geo_to_ewkb(geo, Some(srid)))
                {
                    Ok(ewkb) => builder.put_slice(ewkb.as_slice()),
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<BinaryType, Int32Type, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<BinaryType, Int32Type, GeometryType>(
            |binary, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                match ewkb_to_geo(&mut Ewkb(binary))
                    .and_then(|(geo, _)| geo_to_ewkb(geo, Some(srid)))
                {
                    Ok(ewkb) => builder.put_slice(ewkb.as_slice()),
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _>(
        "st_geometryfromwkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|wkt, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match geometry_from_ewkt(wkt, None) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(builder.len(), e.to_string()),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, Int32Type, GeometryType, _, _>(
        "st_geometryfromwkt",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, Int32Type, GeometryType>(
            |wkt, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }
                match geometry_from_ewkt(wkt, Some(srid)) {
                    Ok(data) => builder.put_slice(data.as_slice()),
                    Err(e) => ctx.set_error(builder.len(), e.to_string()),
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, NumberType<F64>, _>(
        "st_length",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NumberType<F64>>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push(F64::from(0_f64));
                    return;
                }
            }

            match Ewkb(ewkb).to_geo() {
                Ok(geo) => {
                    let mut distance = 0f64;
                    match geo {
                        Geometry::LineString(lines) => {
                            for line in lines.lines() {
                                distance += Euclidean.length(&line);
                            }
                        }
                        Geometry::MultiLineString(multi_lines) => {
                            for line_string in multi_lines.0 {
                                for line in line_string.lines() {
                                    distance += Euclidean.length(&line);
                                }
                            }
                        }
                        Geometry::GeometryCollection(geom_c) => {
                            for geometry in geom_c.0 {
                                if let Geometry::LineString(line_string) = geometry {
                                    for line in line_string.lines() {
                                        distance += Euclidean.length(&line);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                    let distance = (distance * 1_000_000_000_f64).round() / 1_000_000_000_f64;
                    builder.push(distance.into());
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    builder.push(F64::from(0_f64));
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, NumberType<F64>, _>(
        "st_x",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NumberType<F64>>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push(F64::from(0_f64));
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, _)| match geo {
                Geometry::Point(point) => Ok(F64::from(AsPrimitive::<f64>::as_(point.x()))),
                _ => Err(ErrorCode::GeometryError(format!(
                    "Type {} is not supported as argument to ST_X",
                    geometry_type_name(&geo)
                ))),
            }) {
                Ok(x) => {
                    builder.push(x);
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    builder.push(F64::from(0_f64));
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, NumberType<F64>, _>(
        "st_y",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NumberType<F64>>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push(F64::from(0_f64));
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, _)| match geo {
                Geometry::Point(point) => Ok(F64::from(AsPrimitive::<f64>::as_(point.y()))),
                _ => Err(ErrorCode::GeometryError(format!(
                    "Type {} is not supported as argument to ST_Y",
                    geometry_type_name(&geo)
                ))),
            }) {
                Ok(x) => {
                    builder.push(x);
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    builder.push(F64::from(0_f64));
                }
            }
        }),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, Int32Type, GeometryType, _, _>(
        "st_setsrid",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, Int32Type, GeometryType>(
            |ewkb, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                match ewkb_to_geo(&mut Ewkb(ewkb)).and_then(|(geo, _)| geo_to_ewkb(geo, Some(srid)))
                {
                    Ok(ewkb) => {
                        builder.put_slice(ewkb.as_slice());
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, Int32Type, _>(
        "st_srid",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<GeometryType, Int32Type>(|ewkb, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(0);
                    return;
                }
            }

            let srid = read_srid(&mut Ewkb(ewkb)).unwrap_or(4326);
            output.push(srid);
        }),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, NumberType<F64>, _, _>(
        "st_xmax",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<NumberType<F64>>>(
            |ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }
                match Ewkb(ewkb).to_geo() {
                    Ok(geo) => match st_extreme(&geo, Axis::X, Extremum::Max) {
                        Some(x_max) => builder.push(F64::from(AsPrimitive::<f64>::as_(x_max))),
                        None => builder.push_null(),
                    },
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, NumberType<F64>, _, _>(
        "st_xmin",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<NumberType<F64>>>(
            |ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }
                match Ewkb(ewkb).to_geo() {
                    Ok(geo) => match st_extreme(&geo, Axis::X, Extremum::Min) {
                        Some(x_max) => builder.push(F64::from(AsPrimitive::<f64>::as_(x_max))),
                        None => builder.push_null(),
                    },
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, NumberType<F64>, _, _>(
        "st_ymax",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<NumberType<F64>>>(
            |ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match Ewkb(ewkb).to_geo() {
                    Ok(geo) => match st_extreme(&geo, Axis::Y, Extremum::Max) {
                        Some(x_max) => builder.push(F64::from(AsPrimitive::<f64>::as_(x_max))),
                        None => builder.push_null(),
                    },
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, NumberType<F64>, _, _>(
        "st_ymin",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<NumberType<F64>>>(
            |ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match Ewkb(ewkb).to_geo() {
                    Ok(geo) => match st_extreme(&geo, Axis::Y, Extremum::Min) {
                        Some(x_max) => builder.push(F64::from(AsPrimitive::<f64>::as_(x_max))),
                        None => builder.push_null(),
                    },
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, UInt32Type, _>(
        "st_npoints",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, UInt32Type>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push(0);
                    return;
                }
            }

            match Ewkb(ewkb).to_geo() {
                Ok(geo) => {
                    let npoints = count_points(&geo);
                    builder.push(npoints as u32);
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    builder.push(0);
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _>(
        "to_string",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match geometry_format(Ewkb(ewkb), ctx.func_ctx.geometry_output_format) {
                Ok(data) => builder.put_str(&data),
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _>(
        "to_geometry",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|s, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            match geometry_from_str(s, None) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(builder.len(), e.to_string()),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, Int32Type, GeometryType, _, _>(
        "to_geometry",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, Int32Type, GeometryType>(
            |s, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }
                match geometry_from_str(s, Some(srid)) {
                    Ok(data) => builder.put_slice(data.as_slice()),
                    Err(e) => ctx.set_error(builder.len(), e.to_string()),
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, GeometryType, _>(
        "to_geometry",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BinaryType, GeometryType>(|binary, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match ewkb_to_geo(&mut Ewkb(binary)).and_then(|(geo, srid)| geo_to_ewkb(geo, srid)) {
                Ok(ewkb) => builder.put_slice(ewkb.as_slice()),
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<BinaryType, Int32Type, GeometryType, _, _>(
        "to_geometry",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<BinaryType, Int32Type, GeometryType>(
            |binary, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                match ewkb_to_geo(&mut Ewkb(binary))
                    .and_then(|(geo, _)| geo_to_ewkb(geo, Some(srid)))
                {
                    Ok(ewkb) => builder.put_slice(ewkb.as_slice()),
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, GeometryType, _>(
        "to_geometry",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, GeometryType>(|json, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            match json_to_geometry_impl(json, None) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(builder.len(), e.to_string()),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<VariantType, Int32Type, GeometryType, _, _>(
        "to_geometry",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, Int32Type, GeometryType>(
            |json, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }
                match json_to_geometry_impl(json, Some(srid)) {
                    Ok(data) => builder.put_slice(data.as_slice()),
                    Err(e) => ctx.set_error(builder.len(), e.to_string()),
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<VariantType, GeometryType, _, _>(
        "try_to_geometry",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<GeometryType>>(
            |json, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match json_to_geometry_impl(json, None) {
                    Ok(data) => {
                        output.push(data.as_slice());
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, Int32Type, GeometryType, _, _>(
        "try_to_geometry",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, Int32Type, NullableType<GeometryType>>(
            |json, srid, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match json_to_geometry_impl(json, Some(srid)) {
                    Ok(data) => {
                        output.push(data.as_slice());
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<StringType, GeometryType, _, _>(
        "try_to_geometry",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, NullableType<GeometryType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match geometry_from_str(s, None) {
                Ok(data) => {
                    output.push(data.as_slice());
                }
                Err(_) => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_2_arg::<StringType, Int32Type, GeometryType, _, _>(
        "try_to_geometry",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, Int32Type, NullableType<GeometryType>>(
            |s, srid, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match geometry_from_str(s, Some(srid)) {
                    Ok(data) => {
                        output.push(data.as_slice());
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<BinaryType, GeometryType, _, _>(
        "try_to_geometry",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<BinaryType, NullableType<GeometryType>>(
            |binary, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }

                match ewkb_to_geo(&mut Ewkb(binary)) {
                    Ok((geo, srid)) => match geo.to_ewkb(CoordDimensions::xy(), srid) {
                        Ok(ewkb) => {
                            output.push(ewkb.as_slice());
                        }
                        Err(_) => output.push_null(),
                    },
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<BinaryType, Int32Type, GeometryType, _, _>(
        "try_to_geometry",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BinaryType, Int32Type, NullableType<GeometryType>>(
            |binary, srid, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }

                match Ewkb(binary).to_geo() {
                    Ok(geo) => match geo.to_ewkb(CoordDimensions::xy(), Some(srid)) {
                        Ok(ewkb) => {
                            output.push(ewkb.as_slice());
                        }
                        Err(_) => output.push_null(),
                    },
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, Int32Type, GeometryType, _, _>(
        "st_transform",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, Int32Type, GeometryType>(
            |original, to_srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                // All representations of the geo types supported by crates under the GeoRust organization, have not implemented srid().
                // Currently, the srid() of all types returns the default value `None`, so we need to parse it manually here.
                let from_srid = read_srid(&mut Ewkb(original));
                if from_srid.is_none() {
                    ctx.set_error(
                        builder.len(),
                        "input geometry must has the correct SRID".to_string(),
                    );
                    builder.commit_row();
                    return;
                }
                let from_srid = from_srid.unwrap();
                match st_transform_impl(original, from_srid, to_srid) {
                    Ok(data) => {
                        builder.put_slice(data.as_slice());
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }
                builder.commit_row();
            },
        ),
    );

    registry
        .register_passthrough_nullable_3_arg::<GeometryType, Int32Type, Int32Type, GeometryType, _, _>(
            "st_transform",
            |_, _, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_3_arg::<GeometryType, Int32Type, Int32Type, GeometryType>(
                |original, from_srid, to_srid, builder, ctx| {
                    if let Some(validity) = &ctx.validity {
                        if !validity.get_bit(builder.len()) {
                            builder.commit_row();
                            return;
                        }
                    }

                    match st_transform_impl(original, from_srid, to_srid) {
                        Ok(data) => {
                            builder.put_slice(data.as_slice());
                        }
                        Err(e) => {
                            ctx.set_error(builder.len(), e.to_string());
                        }
                    }
                    builder.commit_row();
                },
            ),
        );
}

fn st_transform_impl(
    original: &[u8],
    from_srid: i32,
    to_srid: i32,
) -> std::result::Result<Vec<u8>, Box<ErrorCode>> {
    let from_proj = Proj::from_epsg_code(
        u16::try_from(from_srid).map_err(|_| ErrorCode::GeometryError("invalid from srid"))?,
    )
    .map_err(|_| ErrorCode::GeometryError("invalid from srid"))?;
    let to_proj = Proj::from_epsg_code(
        u16::try_from(to_srid).map_err(|_| ErrorCode::GeometryError("invalid to srid"))?,
    )
    .map_err(|_| ErrorCode::GeometryError("invalid to srid"))?;

    let old = Ewkb(original.to_vec());
    let res = Ewkb(old.to_ewkb(old.dims(), Some(from_srid)).unwrap())
        .to_geo()
        .map_err(Box::new(ErrorCode::from))
        .and_then(|mut geom| {
            // EPSG:4326 WGS84 in proj4rs is in radians, not degrees.
            if from_srid == 4326 {
                geom.to_radians_in_place();
            }
            if transform(&from_proj, &to_proj, &mut geom).is_err() {
                return Err(ErrorCode::GeometryError(format!(
                    "transform from {} to {} failed",
                    from_srid, to_srid
                )));
            }
            if to_srid == 4326 {
                geom.to_degrees_in_place();
            }
            let round_geom = round_geometry_coordinates(geom);
            round_geom
                .to_ewkb(round_geom.dims(), Some(to_srid))
                .map_err(Box::new(ErrorCode::from))
        });
    match res {
        Ok(r) => Ok(r),
        Err(e) => Err(Box::new(e)),
    }
}

/// The argument str must be a string expression that represents a valid geometric object in one of the following formats:
///
/// WKT (well-known text).
/// WKB (well-known binary) in hexadecimal format (without a leading 0x).
/// EWKT (extended well-known text).
/// EWKB (extended well-known binary) in hexadecimal format (without a leading 0x).
/// GEOJSON
fn json_to_geometry_impl(
    binary: &[u8],
    srid: Option<i32>,
) -> std::result::Result<Vec<u8>, Box<ErrorCode>> {
    let raw_jsonb = RawJsonb::new(binary);
    let s = raw_jsonb.to_string();
    let json = GeoJson(s.as_str());
    match json.to_ewkb(CoordDimensions::xy(), srid) {
        Ok(data) => Ok(data),
        Err(e) => Err(Box::new(ErrorCode::GeometryError(e.to_string()))),
    }
}

/// The last three decimal places of the f64 type are inconsistent between aarch64 and x86 platforms,
/// causing unit test results to fail. We will only retain six decimal places.
fn round_geometry_coordinates(geom: Geometry<f64>) -> Geometry<f64> {
    fn round_coordinate(coord: f64) -> f64 {
        (coord * 1_000_000.0).round() / 1_000_000.0
    }

    match geom {
        Geometry::Point(point) => Geometry::Point(Point::new(
            round_coordinate(point.x()),
            round_coordinate(point.y()),
        )),
        Geometry::LineString(linestring) => Geometry::LineString(
            linestring
                .into_iter()
                .map(|coord| coord!(x:round_coordinate(coord.x), y:round_coordinate(coord.y)))
                .collect(),
        ),
        Geometry::Polygon(polygon) => {
            let outer_ring = polygon.exterior();
            let mut rounded_inner_rings = Vec::new();

            for inner_ring in polygon.interiors() {
                let rounded_coords: Vec<Coord<f64>> = inner_ring
                    .into_iter()
                    .map(
                        |coord| coord!( x: round_coordinate(coord.x), y: round_coordinate(coord.y)),
                    )
                    .collect();
                rounded_inner_rings.push(LineString(rounded_coords));
            }

            let rounded_polygon = Polygon::new(
                LineString(
                    outer_ring
                        .into_iter()
                        .map(|coord| coord!( x:round_coordinate(coord.x), y:round_coordinate(coord.y)))
                        .collect(),
                ),
                rounded_inner_rings,
            );

            Geometry::Polygon(rounded_polygon)
        }
        Geometry::MultiPoint(multipoint) => Geometry::MultiPoint(
            multipoint
                .into_iter()
                .map(|point| Point::new(round_coordinate(point.x()), round_coordinate(point.y())))
                .collect(),
        ),
        Geometry::MultiLineString(multilinestring) => {
            let rounded_lines: Vec<LineString<f64>> = multilinestring
                .into_iter()
                .map(|linestring| {
                    LineString(
                        linestring
                            .into_iter()
                            .map(|coord| coord!(x: round_coordinate(coord.x), y: round_coordinate(coord.y)))
                            .collect(),
                    )
                })
                .collect();

            Geometry::MultiLineString(MultiLineString::new(rounded_lines))
        }
        Geometry::MultiPolygon(multipolygon) => {
            let rounded_polygons: Vec<Polygon<f64>> = multipolygon
                .into_iter()
                .map(|polygon| {
                    let outer_ring = polygon.exterior().into_iter()
                        .map(|coord| coord!( x:round_coordinate(coord.x), y:round_coordinate(coord.y)))
                        .collect::<Vec<Coord<f64>>>();

                    let mut rounded_inner_rings = Vec::new();
                    for inner_ring in polygon.interiors() {
                        let rounded_coords: Vec<Coord<f64>> = inner_ring
                            .into_iter()
                            .map(|coord| coord!( x:round_coordinate(coord.x), y: coord.y))
                            .collect();
                        rounded_inner_rings.push(LineString(rounded_coords));
                    }

                    Polygon::new(LineString(outer_ring), rounded_inner_rings)
                })
                .collect();
            Geometry::MultiPolygon(MultiPolygon::new(rounded_polygons))
        }
        Geometry::GeometryCollection(geometrycollection) => Geometry::GeometryCollection(
            geometrycollection
                .into_iter()
                .map(round_geometry_coordinates)
                .collect(),
        ),
        Geometry::Line(line) => Geometry::Line(Line::new(
            Point::new(
                round_coordinate(line.start.x),
                round_coordinate(line.start.y),
            ),
            Point::new(round_coordinate(line.end.x), round_coordinate(line.end.y)),
        )),
        Geometry::Rect(rect) => Geometry::Rect(Rect::new(
            Point::new(
                round_coordinate(rect.min().x),
                round_coordinate(rect.min().y),
            ),
            Point::new(
                round_coordinate(rect.max().x),
                round_coordinate(rect.max().y),
            ),
        )),
        Geometry::Triangle(triangle) => Geometry::Triangle(Triangle::new(
            coord!(x: round_coordinate(triangle.0.x), y: round_coordinate(triangle.0.y)),
            coord!(x: round_coordinate(triangle.1.x), y: round_coordinate(triangle.1.y)),
            coord!(x: round_coordinate(triangle.2.x), y: round_coordinate(triangle.2.y)),
        )),
    }
}

fn check_incompatible_srid(
    l_srid: Option<i32>,
    r_srid: Option<i32>,
    len: usize,
    ctx: &mut EvalContext,
) -> bool {
    if !l_srid.eq(&r_srid) {
        ctx.set_error(
            len,
            format!(
                "Incompatible SRID: {} and {}",
                l_srid.unwrap_or_default(),
                r_srid.unwrap_or_default()
            ),
        );
        false
    } else {
        true
    }
}
