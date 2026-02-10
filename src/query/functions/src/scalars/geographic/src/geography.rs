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
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::types::geography::GeographyRef;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_io::Axis;
use databend_common_io::Extremum;
use databend_common_io::geo_to_ewkb;
use databend_common_io::geo_to_ewkt;
use databend_common_io::geo_to_json;
use databend_common_io::geo_to_wkb;
use databend_common_io::geo_to_wkt;
use databend_common_io::geography::GEOGRAPHY_SRID;
use databend_common_io::geography::geography_from_ewkb;
use databend_common_io::geography::geography_from_ewkt;
use databend_common_io::geography::geography_from_geojson;
use databend_common_io::geometry::count_points;
use databend_common_io::geometry::point_to_geohash;
use databend_common_io::geometry::st_extreme;
use databend_common_io::geometry_format;
use databend_common_io::geometry_type_name;
use databend_common_io::wkb::make_point;
use geo::Closest;
use geo::Coord;
use geo::CoordsIter;
use geo::Distance;
use geo::Geodesic;
use geo::GeodesicArea;
use geo::Geometry;
use geo::HasDimensions;
use geo::Haversine;
use geo::HaversineClosestPoint;
use geo::Length;
use geo::LineString;
use geo::Point;
use geo::Polygon;
use geo::dimensions::Dimensions;
use geohash::decode_bbox;
use geozero::ToGeo;
use geozero::wkb::Ewkb;
use jsonb::RawJsonb;
use jsonb::parse_owned_jsonb_with_buf;
use num_traits::AsPrimitive;

pub fn register(registry: &mut FunctionRegistry) {
    // aliases
    registry.register_aliases("st_makepoint", &["st_point"]);
    registry.register_aliases("st_geographyfromwkt", &[
        "st_geogfromwkt",
        "st_geographyfromewkt",
        "st_geogfromwkt",
        "st_geographyfromtext",
        "st_geogfromtext",
    ]);
    registry.register_aliases("st_geographyfromwkb", &[
        "st_geogfromwkb",
        "st_geogetryfromwkb",
        "st_geogfromewkb",
    ]);

    registry.register_passthrough_nullable_2_arg::<NumberType<F64>, NumberType<F64>, GeographyType, _, _>(
        "st_makepoint",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<NumberType<F64>, NumberType<F64>, GeographyType> (|lon,lat,builder,ctx|{
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            if let Err(e) = GeographyType::check_point(*lon, *lat) {
                ctx.set_error(builder.len(), e.to_string());
                builder.commit_row()
            } else {
                builder.put_slice(&make_point(*lon, *lat));
                builder.commit_row()
            }
        })
    );

    registry.register_passthrough_nullable_1_arg::<GeographyType, VariantType, _>(
        "st_asgeojson",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, VariantType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match Ewkb(ewkb).to_geo() {
                Ok(geo) => match geo_to_json(geo) {
                    Ok(json) => {
                        if let Err(e) =
                            parse_owned_jsonb_with_buf(json.as_bytes(), &mut builder.data)
                        {
                            ctx.set_error(builder.len(), e.to_string());
                        }
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeographyType, BinaryType, _>(
        "st_asewkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, BinaryType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match Ewkb(ewkb).to_geo() {
                Ok(geo) => match geo_to_ewkb(geo, Some(GEOGRAPHY_SRID)) {
                    Ok(ewkb) => {
                        builder.put_slice(ewkb.as_slice());
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeographyType, BinaryType, _>(
        "st_aswkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, BinaryType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match Ewkb(ewkb).to_geo() {
                Ok(geo) => match geo_to_wkb(geo) {
                    Ok(wkb) => {
                        builder.put_slice(wkb.as_slice());
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeographyType, StringType, _>(
        "st_asewkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, StringType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match Ewkb(ewkb).to_geo() {
                Ok(geo) => match geo_to_ewkt(geo, Some(GEOGRAPHY_SRID)) {
                    Ok(ewkt) => {
                        builder.put_str(&ewkt);
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeographyType, StringType, _>(
        "st_aswkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, StringType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match Ewkb(ewkb).to_geo() {
                Ok(geo) => match geo_to_wkt(geo) {
                    Ok(wkt) => {
                        builder.put_str(&wkt);
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry
        .register_passthrough_nullable_2_arg::<GeographyType, GeographyType, NumberType<F64>, _, _>(
            "st_distance",
            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<GeographyType, GeographyType, NumberType<F64>>(
                |l_ewkb, r_ewkb, builder, ctx| {
                    if let Some(validity) = &ctx.validity {
                        if !validity.get_bit(builder.len()) {
                            builder.push(F64::from(0_f64));
                            return;
                        }
                    }

                    match (Ewkb(l_ewkb).to_geo(), Ewkb(r_ewkb).to_geo()) {
                        (Ok(l_geo), Ok(r_geo)) => {
                            let distance = haversine_distance_between_geometries(&l_geo, &r_geo);
                            match distance {
                                Ok(distance) => {
                                    let distance =
                                        (distance * 1_000_000_000_f64).round() / 1_000_000_000_f64;
                                    builder.push(distance.into());
                                }
                                Err(e) => {
                                    ctx.set_error(builder.len(), e.to_string());
                                    builder.push(F64::from(0_f64));
                                }
                            }
                        }
                        (Err(e), _) | (_, Err(e)) => {
                            ctx.set_error(builder.len(), e.to_string());
                            builder.push(F64::from(0_f64));
                        }
                    }
                },
            ),
        );

    registry.register_passthrough_nullable_1_arg::<GeographyType, NumberType<F64>, _>(
        "st_area",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, NumberType<F64>>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push(F64::from(0_f64));
                    return;
                }
            }

            match Ewkb(ewkb).to_geo() {
                Ok(geo) => {
                    let area = geo.geodesic_area_unsigned();
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

    registry.register_combine_nullable_1_arg::<GeographyType, GeographyType, _, _>(
        "st_endpoint",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, NullableType<GeographyType>>(
            |ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match Ewkb(ewkb).to_geo() {
                    Ok(geo) => match geo {
                        Geometry::LineString(line_string) => {
                            let mut points = line_string.points();
                            if let Some(point) = points.next_back() {
                                match geo_to_ewkb(Geometry::from(point), None) {
                                    Ok(binary) => builder.push(GeographyRef(binary.as_slice())),
                                    Err(e) => {
                                        ctx.set_error(builder.len(), e.to_string());
                                        builder.push_null();
                                    }
                                }
                            } else {
                                builder.push_null();
                            }
                        }
                        _ => {
                            ctx.set_error(
                                builder.len(),
                                format!(
                                    "Type {} is not supported as argument to ST_ENDPOINT",
                                    geometry_type_name(&geo)
                                ),
                            );
                            builder.push_null();
                        }
                    },
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeographyType, GeographyType, _, _>(
        "st_startpoint",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, NullableType<GeographyType>>(
            |ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match Ewkb(ewkb).to_geo() {
                    Ok(geo) => match geo {
                        Geometry::LineString(line_string) => {
                            let mut points = line_string.points();
                            if let Some(point) = points.next() {
                                match geo_to_ewkb(Geometry::from(point), None) {
                                    Ok(binary) => builder.push(GeographyRef(binary.as_slice())),
                                    Err(e) => {
                                        ctx.set_error(builder.len(), e.to_string());
                                        builder.push_null();
                                    }
                                }
                            } else {
                                builder.push_null();
                            }
                        }
                        _ => {
                            ctx.set_error(
                                builder.len(),
                                format!(
                                    "Type {} is not supported as argument to ST_STARTPOINT",
                                    geometry_type_name(&geo)
                                ),
                            );
                            builder.push_null();
                        }
                    },
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<GeographyType, Int32Type, GeographyType, _, _>(
        "st_pointn",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeographyType, Int32Type, NullableType<GeographyType>>(
            |ewkb, index, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match Ewkb(ewkb).to_geo() {
                    Ok(geo) => match geo {
                        Geometry::LineString(line_string) => {
                            let mut points = line_string.points();
                            let len = points.len() as i32;
                            let idx = if index < 0 { len + index } else { index - 1 };
                            if let Some(point) = points.nth(idx as usize) {
                                match geo_to_ewkb(Geometry::from(point), None) {
                                    Ok(binary) => builder.push(GeographyRef(binary.as_slice())),
                                    Err(e) => {
                                        ctx.set_error(builder.len(), e.to_string());
                                        builder.push_null();
                                    }
                                }
                            } else {
                                builder.push_null();
                            }
                        }
                        _ => {
                            ctx.set_error(
                                builder.len(),
                                format!(
                                    "Type {} is not supported as argument to ST_POINTN",
                                    geometry_type_name(&geo)
                                ),
                            );
                            builder.push_null();
                        }
                    },
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeographyType, Int32Type, _, _>(
        "st_dimension",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, NullableType<Int32Type>>(
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

    registry.register_passthrough_nullable_1_arg::<StringType, GeographyType, _>(
        "st_geogfromgeohash",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeographyType>(|geohash, builder, ctx| {
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

    registry.register_passthrough_nullable_1_arg::<StringType, GeographyType, _>(
        "st_geogpointfromgeohash",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeographyType>(|geohash, builder, ctx| {
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

    registry.register_passthrough_nullable_1_arg::<GeographyType, GeographyType, _>(
        "st_makepolygon",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, GeographyType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match Ewkb(ewkb).to_geo() {
                Ok(geo) => match geo {
                    Geometry::LineString(line_string) => {
                        let points = line_string.into_points();
                        if points.len() < 4 {
                            ctx.set_error(
                                builder.len(),
                                format!(
                                    "Input lines must have at least 4 points, but got {}",
                                    points.len()
                                ),
                            );
                        } else if points.last() != points.first() {
                            ctx.set_error(
                                builder.len(),
                                "The first point and last point are not equal".to_string(),
                            );
                        } else {
                            let polygon = Polygon::new(LineString::from(points), vec![]);
                            let geo: Geometry = polygon.into();
                            match geo_to_ewkb(geo, None) {
                                Ok(binary) => builder.put_slice(binary.as_slice()),
                                Err(e) => {
                                    ctx.set_error(builder.len(), e.to_string());
                                }
                            }
                        }
                    }
                    _ => {
                        ctx.set_error(
                            builder.len(),
                            format!(
                                "Type {} is not supported as argument to ST_MAKEPOLYGON",
                                geometry_type_name(&geo)
                            ),
                        );
                    }
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<GeographyType, GeographyType, GeographyType, _, _>(
        "st_makeline",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeographyType, GeographyType, GeographyType>(
            |l_ewkb, r_ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                match (Ewkb(l_ewkb).to_geo(), Ewkb(r_ewkb).to_geo()) {
                    (Ok(l_geo), Ok(r_geo)) => {
                        let geos = [l_geo, r_geo];
                        let mut coords: Vec<Coord> = vec![];
                        for geo in geos.iter() {
                            match geo {
                                Geometry::Point(point) => {
                                    coords.push(point.0);
                                }
                                Geometry::LineString(line) => {
                                    coords.append(&mut line.clone().into_inner());
                                }
                                Geometry::MultiPoint(multi_point) => {
                                    for point in multi_point.into_iter() {
                                        coords.push(point.0);
                                    }
                                }
                                _ => {
                                    ctx.set_error(
                                        builder.len(),
                                        format!(
                                            "Geometry expression must be a Point, MultiPoint, or LineString, but got {}",
                                            geometry_type_name(geo)
                                        ),
                                    );
                                    builder.commit_row();
                                    return;
                                }
                            }
                        }

                        let geo = Geometry::from(LineString::new(coords));
                        match geo_to_ewkb(geo, None) {
                            Ok(data) => builder.put_slice(data.as_slice()),
                            Err(e) => ctx.set_error(builder.len(), e.to_string()),
                        }
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<GeographyType, StringType, _>(
        "st_geohash",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, StringType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match point_to_geohash(ewkb.as_ref(), None) {
                Ok(hash) => builder.put_str(&hash),
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<GeographyType, Int32Type, StringType, _, _>(
        "st_geohash",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeographyType, Int32Type, StringType>(
            |geography, precision, builder, ctx| {
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

                match point_to_geohash(geography.as_ref(), Some(precision)) {
                    Ok(hash) => builder.put_str(&hash),
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                };
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeographyType, _>(
        "st_geographyfromwkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeographyType>(|s, builder, ctx| {
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

            match geography_from_ewkb(&binary) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(builder.len(), e.to_string()),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, GeographyType, _>(
        "st_geographyfromwkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BinaryType, GeographyType>(|binary, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match geography_from_ewkb(binary) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(builder.len(), e.to_string()),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeographyType, _>(
        "st_geographyfromwkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeographyType>(|wkt, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match geography_from_ewkt(wkt) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(builder.len(), e.to_string()),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeographyType, NumberType<F64>, _>(
        "st_length",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, NumberType<F64>>(|ewkb, builder, ctx| {
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
                        Geometry::Line(line) => {
                            distance += Geodesic.length(&line);
                        }
                        Geometry::LineString(lines) => {
                            distance += Geodesic.length(&lines);
                        }
                        Geometry::MultiLineString(multi_lines) => {
                            distance += Geodesic.length(&multi_lines);
                        }
                        Geometry::GeometryCollection(geom_c) => {
                            for geometry in geom_c.0 {
                                match geometry {
                                    Geometry::Line(line) => {
                                        distance += Geodesic.length(&line);
                                    }
                                    Geometry::LineString(line_string) => {
                                        distance += Geodesic.length(&line_string);
                                    }
                                    Geometry::MultiLineString(multi_lines) => {
                                        distance += Geodesic.length(&multi_lines);
                                    }
                                    _ => {}
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

    registry.register_passthrough_nullable_1_arg::<GeographyType, NumberType<F64>, _>(
        "st_x",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, NumberType<F64>>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push(F64::from(0_f64));
                    return;
                }
            }

            match Ewkb(ewkb).to_geo() {
                Ok(geo) => match geo {
                    Geometry::Point(point) => {
                        builder.push(F64::from(AsPrimitive::<f64>::as_(point.x())));
                    }
                    _ => {
                        ctx.set_error(
                            builder.len(),
                            format!(
                                "Type {} is not supported as argument to ST_X",
                                geometry_type_name(&geo)
                            ),
                        );
                        builder.push(F64::from(0_f64));
                    }
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    builder.push(F64::from(0_f64));
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeographyType, NumberType<F64>, _>(
        "st_y",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, NumberType<F64>>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.push(F64::from(0_f64));
                    return;
                }
            }

            match Ewkb(ewkb).to_geo() {
                Ok(geo) => match geo {
                    Geometry::Point(point) => {
                        builder.push(F64::from(AsPrimitive::<f64>::as_(point.y())));
                    }
                    _ => {
                        ctx.set_error(
                            builder.len(),
                            format!(
                                "Type {} is not supported as argument to ST_Y",
                                geometry_type_name(&geo)
                            ),
                        );
                        builder.push(F64::from(0_f64));
                    }
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                    builder.push(F64::from(0_f64));
                }
            }
        }),
    );

    registry.register_1_arg::<GeographyType, Int32Type, _>(
        "st_srid",
        |_, _| {
            FunctionDomain::Domain(SimpleDomain {
                min: GEOGRAPHY_SRID,
                max: GEOGRAPHY_SRID,
            })
        },
        |_, _| {
            // For any value of the GEOGRAPHY type, only SRID 4326 is supported.
            GEOGRAPHY_SRID
        },
    );

    registry.register_combine_nullable_1_arg::<GeographyType, NumberType<F64>, _, _>(
        "st_xmax",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, NullableType<NumberType<F64>>>(
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

    registry.register_combine_nullable_1_arg::<GeographyType, NumberType<F64>, _, _>(
        "st_xmin",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, NullableType<NumberType<F64>>>(
            |ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }
                match Ewkb(ewkb).to_geo() {
                    Ok(geo) => match st_extreme(&geo, Axis::X, Extremum::Min) {
                        Some(x_min) => builder.push(F64::from(AsPrimitive::<f64>::as_(x_min))),
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

    registry.register_combine_nullable_1_arg::<GeographyType, NumberType<F64>, _, _>(
        "st_ymax",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, NullableType<NumberType<F64>>>(
            |ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }
                match Ewkb(ewkb).to_geo() {
                    Ok(geo) => match st_extreme(&geo, Axis::Y, Extremum::Max) {
                        Some(y_max) => builder.push(F64::from(AsPrimitive::<f64>::as_(y_max))),
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

    registry.register_combine_nullable_1_arg::<GeographyType, NumberType<F64>, _, _>(
        "st_ymin",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, NullableType<NumberType<F64>>>(
            |ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }
                match Ewkb(ewkb).to_geo() {
                    Ok(geo) => match st_extreme(&geo, Axis::Y, Extremum::Min) {
                        Some(y_min) => builder.push(F64::from(AsPrimitive::<f64>::as_(y_min))),
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

    registry.register_passthrough_nullable_1_arg::<GeographyType, UInt32Type, _>(
        "st_npoints",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, UInt32Type>(|ewkb, builder, ctx| {
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

    registry.register_passthrough_nullable_1_arg::<GeographyType, StringType, _>(
        "to_string",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, StringType>(|ewkb, builder, ctx| {
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

    registry.register_passthrough_nullable_1_arg::<VariantType, GeographyType, _>(
        "to_geography",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, GeographyType>(|val, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            let raw_jsonb = RawJsonb::new(val);
            let json_str = raw_jsonb.to_string();
            match geography_from_geojson(&json_str) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(builder.len(), e.to_string()),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeographyType, _>(
        "to_geography",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeographyType>(|s, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            match geography_from_ewkt(s) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(builder.len(), e.to_string()),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, GeographyType, _>(
        "to_geography",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BinaryType, GeographyType>(|binary, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match geography_from_ewkb(binary) {
                Ok(ewkb) => builder.put_slice(ewkb.as_slice()),
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, GeographyType, _, _>(
        "try_to_geography",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<GeographyType>>(
            |val, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                let raw_jsonb = RawJsonb::new(val);
                let json_str = raw_jsonb.to_string();
                match geography_from_geojson(&json_str) {
                    Ok(data) => {
                        output.push(GeographyRef(data.as_slice()));
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<StringType, GeographyType, _, _>(
        "try_to_geography",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, NullableType<GeographyType>>(
            |s, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match geography_from_ewkt(s) {
                    Ok(data) => {
                        output.push(GeographyRef(data.as_slice()));
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<BinaryType, GeographyType, _, _>(
        "try_to_geography",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<BinaryType, NullableType<GeographyType>>(
            |binary, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match geography_from_ewkb(binary) {
                    Ok(data) => {
                        output.push(GeographyRef(data.as_slice()));
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );
}

fn haversine_distance_point_to_geometry(
    point: &Point<f64>,
    geometry: &Geometry<f64>,
) -> std::result::Result<f64, Box<ErrorCode>> {
    match geometry.haversine_closest_point(point) {
        Closest::Intersection(_) => Ok(0_f64),
        Closest::SinglePoint(closest) => Ok(Haversine.distance(*point, closest)),
        Closest::Indeterminate => Err(Box::new(ErrorCode::GeometryError(
            "failed to calculate distance for empty geography".to_string(),
        ))),
    }
}

fn haversine_distance_between_geometries(
    left: &Geometry<f64>,
    right: &Geometry<f64>,
) -> std::result::Result<f64, Box<ErrorCode>> {
    match (left, right) {
        (Geometry::Point(l_point), _) => {
            return haversine_distance_point_to_geometry(l_point, right);
        }
        (_, Geometry::Point(r_point)) => {
            return haversine_distance_point_to_geometry(r_point, left);
        }
        (_, _) => {}
    }

    let mut min_distance: Option<f64> = None;
    for coord in left.coords_iter() {
        let point = Point::new(coord.x, coord.y);
        let distance = haversine_distance_point_to_geometry(&point, right)?;
        min_distance = Some(match min_distance {
            Some(current) => current.min(distance),
            None => distance,
        });
    }
    for coord in right.coords_iter() {
        let point = Point::new(coord.x, coord.y);
        let distance = haversine_distance_point_to_geometry(&point, left)?;
        min_distance = Some(match min_distance {
            Some(current) => current.min(distance),
            None => distance,
        });
    }

    min_distance.ok_or_else(|| {
        Box::new(ErrorCode::GeometryError(
            "failed to calculate distance for empty geography".to_string(),
        ))
    })
}
