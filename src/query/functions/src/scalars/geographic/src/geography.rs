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
use databend_common_expression::hilbert_index_from_bounds;
use databend_common_expression::hilbert_index_from_bounds_slice;
use databend_common_expression::types::geography::Geography;
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
use databend_common_io::geography::LATITUDE_MAX;
use databend_common_io::geography::LATITUDE_MIN;
use databend_common_io::geography::LONGITUDE_MAX;
use databend_common_io::geography::LONGITUDE_MIN;
use databend_common_io::geography::geography_from_ewkb;
use databend_common_io::geography::geography_from_ewkt;
use databend_common_io::geography::geography_from_geojson;
use databend_common_io::geography::haversine_distance_between_geometries;
use databend_common_io::geography_format;
use databend_common_io::geometry::count_points;
use databend_common_io::geometry::geometry_bbox_center;
use databend_common_io::geometry::point_to_geohash;
use databend_common_io::geometry::st_extreme;
use databend_common_io::geometry_type_name;
use databend_common_io::wkb::make_point;
use geo::Coord;
use geo::Geodesic;
use geo::GeodesicArea;
use geo::Geometry;
use geo::HasDimensions;
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

use crate::register::geo_convert_fn;
use crate::register::geo_convert_with_arg_fn;
use crate::register::geo_try_convert_fn;
use crate::register::geography_binary_fn;
use crate::register::geography_unary_combine_fn;
use crate::register::geography_unary_fn;

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
            let row = builder.len();
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(row) {
                    builder.commit_row();
                    return;
                }
            }
            match GeographyType::check_point(*lon, *lat) {
                Ok(_) => {
                    builder.put_slice(&make_point(*lon, *lat));
                }
                Err(e) => {
                    ctx.set_error(row, e.to_string());
                }
            }
            builder.commit_row();
        })
    );

    registry.register_passthrough_nullable_1_arg::<GeographyType, VariantType, _>(
        "st_asgeojson",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, VariantType>(|ewkb, builder, ctx| {
            let row = builder.len();
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(row) {
                    builder.commit_row();
                    return;
                }
            }

            let result = Ewkb(ewkb)
                .to_geo()
                .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                .and_then(geo_to_json)
                .and_then(|json| {
                    parse_owned_jsonb_with_buf(json.as_bytes(), &mut builder.data)
                        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                });
            if let Err(e) = result {
                ctx.set_error(row, e.to_string());
            }
            builder.commit_row();
        }),
    );

    geography_unary_fn::<BinaryType>("st_asewkb", registry, |geo| {
        let ewkb = geo_to_ewkb(geo, Some(GEOGRAPHY_SRID))?;
        Ok(ewkb)
    });

    geography_unary_fn::<BinaryType>("st_aswkb", registry, |geo| {
        let wkb = geo_to_wkb(geo)?;
        Ok(wkb)
    });

    geography_unary_fn::<StringType>("st_asewkt", registry, |geo| {
        let ewkt = geo_to_ewkt(geo, Some(GEOGRAPHY_SRID))?;
        Ok(ewkt)
    });

    geography_unary_fn::<StringType>("st_aswkt", registry, |geo| {
        let wkt = geo_to_wkt(geo)?;
        Ok(wkt)
    });

    geography_binary_fn::<NumberType<F64>>("st_distance", registry, |l_geo, r_geo| {
        let distance = haversine_distance_between_geometries(&l_geo, &r_geo)?;
        let distance = (distance * 1_000_000_000_f64).round() / 1_000_000_000_f64;
        Ok(distance.into())
    });

    geography_unary_fn::<NumberType<F64>>("st_area", registry, |geo| {
        let area = geo.geodesic_area_unsigned();
        let area = (area * 1_000_000_000_f64).round() / 1_000_000_000_f64;
        Ok(area.into())
    });

    geography_unary_combine_fn::<GeographyType>("st_endpoint", registry, |geo| match geo {
        Geometry::LineString(line_string) => {
            let mut points = line_string.points();
            if let Some(point) = points.next_back() {
                let ewkb = geo_to_ewkb(Geometry::from(point), None)?;
                Ok(Some(Geography(ewkb)))
            } else {
                Ok(None)
            }
        }
        _ => Err(ErrorCode::GeometryError(format!(
            "Type {} is not supported as argument to ST_ENDPOINT",
            geometry_type_name(&geo)
        ))),
    });

    geography_unary_combine_fn::<GeographyType>("st_startpoint", registry, |geo| match geo {
        Geometry::LineString(line_string) => {
            let mut points = line_string.points();
            if let Some(point) = points.next() {
                let ewkb = geo_to_ewkb(Geometry::from(point), None)?;
                Ok(Some(Geography(ewkb)))
            } else {
                Ok(None)
            }
        }
        _ => Err(ErrorCode::GeometryError(format!(
            "Type {} is not supported as argument to ST_STARTPOINT",
            geometry_type_name(&geo)
        ))),
    });

    registry.register_combine_nullable_2_arg::<GeographyType, Int32Type, GeographyType, _, _>(
        "st_pointn",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeographyType, Int32Type, NullableType<GeographyType>>(
            |ewkb, index, builder, ctx| {
                let row = builder.len();
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(row) {
                        builder.push_null();
                        return;
                    }
                }

                let result = Ewkb(ewkb)
                    .to_geo()
                    .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                    .and_then(|geo| match geo {
                        Geometry::LineString(line_string) => {
                            let mut points = line_string.points();
                            let len = points.len() as i32;
                            let idx = if index < 0 { len + index } else { index - 1 };
                            if let Some(point) = points.nth(idx as usize) {
                                geo_to_ewkb(Geometry::from(point), None)
                                    .map(|binary| Some(Geography(binary)))
                            } else {
                                Ok(None)
                            }
                        }
                        _ => Err(ErrorCode::GeometryError(format!(
                            "Type {} is not supported as argument to ST_POINTN",
                            geometry_type_name(&geo)
                        ))),
                    });

                match result {
                    Ok(Some(binary)) => builder.push(binary.as_ref()),
                    Ok(None) => builder.push_null(),
                    Err(e) => {
                        ctx.set_error(row, e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );

    geography_unary_combine_fn::<NumberType<i32>>("st_dimension", registry, |geo| {
        let dimension: Option<i32> = match geo.dimensions() {
            Dimensions::Empty => None,
            Dimensions::ZeroDimensional => Some(0),
            Dimensions::OneDimensional => Some(1),
            Dimensions::TwoDimensional => Some(2),
        };
        Ok(dimension)
    });

    geo_convert_fn::<StringType, GeographyType>("st_geogfromgeohash", registry, |geohash| {
        if geohash.len() > 12 {
            return Err(ErrorCode::GeometryError(
                "Currently the precision only implement within 12 digits!",
            ));
        }
        let rect = decode_bbox(geohash).map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
        let geo: Geometry = rect.into();
        let ewkb = geo_to_ewkb(geo, None)?;
        Ok(Geography(ewkb))
    });

    geo_convert_fn::<StringType, GeographyType>("st_geogpointfromgeohash", registry, |geohash| {
        if geohash.len() > 12 {
            return Err(ErrorCode::GeometryError(
                "Currently the precision only implement within 12 digits!",
            ));
        }
        let rect = decode_bbox(geohash).map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
        let geo: Geometry = Point::from(rect.center()).into();
        let ewkb = geo_to_ewkb(geo, None)?;
        Ok(Geography(ewkb))
    });

    geography_unary_fn::<GeographyType>("st_makepolygon", registry, |geo| match geo {
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
                let ewkb = geo_to_ewkb(geo, None)?;
                Ok(Geography(ewkb))
            }
        }
        _ => Err(ErrorCode::GeometryError(format!(
            "Type {} is not supported as argument to ST_MAKEPOLYGON",
            geometry_type_name(&geo)
        ))),
    });

    geography_binary_fn::<GeographyType>("st_makeline", registry, |l_geo, r_geo| {
        let geos = [l_geo, r_geo];
        let mut coords: Vec<Coord> = vec![];
        for geo in geos.iter() {
            match geo {
                Geometry::Point(point) => {
                    coords.push(point.0);
                }
                Geometry::LineString(line) => {
                    coords.extend(line.clone().into_inner());
                }
                Geometry::MultiPoint(multi_point) => {
                    for point in multi_point.into_iter() {
                        coords.push(point.0);
                    }
                }
                _ => {
                    return Err(ErrorCode::GeometryError(format!(
                        "Geometry expression must be a Point, MultiPoint, or LineString, but got {}",
                        geometry_type_name(geo)
                    )));
                }
            }
        }

        let geo = Geometry::from(LineString::new(coords));
        let data = geo_to_ewkb(geo, None)?;
        Ok(Geography(data))
    });

    geo_convert_fn::<GeographyType, StringType>("st_geohash", registry, |input| {
        point_to_geohash(input.as_ref(), None)
    });

    geo_convert_with_arg_fn::<GeographyType, StringType>(
        "st_geohash",
        registry,
        |input, precision| {
            if precision > 12 {
                return Err(ErrorCode::GeometryError(
                    "Currently the precision only implement within 12 digits!",
                ));
            }
            point_to_geohash(input.as_ref(), Some(precision))
        },
    );

    geo_convert_fn::<StringType, GeographyType>("st_geographyfromwkb", registry, |s| {
        let binary = hex::decode(s).map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
        let data = geography_from_ewkb(&binary)?;
        Ok(Geography(data))
    });

    geo_convert_fn::<BinaryType, GeographyType>("st_geographyfromwkb", registry, |binary| {
        let data = geography_from_ewkb(binary)?;
        Ok(Geography(data))
    });

    geo_convert_fn::<StringType, GeographyType>("st_geographyfromwkt", registry, |wkt| {
        let data = geography_from_ewkt(wkt)?;
        Ok(Geography(data))
    });

    geography_unary_fn::<NumberType<F64>>("st_length", registry, |geo| {
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
        Ok(distance.into())
    });

    geography_unary_fn::<NumberType<F64>>("st_x", registry, |geo| match geo {
        Geometry::Point(point) => Ok(AsPrimitive::<f64>::as_(point.x()).into()),
        _ => Err(ErrorCode::GeometryError(format!(
            "Type {} is not supported as argument to ST_X",
            geometry_type_name(&geo)
        ))),
    });

    geography_unary_fn::<NumberType<F64>>("st_y", registry, |geo| match geo {
        Geometry::Point(point) => Ok(AsPrimitive::<f64>::as_(point.y()).into()),
        _ => Err(ErrorCode::GeometryError(format!(
            "Type {} is not supported as argument to ST_Y",
            geometry_type_name(&geo)
        ))),
    });

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

    geography_unary_combine_fn::<NumberType<F64>>("st_xmax", registry, |geo| {
        Ok(st_extreme(&geo, Axis::X, Extremum::Max)
            .map(|x_max| F64::from(AsPrimitive::<f64>::as_(x_max))))
    });

    geography_unary_combine_fn::<NumberType<F64>>("st_xmin", registry, |geo| {
        Ok(st_extreme(&geo, Axis::X, Extremum::Min)
            .map(|x_min| F64::from(AsPrimitive::<f64>::as_(x_min))))
    });

    geography_unary_combine_fn::<NumberType<F64>>("st_ymax", registry, |geo| {
        Ok(st_extreme(&geo, Axis::Y, Extremum::Max)
            .map(|y_max| F64::from(AsPrimitive::<f64>::as_(y_max))))
    });

    geography_unary_combine_fn::<NumberType<F64>>("st_ymin", registry, |geo| {
        Ok(st_extreme(&geo, Axis::Y, Extremum::Min)
            .map(|y_min| F64::from(AsPrimitive::<f64>::as_(y_min))))
    });

    geography_unary_fn::<UInt32Type>("st_npoints", registry, |geo| {
        let npoints = count_points(&geo);
        Ok(npoints as u32)
    });

    registry.register_passthrough_nullable_1_arg::<GeographyType, StringType, _>(
        "to_string",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeographyType, StringType>(|ewkb, builder, ctx| {
            let row = builder.len();
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(row) {
                    builder.commit_row();
                    return;
                }
            }

            match geography_format(ewkb.0, ctx.func_ctx.geometry_output_format) {
                Ok(data) => builder.put_str(&data),
                Err(e) => {
                    ctx.set_error(row, e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    geo_convert_fn::<VariantType, GeographyType>("to_geography", registry, |val| {
        let raw_jsonb = RawJsonb::new(val);
        let json_str = raw_jsonb.to_string();
        let data = geography_from_geojson(&json_str)?;
        Ok(Geography(data))
    });

    geo_convert_fn::<StringType, GeographyType>("to_geography", registry, |s| {
        let data = geography_from_ewkt(s)?;
        Ok(Geography(data))
    });

    geo_convert_fn::<BinaryType, GeographyType>("to_geography", registry, |binary| {
        let data = geography_from_ewkb(binary)?;
        Ok(Geography(data))
    });

    geo_try_convert_fn::<VariantType, GeographyType>("try_to_geography", registry, |val| {
        let raw_jsonb = RawJsonb::new(val);
        let json_str = raw_jsonb.to_string();
        let data = geography_from_geojson(&json_str)?;
        Ok(Geography(data))
    });

    geo_try_convert_fn::<StringType, GeographyType>("try_to_geography", registry, |s| {
        let data = geography_from_ewkt(s)?;
        Ok(Geography(data))
    });

    geo_try_convert_fn::<BinaryType, GeographyType>("try_to_geography", registry, |binary| {
        let data = geography_from_ewkb(binary)?;
        Ok(Geography(data))
    });

    geography_unary_combine_fn::<UInt64Type>("st_hilbert", registry, |geo| {
        match geometry_bbox_center(&geo) {
            Some((x, y)) => {
                let index = hilbert_index_from_bounds(
                    x,
                    y,
                    LONGITUDE_MIN,
                    LATITUDE_MIN,
                    LONGITUDE_MAX,
                    LATITUDE_MAX,
                )?;
                Ok(Some(index))
            }
            None => Ok(None),
        }
    });

    registry.register_combine_nullable_2_arg::<
        GeographyType,
        ArrayType<NumberType<F64>>,
        UInt64Type,
        _,
        _,
    >(
        "st_hilbert",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<
            GeographyType,
            ArrayType<NumberType<F64>>,
            NullableType<UInt64Type>,
        >(|ewkb, bounds, builder, ctx| {
            let row = builder.len();
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(row) {
                    builder.push_null();
                    return;
                }
            }

            let result = Ewkb(ewkb.as_ref()).to_geo().map(|geo| geometry_bbox_center(&geo));
            match result {
                Ok(Some((x, y))) => match hilbert_index_from_bounds_slice(x, y, &bounds) {
                    Ok(index) => builder.push(index),
                    Err(e) => {
                        ctx.set_error(row, e.to_string());
                        builder.push_null();
                    }
                },
                Ok(None) => builder.push_null(),
                Err(e) => {
                    ctx.set_error(row, e.to_string());
                    builder.push_null();
                }
            }
        }),
    );
}
