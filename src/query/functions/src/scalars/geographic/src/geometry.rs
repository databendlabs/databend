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
use databend_common_expression::geographic::GeoAggOp;
use databend_common_expression::geographic::GeometryDifferenceAggOp;
use databend_common_expression::geographic::GeometryIntersectionAggOp;
use databend_common_expression::geographic::GeometrySymDifferenceAggOp;
use databend_common_expression::geographic::GeometryUnionAggOp;
use databend_common_expression::hilbert_index_from_bounds_slice;
use databend_common_expression::hilbert_index_from_point;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::F64;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::geometry::GeometryType;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_expression::vectorize_with_builder_4_arg;
use databend_common_io::Axis;
use databend_common_io::Extremum;
use databend_common_io::GEOGRAPHY_SRID;
use databend_common_io::ewkb_to_geo;
use databend_common_io::geo_to_ewkb;
use databend_common_io::geo_to_ewkt;
use databend_common_io::geo_to_json;
use databend_common_io::geo_to_wkb;
use databend_common_io::geo_to_wkt;
use databend_common_io::geometry::count_points;
use databend_common_io::geometry::geometry_bbox_center;
use databend_common_io::geometry::geometry_from_str;
use databend_common_io::geometry::point_to_geohash;
use databend_common_io::geometry::rect_to_polygon;
use databend_common_io::geometry::st_extreme;
use databend_common_io::geometry_format;
use databend_common_io::geometry_from_ewkt;
use databend_common_io::geometry_type_name;
use databend_common_io::read_srid;
use geo::Area;
use geo::BoundingRect;
use geo::Centroid;
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

use crate::register::geo_convert_fn;
use crate::register::geo_convert_with_arg_fn;
use crate::register::geo_try_convert_fn;
use crate::register::geo_try_convert_with_arg_fn;
use crate::register::geometry_binary_combine_fn;
use crate::register::geometry_binary_fn;
use crate::register::geometry_unary_combine_fn;
use crate::register::geometry_unary_fn;

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
            let row = builder.len();
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(row) {
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

    geo_convert_fn::<StringType, GeometryType>("st_geomfromgeohash", registry, |input| {
        if input.len() > 12 {
            return Err(ErrorCode::GeometryError(
                "Currently the precision only implement within 12 digits!",
            ));
        }
        let rect = decode_bbox(input).map_err(|_| ErrorCode::GeometryError("invalid geohash"))?;
        let geo = Geometry::from(rect);
        geo_to_ewkb(geo, None)
    });

    geo_convert_fn::<StringType, GeometryType>("st_geompointfromgeohash", registry, |input| {
        if input.len() > 12 {
            return Err(ErrorCode::GeometryError(
                "Currently the precision only implement within 12 digits!",
            ));
        }
        let rect = decode_bbox(input).map_err(|_| ErrorCode::GeometryError("invalid geohash"))?;
        let geo = Geometry::from(Point::from(rect.center()));
        geo_to_ewkb(geo, None)
    });

    registry
        .register_passthrough_nullable_2_arg::<NumberType<F64>, NumberType<F64>, GeometryType, _, _>(
        "st_makegeompoint",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<NumberType<F64>, NumberType<F64>, GeometryType>(
            |longitude, latitude, builder, ctx| {
                let row = builder.len();
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(row) {
                        builder.commit_row();
                        return;
                    }
                }
                let geo = Geometry::from(Point::new(longitude.0, latitude.0));
                match geo_to_ewkb(geo, None) {
                    Ok(binary) => builder.put_slice(binary.as_slice()),
                    Err(e) => ctx.set_error(row, e.to_string()),
                }
                builder.commit_row();
            },
        ),
    );

    geometry_unary_fn::<GeometryType>("st_makepolygon", registry, |geo, srid| match geo {
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
    });

    geometry_binary_fn::<GeometryType>("st_makeline", registry, |l, r, srid| {
        let geos = [l, r];
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
                    return Err(ErrorCode::GeometryError(format!(
                        "Geometry expression must be a Point, MultiPoint, or LineString, but got {}",
                        geometry_type_name(geo)
                    )));
                }
            }
        }
        let geo = Geometry::from(LineString::new(coords));
        geo_to_ewkb(geo, srid)
    });

    geo_convert_fn::<StringType, GeometryType>("st_geometryfromwkb", registry, |input| {
        let binary =
            hex::decode(input).map_err(|_| ErrorCode::GeometryError("invalid binary input"))?;
        ewkb_to_geo(&mut Ewkb(binary)).and_then(|(geo, srid)| geo_to_ewkb(geo, srid))
    });

    geo_convert_fn::<BinaryType, GeometryType>("st_geometryfromwkb", registry, |input| {
        ewkb_to_geo(&mut Ewkb(input)).and_then(|(geo, srid)| geo_to_ewkb(geo, srid))
    });

    geo_convert_with_arg_fn::<StringType, GeometryType>(
        "st_geometryfromwkb",
        registry,
        |input, srid| {
            let binary =
                hex::decode(input).map_err(|_| ErrorCode::GeometryError("invalid binary input"))?;
            ewkb_to_geo(&mut Ewkb(binary)).and_then(|(geo, _)| geo_to_ewkb(geo, Some(srid)))
        },
    );

    geo_convert_with_arg_fn::<BinaryType, GeometryType>(
        "st_geometryfromwkb",
        registry,
        |input, srid| {
            ewkb_to_geo(&mut Ewkb(input)).and_then(|(geo, _)| geo_to_ewkb(geo, Some(srid)))
        },
    );

    geo_convert_fn::<StringType, GeometryType>("st_geometryfromwkt", registry, |input| {
        geometry_from_ewkt(input, None)
    });

    geo_convert_with_arg_fn::<StringType, GeometryType>(
        "st_geometryfromwkt",
        registry,
        |input, srid| geometry_from_ewkt(input, Some(srid)),
    );

    geo_convert_fn::<StringType, GeometryType>("to_geometry", registry, |input| {
        geometry_from_str(input, None)
    });

    geo_convert_with_arg_fn::<StringType, GeometryType>("to_geometry", registry, |input, srid| {
        geometry_from_str(input, Some(srid))
    });

    geo_convert_fn::<BinaryType, GeometryType>("to_geometry", registry, |input| {
        ewkb_to_geo(&mut Ewkb(input)).and_then(|(geo, srid)| geo_to_ewkb(geo, srid))
    });

    geo_convert_with_arg_fn::<BinaryType, GeometryType>("to_geometry", registry, |input, srid| {
        ewkb_to_geo(&mut Ewkb(input)).and_then(|(geo, _)| geo_to_ewkb(geo, Some(srid)))
    });

    geo_convert_fn::<VariantType, GeometryType>("to_geometry", registry, |input| {
        json_to_geometry_impl(input, None).map_err(|e| ErrorCode::GeometryError(e.to_string()))
    });

    geo_convert_with_arg_fn::<VariantType, GeometryType>("to_geometry", registry, |input, srid| {
        json_to_geometry_impl(input, Some(srid))
            .map_err(|e| ErrorCode::GeometryError(e.to_string()))
    });

    geo_try_convert_fn::<VariantType, GeometryType>("try_to_geometry", registry, |input| {
        json_to_geometry_impl(input, None).map_err(|e| ErrorCode::GeometryError(e.to_string()))
    });

    geo_try_convert_with_arg_fn::<VariantType, GeometryType>(
        "try_to_geometry",
        registry,
        |input, srid| {
            json_to_geometry_impl(input, Some(srid))
                .map_err(|e| ErrorCode::GeometryError(e.to_string()))
        },
    );

    geo_try_convert_fn::<StringType, GeometryType>("try_to_geometry", registry, |input| {
        geometry_from_str(input, None)
    });

    geo_try_convert_with_arg_fn::<StringType, GeometryType>(
        "try_to_geometry",
        registry,
        |input, srid| geometry_from_str(input, Some(srid)),
    );

    geo_try_convert_fn::<BinaryType, GeometryType>("try_to_geometry", registry, |input| {
        ewkb_to_geo(&mut Ewkb(input)).and_then(|(geo, srid)| geo_to_ewkb(geo, srid))
    });

    geo_try_convert_with_arg_fn::<BinaryType, GeometryType>(
        "try_to_geometry",
        registry,
        |input, srid| {
            ewkb_to_geo(&mut Ewkb(input)).and_then(|(geo, _)| geo_to_ewkb(geo, Some(srid)))
        },
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, VariantType, _>(
        "st_asgeojson",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, VariantType>(|ewkb, builder, ctx| {
            let row = builder.len();
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(row) {
                    builder.commit_row();
                    return;
                }
            }

            let result = ewkb_to_geo(&mut Ewkb(ewkb))
                .and_then(|(geo, _)| geo_to_json(geo))
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

    geometry_unary_fn::<BinaryType>("st_asewkb", registry, |geo, srid| {
        let wkb = geo_to_ewkb(geo, srid)?;
        Ok(wkb)
    });

    geometry_unary_fn::<BinaryType>("st_aswkb", registry, |geo, _| {
        let wkb = geo_to_wkb(geo)?;
        Ok(wkb)
    });

    geometry_unary_fn::<StringType>("st_asewkt", registry, |geo, srid| {
        let wkb = geo_to_ewkt(geo, srid)?;
        Ok(wkb)
    });

    geometry_unary_fn::<StringType>("st_aswkt", registry, |geo, _| {
        let wkb = geo_to_wkt(geo)?;
        Ok(wkb)
    });

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _>(
        "to_string",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|ewkb, builder, ctx| {
            let row = builder.len();
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(row) {
                    builder.commit_row();
                    return;
                }
            }

            match geometry_format(ewkb, ctx.func_ctx.geometry_output_format) {
                Ok(data) => builder.put_str(&data),
                Err(e) => {
                    ctx.set_error(row, e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    geo_convert_fn::<GeometryType, StringType>("st_geohash", registry, |input| {
        point_to_geohash(input, None)
    });

    geo_convert_with_arg_fn::<GeometryType, StringType>(
        "st_geohash",
        registry,
        |input, precision| {
            if precision > 12 {
                return Err(ErrorCode::GeometryError(
                    "Currently the precision only implement within 12 digits!",
                ));
            }
            point_to_geohash(input, Some(precision))
        },
    );

    geometry_unary_fn::<NumberType<F64>>("st_area", registry, |geo, _| {
        let area = geo.unsigned_area();
        let area = (area * 1_000_000_000_f64).round() / 1_000_000_000_f64;
        Ok(area.into())
    });

    geometry_unary_combine_fn::<GeometryType>("st_envelope", registry, |geo, srid| {
        match geo.bounding_rect() {
            Some(rect) => {
                let value = Geometry::from(rect_to_polygon(rect));
                let ewkb = geo_to_ewkb(value, srid)?;
                Ok(Some(ewkb))
            }
            None => Ok(None),
        }
    });

    geometry_unary_combine_fn::<GeometryType>("st_centroid", registry, |geo, srid| {
        match geo.centroid() {
            Some(point) => {
                let value = Geometry::from(point);
                let ewkb = geo_to_ewkb(value, srid)?;
                Ok(Some(ewkb))
            }
            None => Ok(None),
        }
    });

    geometry_unary_combine_fn::<GeometryType>("st_startpoint", registry, |geo, srid| match geo {
        Geometry::LineString(line_string) => {
            let mut points = line_string.points();
            if let Some(point) = points.next() {
                let value = Geometry::from(point);
                let ewkb = geo_to_ewkb(value, srid)?;
                Ok(Some(ewkb))
            } else {
                Ok(None)
            }
        }
        _ => Err(ErrorCode::GeometryError(format!(
            "Type {} is not supported as argument to ST_ENDPOINT",
            geometry_type_name(&geo)
        ))),
    });

    geometry_unary_combine_fn::<GeometryType>("st_endpoint", registry, |geo, srid| match geo {
        Geometry::LineString(line_string) => {
            let mut points = line_string.points();
            if let Some(point) = points.next_back() {
                let value = Geometry::from(point);
                let ewkb = geo_to_ewkb(value, srid)?;
                Ok(Some(ewkb))
            } else {
                Ok(None)
            }
        }
        _ => Err(ErrorCode::GeometryError(format!(
            "Type {} is not supported as argument to ST_ENDPOINT",
            geometry_type_name(&geo)
        ))),
    });

    registry.register_combine_nullable_2_arg::<GeometryType, Int32Type, GeometryType, _, _>(
        "st_pointn",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, Int32Type, NullableType<GeometryType>>(
            |ewkb, index, builder, ctx| {
                let row = builder.len();
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(row) {
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
                        ctx.set_error(row, e.to_string());
                        builder.push_null();
                    }
                }
            },
        ),
    );

    geometry_unary_fn::<NumberType<F64>>("st_length", registry, |geo, _| {
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
        Ok(distance.into())
    });

    geometry_unary_fn::<NumberType<F64>>("st_x", registry, |geo, _| match geo {
        Geometry::Point(point) => Ok(AsPrimitive::<f64>::as_(point.x()).into()),
        _ => Err(ErrorCode::GeometryError(format!(
            "Type {} is not supported as argument to ST_X",
            geometry_type_name(&geo)
        ))),
    });

    geometry_unary_fn::<NumberType<F64>>("st_y", registry, |geo, _| match geo {
        Geometry::Point(point) => Ok(AsPrimitive::<f64>::as_(point.y()).into()),
        _ => Err(ErrorCode::GeometryError(format!(
            "Type {} is not supported as argument to ST_Y",
            geometry_type_name(&geo)
        ))),
    });

    geometry_unary_combine_fn::<NumberType<F64>>("st_xmax", registry, |geo, _| {
        match st_extreme(&geo, Axis::X, Extremum::Max) {
            Some(x_max) => Ok(Some(AsPrimitive::<f64>::as_(x_max).into())),
            None => Ok(None),
        }
    });

    geometry_unary_combine_fn::<NumberType<F64>>("st_xmin", registry, |geo, _| {
        match st_extreme(&geo, Axis::X, Extremum::Min) {
            Some(x_min) => Ok(Some(AsPrimitive::<f64>::as_(x_min).into())),
            None => Ok(None),
        }
    });

    geometry_unary_combine_fn::<NumberType<F64>>("st_ymax", registry, |geo, _| {
        match st_extreme(&geo, Axis::Y, Extremum::Max) {
            Some(y_max) => Ok(Some(AsPrimitive::<f64>::as_(y_max).into())),
            None => Ok(None),
        }
    });

    geometry_unary_combine_fn::<NumberType<F64>>("st_ymin", registry, |geo, _| {
        match st_extreme(&geo, Axis::Y, Extremum::Min) {
            Some(y_min) => Ok(Some(AsPrimitive::<f64>::as_(y_min).into())),
            None => Ok(None),
        }
    });

    geometry_unary_fn::<NumberType<u32>>("st_npoints", registry, |geo, _| {
        let npoints = count_points(&geo);
        Ok(npoints as u32)
    });

    geometry_unary_fn::<GeometryType>("st_convexhull", registry, |geo, srid| {
        let polygon = geo.convex_hull();
        let value = Geometry::from(polygon);
        let ewkb = geo_to_ewkb(value, srid)?;
        Ok(ewkb)
    });

    geometry_binary_fn::<BooleanType>("st_contains", registry, |l, r, _| Ok(l.contains(&r)));

    geometry_binary_fn::<BooleanType>("st_intersects", registry, |l, r, _| Ok(l.intersects(&r)));

    geometry_binary_fn::<BooleanType>("st_disjoint", registry, |l, r, _| Ok(!l.intersects(&r)));

    geometry_binary_fn::<BooleanType>("st_within", registry, |l, r, _| Ok(l.is_within(&r)));

    geometry_binary_fn::<BooleanType>("st_equals", registry, |l, r, _| {
        Ok(l.is_within(&r) && r.is_within(&l))
    });

    geometry_binary_fn::<NumberType<F64>>("st_distance", registry, |l, r, _| {
        let distance = Euclidean.distance(&l, &r);
        let distance = (distance * 1_000_000_000_f64).round() / 1_000_000_000_f64;
        Ok(distance.into())
    });

    geometry_binary_combine_fn::<GeometryType>("st_union", registry, |l, r, srid| {
        GeometryUnionAggOp::binary_compute(l, r)?
            .map(|value| geo_to_ewkb(value, srid))
            .transpose()
    });

    geometry_binary_combine_fn::<GeometryType>("st_intersection", registry, |l, r, srid| {
        GeometryIntersectionAggOp::binary_compute(l, r)?
            .map(|value| geo_to_ewkb(value, srid))
            .transpose()
    });

    geometry_binary_combine_fn::<GeometryType>("st_difference", registry, |l, r, srid| {
        GeometryDifferenceAggOp::binary_compute(l, r)?
            .map(|value| geo_to_ewkb(value, srid))
            .transpose()
    });

    geometry_binary_combine_fn::<GeometryType>("st_symdifference", registry, |l, r, srid| {
        GeometrySymDifferenceAggOp::binary_compute(l, r)?
            .map(|value| geo_to_ewkb(value, srid))
            .transpose()
    });

    geometry_unary_combine_fn::<NumberType<i32>>("st_dimension", registry, |geo, _| {
        let dimension: Option<i32> = match geo.dimensions() {
            Dimensions::Empty => None,
            Dimensions::ZeroDimensional => Some(0),
            Dimensions::OneDimensional => Some(1),
            Dimensions::TwoDimensional => Some(2),
        };
        Ok(dimension)
    });

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

                match Ewkb(ewkb)
                    .to_geo()
                    .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                    .and_then(|geo| geo_to_ewkb(geo, Some(srid)))
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

            let srid = read_srid(&mut Ewkb(ewkb)).unwrap_or_default();
            output.push(srid);
        }),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, Int32Type, GeometryType, _, _>(
        "st_transform",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, Int32Type, GeometryType>(
            |original, to_srid, builder, ctx| {
                let row = builder.len();
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(row) {
                        builder.commit_row();
                        return;
                    }
                }

                // All representations of the geo types supported by crates under the GeoRust organization, have not implemented srid().
                // Currently, the srid() of all types returns the default value `None`, so we need to parse it manually here.
                let Some(from_srid) = read_srid(&mut Ewkb(original)) else {
                    ctx.set_error(row, "input geometry must has the correct SRID".to_string());
                    builder.commit_row();
                    return;
                };
                match st_transform_impl(original, from_srid, to_srid) {
                    Ok(data) => {
                        builder.put_slice(data.as_slice());
                    }
                    Err(e) => {
                        ctx.set_error(row, e.to_string());
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
                    let row = builder.len();
                    if let Some(validity) = &ctx.validity {
                        if !validity.get_bit(row) {
                            builder.commit_row();
                            return;
                        }
                    }

                    match st_transform_impl(original, from_srid, to_srid) {
                        Ok(data) => {
                            builder.put_slice(data.as_slice());
                        }
                        Err(e) => {
                            ctx.set_error(row, e.to_string());
                        }
                    }
                    builder.commit_row();
                },
            ),
        );

    geometry_unary_combine_fn::<NumberType<u64>>("st_hilbert", registry, |geo, _| {
        match geometry_bbox_center(&geo) {
            Some((x, y)) => {
                let index = hilbert_index_from_point(x, y)?;
                Ok(Some(index))
            }
            None => Ok(None),
        }
    });

    registry.register_combine_nullable_2_arg::<
        GeometryType,
        ArrayType<NumberType<F64>>,
        UInt64Type,
        _,
        _,
    >(
        "st_hilbert",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<
            GeometryType,
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

            let result = Ewkb(ewkb).to_geo().map(|geo| geometry_bbox_center(&geo));
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
            if from_srid == GEOGRAPHY_SRID {
                geom.to_radians_in_place();
            }
            if transform(&from_proj, &to_proj, &mut geom).is_err() {
                return Err(ErrorCode::GeometryError(format!(
                    "transform from {} to {} failed",
                    from_srid, to_srid
                )));
            }
            if to_srid == GEOGRAPHY_SRID {
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
