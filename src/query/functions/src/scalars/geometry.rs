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

use std::io;

use databend_common_exception::ErrorCode;
use databend_common_expression::types::geometry::GeometryType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::F64;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_io::geometry_format;
use databend_common_io::parse_to_ewkb;
use databend_common_io::parse_to_subtype;
use databend_common_io::read_ewkb_srid;
use databend_common_io::GeometryDataType;
use geo::dimensions::Dimensions;
use geo::HasDimensions;
use geo::MultiPoint;
use geo::Point;
use geo_types::Polygon;
use geohash::decode_bbox;
use geohash::encode;
use geos::geo_types;
use geos::geo_types::Coord;
use geos::geo_types::LineString;
use geos::Geom;
use geos::Geometry;
use geozero::geojson::GeoJson;
use geozero::wkb::Ewkb;
use geozero::wkb::Wkb;
use geozero::CoordDimensions;
use geozero::ToGeo;
use geozero::ToJson;
use geozero::ToWkb;
use geozero::ToWkt;
use jsonb::parse_value;
use jsonb::to_string;

pub fn register(registry: &mut FunctionRegistry) {
    // aliases
    registry.register_aliases("st_aswkb", &["st_asbinary"]);
    registry.register_aliases("st_aswkt", &["st_astext"]);
    registry.register_aliases("st_makegeompoint", &["st_geom_point"]);
    registry.register_aliases("st_makepolygon", &["st_polygon"]);
    registry.register_aliases("st_makeline", &["st_make_line"]);
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
    registry.register_passthrough_nullable_1_arg::<GeometryType, VariantType, _, _>(
        "st_asgeojson",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, VariantType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let json = ewkb_to_json(geometry);
            match parse_value(json.unwrap().as_bytes()) {
                Ok(json) => {
                    json.write_to_vec(&mut builder.data);
                }
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                }
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, BinaryType, _, _>(
        "st_asewkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, BinaryType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let srid = match read_ewkb_srid(&mut io::Cursor::new(&geometry)) {
                Ok(srid) => srid,
                _ => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError("input geometry must has the correct SRID")
                            .to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match Ewkb(geometry).to_ewkb(CoordDimensions::xy(), srid) {
                Ok(wkb) => builder.put_slice(wkb.as_slice()),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                }
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, BinaryType, _, _>(
        "st_aswkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, BinaryType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match Ewkb(geometry).to_wkb(CoordDimensions::xy()) {
                Ok(wkb) => builder.put_slice(wkb.as_slice()),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                }
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _, _>(
        "st_asewkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let srid = match read_ewkb_srid(&mut io::Cursor::new(&geometry)) {
                Ok(srid) => srid,
                _ => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError("input geometry must has the correct SRID")
                            .to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match Ewkb(geometry).to_ewkt(srid) {
                Ok(ewkt) => builder.put_str(&ewkt),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _, _>(
        "st_aswkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match Ewkb(geometry).to_wkt() {
                Ok(wkt) => builder.put_str(&wkt),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, GeometryType, _, _>(
        "st_endpoint",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, GeometryType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let geo: geo_types::Geometry = match Ewkb(geometry).to_geo() {
                Ok(geo) => geo,
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            let point = match <geo_types::Geometry as TryInto<LineString>>::try_into(geo) {
                Ok(line_string) => line_string.points().last().unwrap(),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match geo_types::Geometry::from(point).to_wkb(CoordDimensions::xy()) {
                Ok(binary) => builder.put_slice(binary.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };

            builder.commit_row();
        }),

    registry.register_combine_nullable_1_arg::<GeometryType, Int32Type, _, _>(
        "st_dimension",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<Int32Type>>(
            |ewkb, output, ctx| {
                let geo: geo_types::Geometry = match Ewkb(ewkb).to_geo() {
                    Ok(geo) => geo,
                    Err(e) => {
                        ctx.set_error(
                            output.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
                        output.push_null();
                        return;
                    }
                };

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
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _, _>(
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
                    "Currently the precision only implement within 12 digits!",
                );
                builder.commit_row();
                return;
            }

            let geo: geo_types::Geometry = match decode_bbox(geohash) {
                Ok(rect) => rect.into(),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match geo.to_wkb(CoordDimensions::xy()) {
                Ok(binary) => builder.put_slice(binary.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _, _>(
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
                    ErrorCode::GeometryError(
                        "Currently the precision only implement within 12 digits!".to_string(),
                    )
                    .to_string(),
                );
                builder.commit_row();
                return;
            }

            let geo: geo_types::Geometry = match decode_bbox(geohash) {
                Ok(rect) => Point::from(rect.center()).into(),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match geo.to_wkb(CoordDimensions::xy()) {
                Ok(binary) => builder.put_slice(binary.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<NumberType<F64>, NumberType<F64>, GeometryType, _, _>(
        "st_makegeompoint",
        |_,_, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<NumberType<F64>, NumberType<F64>, GeometryType>(|longitude, latitude, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            let geom = geo::Geometry::from(Point::new(longitude.0, latitude.0));
            match geom.to_wkb(CoordDimensions::xy()) {
                Ok(data) => {
                    builder.put_slice(data.as_slice())
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string())
                }
            }
            builder.commit_row();
        })
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, GeometryType, _, _>(
        "st_makepolygon",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, GeometryType>(|wkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let line_string = match Wkb(wkb).to_geo() {
                Ok(geo) => geo.try_into(),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            let polygon = line_string
                .map_err(|e: geo_types::Error| ErrorCode::GeometryError(e.to_string()))
                .and_then(|line_string: LineString| {
                    let points = line_string.into_points();
                    if points.len() < 4 {
                        Err(ErrorCode::GeometryError(
                            "Input lines must have at least 4 points!",
                        ))
                    } else if points.last() != points.first() {
                        Err(ErrorCode::GeometryError(
                            "The first and last elements are not equal.",
                        ))
                    } else {
                        geo_types::Geometry::from(Polygon::new(LineString::from(points), vec![]))
                            .to_wkb(CoordDimensions::xy())
                            .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                    }
                });

            match polygon {
                Ok(p) => builder.put_slice(p.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, GeometryType, GeometryType, _, _>(
        "st_makeline",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, GeometryType>(
            |left_ewkb, right_ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }
                let srid: Option<i32>;
                let params = &vec![left_ewkb, right_ewkb];
                let geos: Vec<Geometry> =
                    match binary_to_geos(params)
                    {
                        Ok(geos) => {
                            match get_shared_srid(&geos){
                                Ok(s) => {
                                    srid = s;
                                    geos
                                },
                                Err(e) => {
                                    ctx.set_error(builder.len(), ErrorCode::GeometryError(e).to_string());
                                    builder.commit_row();
                                    return;
                                }
                            }
                        },
                        Err(e) => {
                            ctx.set_error(builder.len(), ErrorCode::GeometryError(e.to_string()).to_string());
                            builder.commit_row();
                            return;
                        }
                    };

                let mut coords: Vec<Coord> = vec![];
                for geometry in geos.into_iter() {
                    let g : geo_types::Geometry = (&geometry).try_into().unwrap();
                    match g {
                        geo_types::Geometry::Point(_) => {
                            let point: Point = match g.try_into() {
                                Ok(point) => point,
                                Err(e) => {
                                    ctx.set_error(builder.len(), ErrorCode::GeometryError(e.to_string()).to_string());
                                    builder.commit_row();
                                    return;
                                }
                            };
                            coords.push(point.into());
                        },
                        geo_types::Geometry::LineString(_)=> {
                            let line: LineString = match g.try_into() {
                                Ok(line) => line,
                                Err(e) => {
                                    ctx.set_error(builder.len(), ErrorCode::GeometryError(e.to_string()).to_string());
                                    builder.commit_row();
                                    return;
                                }
                            };
                            coords.append(&mut line.into_inner());
                        },
                        geo_types::Geometry::MultiPoint(_)=> {
                            let multi_point: MultiPoint = match g.try_into() {
                                Ok(multi_point) => multi_point,
                                Err(e) => {
                                    ctx.set_error(builder.len(), ErrorCode::GeometryError(e.to_string()).to_string());
                                    builder.commit_row();
                                    return;
                                }
                            };
                            for point in multi_point.into_iter() {
                                coords.push(point.into());
                            }
                        },
                        _ => {
                            ctx.set_error(
                                builder.len(),
                                ErrorCode::GeometryError("Geometry expression must be a Point, MultiPoint, or LineString.").to_string(),
                            );
                            builder.commit_row();
                            return;
                        }
                    }
                }
                let geom = geo::Geometry::from(LineString::new(coords));
                match geom.to_ewkb(CoordDimensions::xy(), srid) {
                    Ok(data) => builder.put_slice(data.as_slice()),
                    Err(e) => ctx.set_error(builder.len(), e.to_string()),
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _, _>(
        "st_geohash",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match point_to_geohash(geometry, None) {
                Ok(hash) => builder.put_str(&hash),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
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
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
                    }
                };
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|str, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let binary = match hex::decode(str) {
                Ok(binary) => binary,
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            let srid = match read_ewkb_srid(&mut io::Cursor::new(&binary)) {
                Ok(srid) => srid,
                _ => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError("input geometry must has the correct SRID")
                            .to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match Ewkb(binary).to_ewkb(CoordDimensions::xy(), srid) {
                Ok(ewkb) => {
                    builder.put_slice(ewkb.as_slice());
                }
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BinaryType, GeometryType>(|binary, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match binary_to_geometry_impl(binary, None) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            }
            builder.commit_row();
        }),
    );
    registry.register_passthrough_nullable_2_arg::<StringType, Int32Type, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, Int32Type, GeometryType>(
            |str, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                let binary = match hex::decode(str) {
                    Ok(binary) => binary,
                    Err(e) => {
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
                        builder.commit_row();
                        return;
                    }
                };

                match Ewkb(binary).to_ewkb(CoordDimensions::xy(), Some(srid)) {
                    Ok(ewkb) => {
                        builder.put_slice(ewkb.as_slice());
                    }
                    Err(e) => {
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
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
                match binary_to_geometry_impl(binary, Some(srid)) {
                    Ok(data) => builder.put_slice(data.as_slice()),
                    Err(e) => ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    ),
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _, _>(
        "st_geometryfromwkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|wkt, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            match parse_to_ewkb(wkt.as_bytes(), None) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
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
                match parse_to_ewkb(wkt.as_bytes(), Some(srid)) {
                    Ok(data) => builder.put_slice(data.as_slice()),
                    Err(e) => ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    ),
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|b, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match geometry_format(Ewkb(b), ctx.func_ctx.geometry_output_format) {
                Ok(data) => builder.put_str(&data),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _, _>(
        "to_geometry",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|str, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            match str_to_geometry_impl(str, None) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, Int32Type, GeometryType, _, _>(
        "to_geometry",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, Int32Type, GeometryType>(
            |str, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }
                match str_to_geometry_impl(str, Some(srid)) {
                    Ok(data) => builder.put_slice(data.as_slice()),
                    Err(e) => ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    ),
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, GeometryType, _, _>(
        "to_geometry",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BinaryType, GeometryType>(|binary, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            let srid = match read_ewkb_srid(&mut io::Cursor::new(&binary)) {
                Ok(srid) => srid,
                _ => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError("input geometry must has the correct SRID")
                            .to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match binary_to_geometry_impl(binary, srid) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
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

                match binary_to_geometry_impl(binary, Some(srid)) {
                    Ok(data) => builder.put_slice(data.as_slice()),
                    Err(e) => ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    ),
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, GeometryType, _, _>(
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
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
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
                    Err(e) => ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    ),
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
                        output.validity.push(true);
                        output.builder.put_slice(data.as_slice());
                        output.builder.commit_row();
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
                        output.validity.push(true);
                        output.builder.put_slice(data.as_slice());
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<StringType, GeometryType, _, _>(
        "try_to_geometry",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, NullableType<GeometryType>>(
            |str, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match str_to_geometry_impl(str, None) {
                    Ok(data) => {
                        output.validity.push(true);
                        output.builder.put_slice(data.as_slice());
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<StringType, Int32Type, GeometryType, _, _>(
        "try_to_geometry",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, Int32Type, NullableType<GeometryType>>(
            |str, srid, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match str_to_geometry_impl(str, Some(srid)) {
                    Ok(data) => {
                        output.validity.push(true);
                        output.builder.put_slice(data.as_slice());
                        output.builder.commit_row();
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
                let srid = match read_ewkb_srid(&mut io::Cursor::new(&binary)) {
                    Ok(srid) => srid,
                    Err(_) => {
                        output.push_null();
                        return;
                    }
                };

                match binary_to_geometry_impl(binary, srid) {
                    Ok(data) => {
                        output.validity.push(true);
                        output.builder.put_slice(data.as_slice());
                        output.builder.commit_row();
                    }
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

                match binary_to_geometry_impl(binary, Some(srid)) {
                    Ok(data) => {
                        output.validity.push(true);
                        output.builder.put_slice(data.as_slice());
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    // registry.register_passthrough_nullable_2_arg::<GeometryType, Int32Type, GeometryType, _, _>(
    //     "st_transform",
    //     |_, _, _| FunctionDomain::MayThrow,
    //     vectorize_with_builder_2_arg::<GeometryType, Int32Type, GeometryType>(
    //         |original, srid, builder, ctx| {
    //             if let Some(validity) = &ctx.validity {
    //                 if !validity.get_bit(builder.len()) {
    //                     builder.commit_row();
    //                     return;
    //                 }
    //             }
    //
    //             #[allow(unused_assignments)]
    //             let mut from_srid = 0;
    //
    //             // All representations of the geo types supported by crates under the GeoRust organization, have not implemented srid().
    //             // Currently, the srid() of all types returns the default value `None`, so we need to parse it manually here.
    //             match read_ewkb_srid(&mut std::io::Cursor::new(original)) {
    //                 Ok(srid) if srid.is_some() => from_srid = srid.unwrap(),
    //                 _ => {
    //                     ctx.set_error(
    //                         builder.len(),
    //                         ErrorCode::GeometryError(" input geometry must has the correct SRID")
    //                             .to_string(),
    //                     );
    //                     builder.commit_row();
    //                     return;
    //                 }
    //             }
    //
    //             let result = {
    //                 Ewkb(original).to_geo().map_err(ErrorCode::from).and_then(
    //                     |mut geom: Geometry| {
    //                         Proj::new_known_crs(&make_crs(from_srid), &make_crs(srid), None)
    //                             .map_err(|e| ErrorCode::GeometryError(e.to_string()))
    //                             .and_then(|proj| {
    //                                 geom.transform(&proj)
    //                                     .map_err(|e| ErrorCode::GeometryError(e.to_string()))
    //                                     .and_then(|_| {
    //                                         geom.to_ewkb(geom.dims(), Some(srid))
    //                                             .map_err(ErrorCode::from)
    //                                     })
    //                             })
    //                     },
    //                 )
    //             };
    //
    //             match result {
    //                 Ok(data) => {
    //                     builder.put_slice(data.as_slice());
    //                 }
    //                 Err(e) => {
    //                     ctx.set_error(builder.len(), e.to_string());
    //                 }
    //             }
    //
    //             builder.commit_row();
    //         },
    //     ),
    // );
    //
    // registry.register_passthrough_nullable_3_arg::<GeometryType, Int32Type, Int32Type, GeometryType, _, _>(
    //     "st_transform",
    //     |_, _, _,_| FunctionDomain::MayThrow,
    //     vectorize_with_builder_3_arg::<GeometryType, Int32Type,Int32Type, GeometryType>(
    //         |original, from_srid, to_srid, builder, ctx| {
    //             if let Some(validity) = &ctx.validity {
    //                 if !validity.get_bit(builder.len()) {
    //                     builder.commit_row();
    //                     return;
    //                 }
    //             }
    //
    //             let result = {
    //                 Proj::new_known_crs(&make_crs(from_srid), &make_crs(to_srid), None)
    //                     .map_err(|e| ErrorCode::GeometryError(e.to_string()))
    //                     .and_then(|proj| {
    //                     let old = Ewkb(original.to_vec());
    //                     Ewkb(old.to_ewkb(old.dims(), Some(from_srid)).unwrap()).to_geo().map_err(ErrorCode::from).and_then(|mut geom| {
    //                         geom.transform(&proj).map_err(|e|ErrorCode::GeometryError(e.to_string())).and_then(|_| {
    //                             geom.to_ewkb(old.dims(), Some(to_srid)).map_err(ErrorCode::from)
    //                         })
    //                     })
    //                 })
    //             };
    //             match result {
    //                 Ok(data) => {
    //                     builder.put_slice(data.as_slice());
    //                 }
    //                 Err(e) => {
    //                     ctx.set_error(builder.len(), e.to_string());
    //                 }
    //             }
    //
    //             builder.commit_row();
    //         },
    //     ),
    // );
}

// fn make_crs(srid: i32) -> String {
//     format!("EPSG:{}", srid)
// }

#[inline]
fn binary_to_geos<'a>(binaries: &'a Vec<&'a [u8]>) -> Result<Vec<Geometry<'a>>, String> {
    let mut geos: Vec<Geometry> = Vec::with_capacity(binaries.len());
    let mut srid: Option<i32> = None;
    for (index, binary) in binaries.iter().enumerate() {
        match Geometry::new_from_wkb(binary) {
            Ok(data) => {
                if index == 0 {
                    srid = data.get_srid().map_or_else(|_| None, |v| Some(v as i32));
                } else {
                    let t_srid = data.get_srid().map_or_else(|_| None, |v| Some(v as i32));
                    if !srid.eq(&t_srid) {
                        return Err("Srid does not match!".to_string());
                    }
                }
                geos.push(data)
            }
            Err(e) => return Err(e.to_string()),
        };
    }
    Ok(geos)
}

#[inline]
fn get_shared_srid(geometries: &Vec<Geometry>) -> Result<Option<i32>, String> {
    let mut srid: Option<i32> = None;
    let mut error_srid: String = String::new();
    let check_srid = geometries.windows(2).all(|w| {
        let v1 = w[0].get_srid().map_or_else(|_| None, |v| Some(v as i32));
        let v2 = w[1].get_srid().map_or_else(|_| None, |v| Some(v as i32));
        match v1.eq(&v2) {
            true => {
                srid = v1;
                true
            }
            false => {
                error_srid = "Srid does not match!".to_string();
                false
            }
        }
    });
    match check_srid {
        true => Ok(srid),
        false => Err(error_srid.clone()),
    }
}

pub fn ewkb_to_json(buf: &[u8]) -> databend_common_exception::Result<String> {
    Ewkb(buf)
        .to_geo()
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
        .and_then(|geo| {
            geo.to_json()
                .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                .map(|json: String| json)
        })
}

fn binary_to_geometry_impl(
    binary: &[u8],
    srid: Option<i32>,
) -> databend_common_exception::Result<Vec<u8>> {
    let ewkb_srid = match read_ewkb_srid(&mut io::Cursor::new(&binary)) {
        Ok(srid) => srid,
        Err(e) => return Err(ErrorCode::GeometryError(e.to_string())),
    };
    match Ewkb(&binary).to_ewkb(CoordDimensions::xy(), srid.or(ewkb_srid)) {
        Ok(ewkb) => Ok(ewkb),
        Err(e) => Err(ErrorCode::GeometryError(e.to_string())),
    }
}

/// The argument str must be a string expression that represents a valid geometric object in one of the following formats:
///
/// WKT (well-known text).
/// WKB (well-known binary) in hexadecimal format (without a leading 0x).
/// EWKT (extended well-known text).
/// EWKB (extended well-known binary) in hexadecimal format (without a leading 0x).
/// GEOJSON
fn str_to_geometry_impl(
    str: &str,
    srid: Option<i32>,
) -> databend_common_exception::Result<Vec<u8>> {
    let geo_type = match parse_to_subtype(str.as_bytes()) {
        Ok(geo_types) => geo_types,
        Err(e) => return Err(ErrorCode::GeometryError(e.to_string())),
    };

    let ewkb = match geo_type {
        GeometryDataType::WKT | GeometryDataType::EWKT => parse_to_ewkb(str.as_bytes(), srid),
        GeometryDataType::GEOJSON => GeoJson(str)
            .to_ewkb(CoordDimensions::xy(), srid)
            .map_err(|e| ErrorCode::GeometryError(e.to_string())),
        GeometryDataType::WKB | GeometryDataType::EWKB => {
            let binary = match hex::decode(str) {
                Ok(binary) => binary,
                Err(e) => return Err(ErrorCode::GeometryError(e.to_string())),
            };

            let ewkb_srid = match read_ewkb_srid(&mut io::Cursor::new(&binary)) {
                Ok(ewkb_srid) => ewkb_srid,
                Err(e) => return Err(ErrorCode::GeometryError(e.to_string())),
            };
            Ewkb(binary)
                .to_ewkb(CoordDimensions::xy(), srid.or(ewkb_srid))
                .map_err(|e| ErrorCode::GeometryError(e.to_string()))
        }
    };

    match ewkb {
        Ok(data) => Ok(data),
        Err(e) => Err(ErrorCode::GeometryError(e.to_string())),
    }
}

fn json_to_geometry_impl(
    binary: &[u8],
    srid: Option<i32>,
) -> databend_common_exception::Result<Vec<u8>> {
    let s = to_string(binary);
    let json = GeoJson(s.as_str());
    match json.to_ewkb(CoordDimensions::xy(), srid) {
        Ok(data) => Ok(data),
        Err(e) => Err(ErrorCode::GeometryError(e.to_string())),
    }
}

fn point_to_geohash(
    geometry: &[u8],
    precision: Option<i32>,
) -> databend_common_exception::Result<String> {
    let point = match Ewkb(geometry).to_geo() {
        Ok(geo) => Point::try_from(geo),
        Err(e) => return Err(ErrorCode::GeometryError(e.to_string())),
    };

    let hash = match point {
        Ok(point) => encode(point.0, precision.map_or(12, |p| p as usize)),
        Err(e) => return Err(ErrorCode::GeometryError(e.to_string())),
    };
    match hash {
        Ok(hash) => Ok(hash),
        Err(e) => Err(ErrorCode::GeometryError(e.to_string())),
    }
}
