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

// Portions of this file are adapted from PostGIS:
// - postgis/geography_measurement.c
// Licensed under GPL-2.0-or-later.
// Modifications by Databend.

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use geo::BoundingRect;
use geo::Contains;
use geo::Coord;
use geo::CoordsIter;
use geo::Geometry;
use geo::GeometryCollection;
use geo::Intersects;
use geo::Line;
use geo::LineString;
use geo::MultiLineString;
use geo::MultiPoint;
use geo::MultiPolygon;
use geo::Point;
use geo::Polygon;
use geo::Rect;
use geo::ToDegrees;
use geo::ToRadians;
use geo::Triangle;
use geo::algorithm::bool_ops::BooleanOps;
use geo::algorithm::bool_ops::unary_union;
use geo::algorithm::buffer::Buffer;
use geo::algorithm::line_intersection::LineIntersection;
use geo::algorithm::line_intersection::line_intersection;
use proj4rs::Proj;
use proj4rs::transform::transform;

use crate::geographic::gbox::Gbox;
use crate::geographic::gbox::gbox_angular_height;
use crate::geographic::gbox::gbox_angular_width;
use crate::geographic::gbox::gbox_centroid;
use crate::geographic::gbox::gbox_union;
use crate::geographic::gbox::geocentric_bounds_from_geo;
use crate::geographic::gbox::geocentric_bounds_from_geos;

const LINE_MERGE_EPS: f64 = 1e-9;
pub const MIN_BUFFER_DISTANCE: f64 = 1e-6;
pub const BUFFER_DISTANCE_SCALE: f64 = 1e-9;

pub const GEOGRAPHY_SRID: i32 = 4326;

pub enum BestSrid {
    Epsg(u16),
    Proj(String),
}

impl BestSrid {
    pub fn to_proj(&self) -> Result<Proj> {
        match self {
            BestSrid::Epsg(code) => Proj::from_epsg_code(*code)
                .map_err(|_| ErrorCode::GeometryError("invalid best srid".to_string())),
            BestSrid::Proj(def) => Proj::from_proj_string(def)
                .map_err(|_| ErrorCode::GeometryError("invalid best srid".to_string())),
        }
    }
}

fn geography_best_srid_from_gbox(gbox: &Gbox) -> Result<BestSrid> {
    // Mirrors PostGIS geography_bestsrid (geography_measurement.c):
    // - SRID_NORTH_LAMBERT: north polar LAEA (lat_0=90, lon_0=-40)
    // - SRID_SOUTH_LAMBERT: south polar LAEA (lat_0=-90, lon_0=0)
    // - SRID_NORTH_UTM_START / SRID_SOUTH_UTM_START: UTM zones (EPSG 326xx/327xx)
    // - SRID_LAEA_START: mid-latitude LAEA tiles (Proj string built from lon_0/lat_0)
    // - SRID_WORLD_MERCATOR: fallback world Mercator (Proj string)
    let (center_x, center_y) = gbox_centroid(gbox);
    let xwidth = 180.0 * gbox_angular_width(gbox) / std::f64::consts::PI;
    let ywidth = 180.0 * gbox_angular_height(gbox) / std::f64::consts::PI;

    let best = if center_y > 70.0 && ywidth < 45.0 {
        // PostGIS SRID_NORTH_LAMBERT.
        BestSrid::Proj(
            "+proj=laea +lat_0=90 +lon_0=-40 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
                .to_string(),
        )
    } else if center_y < -70.0 && ywidth < 45.0 {
        // PostGIS SRID_SOUTH_LAMBERT.
        BestSrid::Proj(
            "+proj=laea +lat_0=-90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
                .to_string(),
        )
    } else if xwidth < 6.0 {
        // PostGIS SRID_NORTH_UTM_START / SRID_SOUTH_UTM_START (EPSG 326xx / 327xx).
        let mut zone = ((center_x + 180.0) / 6.0).floor() as i32;
        if zone > 59 {
            zone = 59;
        }
        let utm_zone = zone + 1;
        let epsg = if center_y < 0.0 {
            32700 + utm_zone
        } else {
            32600 + utm_zone
        };
        BestSrid::Epsg(
            u16::try_from(epsg).map_err(|_| ErrorCode::GeometryError("invalid UTM zone"))?,
        )
    } else if ywidth < 25.0 {
        let yzone = 3 + (center_y / 30.0).floor() as i32;
        let mut xzone = -1;
        let (xbase, xwidth_deg) = if (yzone == 2 || yzone == 3) && xwidth < 30.0 {
            xzone = 6 + (center_x / 30.0).floor() as i32;
            (6, 30.0)
        } else if (yzone == 1 || yzone == 4) && xwidth < 45.0 {
            xzone = 4 + (center_x / 45.0).floor() as i32;
            (4, 45.0)
        } else if (yzone == 0 || yzone == 5) && xwidth < 90.0 {
            xzone = 2 + (center_x / 90.0).floor() as i32;
            (2, 90.0)
        } else {
            (0, 0.0)
        };

        if xzone != -1 {
            // PostGIS SRID_LAEA_START + tile (we use the equivalent Proj string).
            let tile = xzone - xbase;
            let lon_0 = (tile as f64 + 0.5) * xwidth_deg;
            let lat_0 = -75.0 + (yzone as f64) * 30.0;
            BestSrid::Proj(format!(
                "+proj=laea +lat_0={lat_0} +lon_0={lon_0} +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
            ))
        } else {
            // PostGIS SRID_WORLD_MERCATOR.
            BestSrid::Proj(
                "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs".to_string(),
            )
        }
    } else {
        // PostGIS SRID_WORLD_MERCATOR.
        BestSrid::Proj(
            "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs".to_string(),
        )
    };

    let _ = best.to_proj()?;
    Ok(best)
}

pub fn geography_best_srid(
    left: &Geometry<f64>,
    right: Option<&Geometry<f64>>,
) -> Result<Option<BestSrid>> {
    let left_box = geocentric_bounds_from_geo(left)?;
    let right_box = match right {
        Some(geo) => geocentric_bounds_from_geo(geo)?,
        None => None,
    };
    let gbox = match (left_box, right_box) {
        (None, None) => return Ok(None),
        (Some(gbox), None) | (None, Some(gbox)) => gbox,
        (Some(left), Some(right)) => gbox_union(&left, &right),
    };
    Ok(Some(geography_best_srid_from_gbox(&gbox)?))
}

pub fn geography_best_srid_for_geos(geos: &[Geometry<f64>]) -> Result<Option<BestSrid>> {
    let gbox = match geocentric_bounds_from_geos(geos)? {
        Some(gbox) => gbox,
        None => return Ok(None),
    };
    Ok(Some(geography_best_srid_from_gbox(&gbox)?))
}

fn wgs84_proj() -> Result<Proj> {
    Proj::from_epsg_code(
        u16::try_from(GEOGRAPHY_SRID).map_err(|_| ErrorCode::GeometryError("invalid srid"))?,
    )
    .map_err(|_| ErrorCode::GeometryError("invalid srid".to_string()))
}

const GEOGRAPHY_ROUND_SCALE: f64 = 1_000_000_000.0;

fn round_coordinate(value: f64) -> f64 {
    let rounded = (value * GEOGRAPHY_ROUND_SCALE).round() / GEOGRAPHY_ROUND_SCALE;
    if rounded == 0.0 { 0.0 } else { rounded }
}

fn round_coord(coord: Coord<f64>) -> Coord<f64> {
    Coord {
        x: round_coordinate(coord.x),
        y: round_coordinate(coord.y),
    }
}

fn round_line_string(line: &LineString<f64>) -> LineString<f64> {
    let coords: Vec<Coord<f64>> = line.0.iter().map(|c| round_coord(*c)).collect();
    LineString::from(coords)
}

fn round_polygon(polygon: &Polygon<f64>) -> Polygon<f64> {
    let exterior = round_line_string(polygon.exterior());
    let interiors = polygon.interiors().iter().map(round_line_string).collect();
    Polygon::new(exterior, interiors)
}

fn round_geometry_coords(geom: Geometry<f64>) -> Geometry<f64> {
    match geom {
        Geometry::Point(point) => Geometry::Point(Point::new(
            round_coordinate(point.x()),
            round_coordinate(point.y()),
        )),
        Geometry::Line(line) => {
            Geometry::Line(Line::new(round_coord(line.start), round_coord(line.end)))
        }
        Geometry::LineString(line) => Geometry::LineString(round_line_string(&line)),
        Geometry::Polygon(polygon) => Geometry::Polygon(round_polygon(&polygon)),
        Geometry::MultiPoint(multi_point) => {
            let points = multi_point
                .0
                .iter()
                .map(|p| Point::new(round_coordinate(p.x()), round_coordinate(p.y())));
            Geometry::MultiPoint(MultiPoint::from_iter(points))
        }
        Geometry::MultiLineString(multi_line) => {
            let lines = multi_line.0.iter().map(round_line_string);
            Geometry::MultiLineString(MultiLineString::from_iter(lines))
        }
        Geometry::MultiPolygon(multi_polygon) => {
            let polys = multi_polygon.0.iter().map(round_polygon);
            Geometry::MultiPolygon(MultiPolygon::from_iter(polys))
        }
        Geometry::GeometryCollection(collection) => {
            let geoms = collection
                .0
                .into_iter()
                .map(round_geometry_coords)
                .collect::<Vec<_>>();
            Geometry::GeometryCollection(GeometryCollection::from_iter(geoms))
        }
        Geometry::Rect(rect) => {
            let min = round_coord(rect.min());
            let max = round_coord(rect.max());
            let min_x = min.x.min(max.x);
            let min_y = min.y.min(max.y);
            let max_x = min.x.max(max.x);
            let max_y = min.y.max(max.y);
            Geometry::Rect(Rect::new(Coord { x: min_x, y: min_y }, Coord {
                x: max_x,
                y: max_y,
            }))
        }
        Geometry::Triangle(triangle) => Geometry::Triangle(Triangle::new(
            round_coord(triangle.0),
            round_coord(triangle.1),
            round_coord(triangle.2),
        )),
    }
}

fn transform_geometry_with_proj(
    mut geom: Geometry<f64>,
    from_proj: &Proj,
    to_proj: &Proj,
    from_is_geography: bool,
    to_is_geography: bool,
) -> Result<Geometry<f64>> {
    if from_is_geography {
        geom.to_radians_in_place();
    }
    if transform(from_proj, to_proj, &mut geom).is_err() {
        return Err(ErrorCode::GeometryError("transform failed".to_string()));
    }
    if to_is_geography {
        geom.to_degrees_in_place();
    }
    Ok(geom)
}

pub fn project_geography_to_best(geo: Geometry<f64>, best: &BestSrid) -> Result<Geometry<f64>> {
    let from_proj = wgs84_proj()?;
    let to_proj = best.to_proj()?;
    transform_geometry_with_proj(geo, &from_proj, &to_proj, true, false)
}

pub fn unproject_geometry_from_best(geo: Geometry<f64>, best: &BestSrid) -> Result<Geometry<f64>> {
    let from_proj = best.to_proj()?;
    let to_proj = wgs84_proj()?;
    let geom = transform_geometry_with_proj(geo, &from_proj, &to_proj, false, true)?;
    Ok(round_geometry_coords(geom))
}
