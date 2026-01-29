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
use geo::Closest;
use geo::CoordsIter;
use geo::Distance;
use geo::Geometry;
use geo::Haversine;
use geo::HaversineClosestPoint;
use geo::Point;
use geozero::ToGeo;
use geozero::ToWkb;
use geozero::geojson::GeoJson;
use geozero::wkb::Ewkb;

use crate::ewkb_to_geo;
use crate::geometry::ewkt_str_to_geo;

pub const LONGITUDE_MIN: f64 = -180.0;
pub const LONGITUDE_MAX: f64 = 180.0;
pub const LATITUDE_MIN: f64 = -90.0;
pub const LATITUDE_MAX: f64 = 90.0;

pub const GEOGRAPHY_SRID: i32 = 4326;

pub fn geography_from_ewkt_bytes(ewkt: &[u8]) -> Result<Vec<u8>> {
    let s = std::str::from_utf8(ewkt).map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
    geography_from_ewkt(s)
}

/// Parses an EWKT input and returns a value of EWKB Geography.
pub fn geography_from_ewkt(input: &str) -> Result<Vec<u8>> {
    let input = input.trim();
    let (geo, parsed_srid) = ewkt_str_to_geo(input)?;
    check_srid(parsed_srid)?;
    geo.coords_iter().try_for_each(|c| check_point(c.x, c.y))?;
    geo.to_ewkb(geozero::CoordDimensions::xy(), None)
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
}

pub fn geography_from_ewkb(ewkb: &[u8]) -> Result<Vec<u8>> {
    let (geo, srid) = ewkb_to_geo(&mut Ewkb(ewkb))?;
    check_srid(srid)?;
    geo.coords_iter().try_for_each(|c| check_point(c.x, c.y))?;
    geo.to_ewkb(geozero::CoordDimensions::xy(), None)
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
}

pub fn geography_from_geojson(json_str: &str) -> Result<Vec<u8>> {
    let json = GeoJson(json_str);
    let geo = json
        .to_geo()
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
    geo.coords_iter().try_for_each(|c| check_point(c.x, c.y))?;
    geo.to_ewkb(geozero::CoordDimensions::xy(), None)
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
}

pub fn check_srid(srid: Option<i32>) -> Result<()> {
    if let Some(srid) = srid
        && srid != GEOGRAPHY_SRID
    {
        return Err(ErrorCode::GeometryError(format!(
            "SRIDs other than 4326 are not supported. Got SRID: {}",
            srid
        )));
    }
    Ok(())
}

pub fn check_point(lon: f64, lat: f64) -> Result<()> {
    if !(LONGITUDE_MIN..=LONGITUDE_MAX).contains(&lon) {
        return Err(ErrorCode::GeometryError(
            "longitude is out of range".to_string(),
        ));
    }
    if !(LATITUDE_MIN..=LATITUDE_MAX).contains(&lat) {
        return Err(ErrorCode::GeometryError(
            "latitude is out of range".to_string(),
        ));
    }
    Ok(())
}

fn haversine_distance_point_to_geometry(
    point: &Point<f64>,
    geometry: &Geometry<f64>,
) -> Result<f64> {
    match geometry.haversine_closest_point(point) {
        Closest::Intersection(_) => Ok(0_f64),
        Closest::SinglePoint(closest) => Ok(Haversine.distance(*point, closest)),
        Closest::Indeterminate => Err(ErrorCode::GeometryError(
            "failed to calculate distance for empty geography".to_string(),
        )),
    }
}

pub fn haversine_distance_between_geometries(
    left: &Geometry<f64>,
    right: &Geometry<f64>,
) -> Result<f64> {
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
        ErrorCode::GeometryError("failed to calculate distance for empty geography".to_string())
    })
}
