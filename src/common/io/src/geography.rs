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
use geo::CoordsIter;
use geo::Geometry;
use geos::wkt::TryFromWkt;
use geozero::ToWkb;

pub const LONGITUDE_MIN: f64 = -180.0;
pub const LONGITUDE_MAX: f64 = 180.0;
pub const LATITUDE_MIN: f64 = -90.0;
pub const LATITUDE_MAX: f64 = 90.0;

use super::geometry::cut_srid;

pub fn geography_from_ewkt_bytes(ewkt: &[u8]) -> Result<Vec<u8>> {
    let s = std::str::from_utf8(ewkt).map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
    geography_from_ewkt(s)
}

pub fn geography_from_ewkt(ewkt: &str) -> Result<Vec<u8>> {
    let (srid, wkt) = cut_srid(ewkt)?;
    let geog: Geometry<f64> =
        Geometry::try_from_wkt_str(wkt).map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
    geog.coords_iter().try_for_each(|c| check_point(c.x, c.y))?;
    geog.to_ewkb(geozero::CoordDimensions::xy(), srid)
        .map_err(ErrorCode::from)
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
