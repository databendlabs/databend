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
use geo::Geometry;
use geo::Point;
use wkt::TryFromWkt;

pub fn parse_ewkt_point(buf: &[u8]) -> Result<Point<f64>> {
    let wkt = std::str::from_utf8(buf).map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
    let input_wkt = wkt.trim().to_ascii_uppercase();

    let parts: Vec<&str> = input_wkt.split(';').collect();

    const WGS84: i32 = 4326;
    if input_wkt.starts_with("SRID=") {
        let srid = parts[0].replace("SRID=", "").parse::<i32>()?;
        if srid != WGS84 {
            return Err(ErrorCode::GeometryError("supports only WGS84"));
        }
    }

    let geo_part = if parts.len() == 2 { parts[1] } else { parts[0] };

    let geom: Geometry<f64> = Geometry::try_from_wkt_str(geo_part)
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))?;

    match geom {
        Geometry::Point(p) => Ok(p),
        _ => Err(ErrorCode::GeometryError("not a point")),
    }
}
