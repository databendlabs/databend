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
use geozero::CoordDimensions;
use geozero::ToWkb;
use wkt::TryFromWkt;

pub fn parse_to_ewkb(buf: &[u8]) -> Result<Vec<u8>> {
    let wkt = std::str::from_utf8(buf).map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
    let mut srid: Option<i32> = None;
    let input_wkt = wkt.trim().to_ascii_uppercase();

    let parts: Vec<&str> = input_wkt.split(';').collect();

    if input_wkt.starts_with("SRID=") && parts.len() == 2 {
        srid = Some(parts[0].replace("SRID=", "").parse()?);
    }

    let geo_part = if parts.len() == 2 { parts[1] } else { parts[0] };

    let geom: Geometry<f64> = Geometry::try_from_wkt_str(geo_part)
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))?;

    geom.to_ewkb(CoordDimensions::xy(), srid)
        .map_err(ErrorCode::from)
}
