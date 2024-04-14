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

pub enum GeometryDataType {
    EWKB,
    EWKT,
    GEOJSON,
}

pub fn parse_to_ewkb(buf: &[u8], srid: Option<i32>) -> Result<Vec<u8>> {
    let wkt = std::str::from_utf8(buf).map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
    let input_wkt = wkt.trim().to_ascii_uppercase();

    let parts: Vec<&str> = input_wkt.split(';').collect();

    let parsed_srid: Option<i32> = srid.or_else(|| {
        if input_wkt.starts_with("SRID=") && parts.len() == 2 {
            parts[0].replace("SRID=", "").parse().ok()
        } else {
            None
        }
    });

    let geo_part = if parts.len() == 2 { parts[1] } else { parts[0] };

    let geom: Geometry<f64> = Geometry::try_from_wkt_str(geo_part)
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))?;

    geom.to_ewkb(CoordDimensions::xy(), parsed_srid)
        .map_err(ErrorCode::from)
}

/// An enum representing any possible geometry subtype.
///
/// WKB/EWKB: start with 01/00(1bit)
///
/// WKT/EWKT: start with SRID/POINT/LINESTRING/POLYGON/MULTIPOINT/MULTILINESTRING/MULTIPOLYGON/GEOMETRYCOLLECTION
///
/// GEOJSON: start with '{' and end with '}'
///
/// # Example
///
/// ```
/// let geo_json = r#"
///         {
///           "type": "Feature",
///           "geometry": {
///             "type": "Point",
///             "coordinates": [125.6, 10.1]
///           },
///           "properties": {
///             "name": "Dinagat Islands"
///           }
///         }
///     "#;
///
/// let wkt: &[u8] = "LINESTRING(0 0 1, 1 1 1, 2 1 2)".as_bytes();
/// let wkb: &[u8] = "0101000020797f000066666666a9cb17411f85ebc19e325641".as_bytes();
/// println!(
///     "wkt:{ } wkb:{ } json: { }",
///     parse_to_subtype(wkt).unwrap(),
///     parse_to_subtype(wkb).unwrap(),
///     parse_to_subtype(geo_json.as_bytes()).unwrap()
/// );
/// ```
pub fn parse_to_subtype(buf: &[u8]) -> Result<GeometryDataType> {
    let bit_0_1: u8 = buf[0] & 1 | (buf[0] >> 1) & 1;
    match std::str::from_utf8(buf) {
        Ok(str) => {
            let text: &String = &str.replace([' ', '\n'], "");
            let prefixes = [
                "SRID",
                "POINT",
                "LINESTRING",
                "POLYGON",
                "MULTIPOINT",
                "MULTILINESTRING",
                "MULTIPOLYGON",
                "GEOMETRYCOLLECTION",
            ];
            if prefixes.iter().any(|&prefix| text.starts_with(prefix)) {
                Ok(GeometryDataType::EWKT)
            } else if (text.starts_with('{')) && (text.ends_with('}')) {
                Ok(GeometryDataType::GEOJSON)
            } else if bit_0_1 == 0b00 || bit_0_1 == 0b01 {
                Ok(GeometryDataType::EWKB)
            } else {
                Err(ErrorCode::GeometryError(
                    "Invalid geometry type format".to_string(),
                ))
            }
        }
        Err(_) => {
            if bit_0_1 == 0b00 || bit_0_1 == 0b01 {
                Ok(GeometryDataType::EWKB)
            } else {
                Err(ErrorCode::GeometryError(
                    "Invalid geometry type format".to_string(),
                ))
            }
        }
    }
}
