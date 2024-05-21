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

use std::fmt::Display;
use std::io;
use std::io::Read;
use std::str::FromStr;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use geo::Geometry;
use geozero::wkb::Ewkb;
use geozero::CoordDimensions;
use geozero::GeozeroGeometry;
use geozero::ToJson;
use geozero::ToWkb;
use geozero::ToWkt;
use scroll::Endian;
use scroll::IOread;
use serde::Deserialize;
use serde::Serialize;
use wkt::TryFromWkt;

const GEO_TYPE_ID_MASK: u32 = 0x2000_0000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum GeometryDataType {
    WKB,
    WKT,
    EWKB,
    #[default]
    EWKT,
    GEOJSON,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Axis {
    X,
    Y,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Extremum {
    Max,
    Min,
}

impl FromStr for GeometryDataType {
    type Err = ErrorCode;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "WKB" => Ok(GeometryDataType::WKB),
            "WKT" => Ok(GeometryDataType::WKT),
            "EWKB" => Ok(GeometryDataType::EWKB),
            "EWKT" => Ok(GeometryDataType::EWKT),
            "GEOJSON" => Ok(GeometryDataType::GEOJSON),
            _ => Err(ErrorCode::GeometryError(
                "Invalid geometry type format".to_string(),
            )),
        }
    }
}

impl Display for GeometryDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let str = match self {
            GeometryDataType::WKB => "WKB".to_string(),
            GeometryDataType::WKT => "WKT".to_string(),
            GeometryDataType::EWKB => "EWKB".to_string(),
            GeometryDataType::EWKT => "EWKT".to_string(),
            GeometryDataType::GEOJSON => "GEOJSON".to_string(),
        };
        write!(f, "{}", str)
    }
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

pub fn read_ewkb_srid<R: Read>(raw: &mut R) -> std::result::Result<Option<i32>, io::Error> {
    let byte_order = raw.ioread::<u8>()?;
    let is_little_endian = byte_order != 0;
    let endian = Endian::from(is_little_endian);
    let type_id = raw.ioread_with::<u32>(endian)?;
    let srid = if type_id & GEO_TYPE_ID_MASK == GEO_TYPE_ID_MASK {
        Some(raw.ioread_with::<i32>(endian)?)
    } else {
        None
    };

    Ok(srid)
}

pub trait GeometryFormatOutput {
    fn format(self, data_type: GeometryDataType) -> Result<String>;
}
impl<B: AsRef<[u8]>> GeometryFormatOutput for Ewkb<B> {
    fn format(self, format_type: GeometryDataType) -> Result<String> {
        match format_type {
            GeometryDataType::WKB => self
                .to_wkb(CoordDimensions::xy())
                .map(|bytes| {
                    bytes
                        .iter()
                        .map(|b| format!("{:02X}", b))
                        .collect::<Vec<_>>()
                        .join("")
                })
                .map_err(|e| ErrorCode::GeometryError(e.to_string())),
            GeometryDataType::EWKB => Ok(self
                .0
                .as_ref()
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<Vec<_>>()
                .join("")),
            GeometryDataType::WKT => self
                .to_wkt()
                .map_err(|e| ErrorCode::GeometryError(e.to_string())),
            GeometryDataType::EWKT => self
                .to_ewkt(self.srid())
                .map_err(|e| ErrorCode::GeometryError(e.to_string())),
            GeometryDataType::GEOJSON => self
                .to_json()
                .map_err(|e| ErrorCode::GeometryError(e.to_string())),
        }
    }
}

pub fn geometry_format<T: GeometryFormatOutput>(
    geometry: T,
    format_type: GeometryDataType,
) -> Result<String> {
    geometry.format(format_type)
}
