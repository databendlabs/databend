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
use std::str::FromStr;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use geo::BoundingRect;
use geo::Geometry;
use geo::Point;
use geohash::encode;
use geozero::CoordDimensions;
use geozero::GeomProcessor;
use geozero::GeozeroGeometry;
use geozero::ToGeo;
use geozero::ToJson;
use geozero::ToWkb;
use geozero::ToWkt;
use geozero::geo_types::GeoWriter;
use geozero::geojson::GeoJson;
use geozero::wkb::Ewkb;
use serde::Deserialize;
use serde::Serialize;
use wkt::TryFromWkt;

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

pub fn parse_bytes_to_ewkb(buf: &[u8], srid: Option<i32>) -> Result<Vec<u8>> {
    let s = std::str::from_utf8(buf).map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
    geometry_from_str(s, srid)
}

/// Parses an input and returns a value of ewkb geometry.
///
/// Support any possible geometry format.
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
///     geometry_from_str(wkt).unwrap(),
///     geometry_from_str(wkb).unwrap(),
///     geometry_from_str(geo_json.as_bytes()).unwrap()
/// );
/// ```
pub fn geometry_from_str(input: &str, srid: Option<i32>) -> Result<Vec<u8>> {
    let input = input.trim();
    let (geo, parsed_srid) = str_to_geo(input)?;
    let srid = srid.or(parsed_srid);

    geo.to_ewkb(CoordDimensions::xy(), srid)
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
}

/// Parses an EWKT input and returns a value of EWKB geometry.
pub fn geometry_from_ewkt(input: &str, srid: Option<i32>) -> Result<Vec<u8>> {
    let input = input.trim();
    let (geo, parsed_srid) = ewkt_str_to_geo(input)?;
    let srid = srid.or(parsed_srid);
    geo.to_ewkb(CoordDimensions::xy(), srid)
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
}

/// Parses a GEOJSON/EWKB/WKB/EWKT/WKT input and returns a Geometry object.
pub(crate) fn str_to_geo(input: &str) -> Result<(Geometry, Option<i32>)> {
    if input.starts_with(['{']) {
        let geo = GeoJson(input)
            .to_geo()
            .map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
        Ok((geo, None))
    } else if input.starts_with(['0', '0']) || input.starts_with(['0', '1']) {
        let binary = match hex::decode(input) {
            Ok(binary) => binary,
            Err(e) => return Err(ErrorCode::GeometryError(e.to_string())),
        };
        ewkb_to_geo(&mut Ewkb(&binary))
    } else {
        ewkt_str_to_geo(input)
    }
}

/// Parses an EWKT input and returns Geometry object and SRID.
pub(crate) fn ewkt_str_to_geo(input: &str) -> Result<(Geometry, Option<i32>)> {
    if input.starts_with(['s']) || input.starts_with(['S']) {
        if let Some((srid_input, wkt_input)) = input.split_once(';') {
            let srid_input = srid_input.to_uppercase();
            if let Some(srid_str) = srid_input.strip_prefix("SRID")
                && let Some(srid_str) = srid_str.trim().strip_prefix("=")
            {
                let parsed_srid = srid_str.trim().parse::<i32>()?;
                let geo = Geometry::try_from_wkt_str(wkt_input)
                    .map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
                return Ok((geo, Some(parsed_srid)));
            }
        }
        Err(ErrorCode::GeometryError("invalid srid"))
    } else {
        let geo = Geometry::try_from_wkt_str(input)
            .map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
        Ok((geo, None))
    }
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

/// Convert Geometry object to GEOJSON format.
pub fn geo_to_json(geo: Geometry) -> Result<String> {
    geo.to_json()
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
}

/// Convert Geometry object to WKB format.
pub fn geo_to_wkb(geo: Geometry) -> Result<Vec<u8>> {
    geo.to_wkb(geo.dims())
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
}

/// Convert Geometry object to EWKB format.
pub fn geo_to_ewkb(geo: Geometry, srid: Option<i32>) -> Result<Vec<u8>> {
    geo.to_ewkb(geo.dims(), srid)
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
}

/// Convert Geometry object to WKT format.
pub fn geo_to_wkt(geo: Geometry) -> Result<String> {
    geo.to_wkt()
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
}

/// Convert Geometry object to EWKT format.
pub fn geo_to_ewkt(geo: Geometry, srid: Option<i32>) -> Result<String> {
    geo.to_ewkt(srid)
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
}

/// Process EWKB input and return SRID.
pub fn read_srid<B: AsRef<[u8]>>(ewkb: &mut Ewkb<B>) -> Option<i32> {
    let mut srid_processor = SridProcessor::new();
    ewkb.process_geom(&mut srid_processor).ok()?;

    srid_processor.srid
}

/// Process EWKB input and return Geometry object and SRID.
pub fn ewkb_to_geo<B: AsRef<[u8]>>(ewkb: &mut Ewkb<B>) -> Result<(Geometry<f64>, Option<i32>)> {
    let mut ewkb_processor = EwkbProcessor::new();
    ewkb.process_geom(&mut ewkb_processor)?;

    let geo = ewkb_processor
        .geo_writer
        .take_geometry()
        .ok_or_else(|| ErrorCode::GeometryError("Invalid ewkb format"))?;
    let srid = ewkb_processor.srid;
    Ok((geo, srid))
}

struct SridProcessor {
    srid: Option<i32>,
}

impl SridProcessor {
    fn new() -> Self {
        Self { srid: None }
    }
}

impl GeomProcessor for SridProcessor {
    fn srid(&mut self, srid: Option<i32>) -> geozero::error::Result<()> {
        self.srid = srid;
        Ok(())
    }
}

struct EwkbProcessor {
    geo_writer: GeoWriter,
    srid: Option<i32>,
}

impl EwkbProcessor {
    fn new() -> Self {
        Self {
            geo_writer: GeoWriter::new(),
            srid: None,
        }
    }
}

impl GeomProcessor for EwkbProcessor {
    fn srid(&mut self, srid: Option<i32>) -> geozero::error::Result<()> {
        self.srid = srid;
        Ok(())
    }

    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.xy(x, y, idx)
    }

    fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.point_begin(idx)
    }

    fn point_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.point_end(idx)
    }

    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.multipoint_begin(size, idx)
    }

    fn multipoint_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.multipoint_end(idx)
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.geo_writer.linestring_begin(tagged, size, idx)
    }

    fn linestring_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.linestring_end(tagged, idx)
    }

    fn multilinestring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.multilinestring_begin(size, idx)
    }

    fn multilinestring_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.multilinestring_end(idx)
    }

    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.geo_writer.polygon_begin(tagged, size, idx)
    }

    fn polygon_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.polygon_end(tagged, idx)
    }

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.multipolygon_begin(size, idx)
    }

    fn multipolygon_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.multipolygon_end(idx)
    }

    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.geometrycollection_begin(size, idx)
    }

    fn geometrycollection_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geo_writer.geometrycollection_end(idx)
    }
}

/// Return Geometry type name.
pub fn geometry_type_name(geo: &Geometry) -> &'static str {
    match geo {
        Geometry::Point(_) => "Point",
        Geometry::Line(_) => "Line",
        Geometry::LineString(_) => "LineString",
        Geometry::Polygon(_) => "Polygon",
        Geometry::MultiPoint(_) => "MultiPoint",
        Geometry::MultiLineString(_) => "MultiLineString",
        Geometry::MultiPolygon(_) => "MultiPolygon",
        Geometry::GeometryCollection(_) => "GeometryCollection",
        Geometry::Rect(_) => "Rect",
        Geometry::Triangle(_) => "Triangle",
    }
}

pub fn st_extreme(geometry: &Geometry<f64>, axis: Axis, extremum: Extremum) -> Option<f64> {
    match geometry {
        Geometry::Point(point) => {
            let coord = match axis {
                Axis::X => point.x(),
                Axis::Y => point.y(),
            };
            Some(coord)
        }
        Geometry::MultiPoint(multi_point) => {
            let mut extreme_coord: Option<f64> = None;
            for point in multi_point {
                if let Some(coord) = st_extreme(&Geometry::Point(*point), axis, extremum) {
                    extreme_coord = match extreme_coord {
                        Some(existing) => match extremum {
                            Extremum::Max => Some(existing.max(coord)),
                            Extremum::Min => Some(existing.min(coord)),
                        },
                        None => Some(coord),
                    };
                }
            }
            extreme_coord
        }
        Geometry::Line(line) => {
            let bounding_rect = line.bounding_rect();
            let coord = match axis {
                Axis::X => match extremum {
                    Extremum::Max => bounding_rect.max().x,
                    Extremum::Min => bounding_rect.min().x,
                },
                Axis::Y => match extremum {
                    Extremum::Max => bounding_rect.max().y,
                    Extremum::Min => bounding_rect.min().y,
                },
            };
            Some(coord)
        }
        Geometry::MultiLineString(multi_line) => {
            let mut extreme_coord: Option<f64> = None;
            for line in multi_line {
                if let Some(coord) = st_extreme(&Geometry::LineString(line.clone()), axis, extremum)
                {
                    extreme_coord = match extreme_coord {
                        Some(existing) => match extremum {
                            Extremum::Max => Some(existing.max(coord)),
                            Extremum::Min => Some(existing.min(coord)),
                        },
                        None => Some(coord),
                    };
                }
            }
            extreme_coord
        }
        Geometry::Polygon(polygon) => {
            let bounding_rect = polygon.bounding_rect()?;
            let coord = match axis {
                Axis::X => match extremum {
                    Extremum::Max => bounding_rect.max().x,
                    Extremum::Min => bounding_rect.min().x,
                },
                Axis::Y => match extremum {
                    Extremum::Max => bounding_rect.max().y,
                    Extremum::Min => bounding_rect.min().y,
                },
            };
            Some(coord)
        }
        Geometry::MultiPolygon(multi_polygon) => {
            let mut extreme_coord: Option<f64> = None;
            for polygon in multi_polygon {
                if let Some(coord) = st_extreme(&Geometry::Polygon(polygon.clone()), axis, extremum)
                {
                    extreme_coord = match extreme_coord {
                        Some(existing) => match extremum {
                            Extremum::Max => Some(existing.max(coord)),
                            Extremum::Min => Some(existing.min(coord)),
                        },
                        None => Some(coord),
                    };
                }
            }
            extreme_coord
        }
        Geometry::GeometryCollection(geometry_collection) => {
            let mut extreme_coord: Option<f64> = None;
            for geometry in geometry_collection {
                if let Some(coord) = st_extreme(geometry, axis, extremum) {
                    extreme_coord = match extreme_coord {
                        Some(existing) => match extremum {
                            Extremum::Max => Some(existing.max(coord)),
                            Extremum::Min => Some(existing.min(coord)),
                        },
                        None => Some(coord),
                    };
                }
            }
            extreme_coord
        }
        Geometry::LineString(line_string) => line_string.bounding_rect().map(|rect| match axis {
            Axis::X => match extremum {
                Extremum::Max => rect.max().x,
                Extremum::Min => rect.min().x,
            },
            Axis::Y => match extremum {
                Extremum::Max => rect.max().y,
                Extremum::Min => rect.min().y,
            },
        }),
        Geometry::Rect(rect) => {
            let coord = match axis {
                Axis::X => match extremum {
                    Extremum::Max => rect.max().x,
                    Extremum::Min => rect.min().x,
                },
                Axis::Y => match extremum {
                    Extremum::Max => rect.max().y,
                    Extremum::Min => rect.min().y,
                },
            };
            Some(coord)
        }
        Geometry::Triangle(triangle) => {
            let bounding_rect = triangle.bounding_rect();
            let coord = match axis {
                Axis::X => match extremum {
                    Extremum::Max => bounding_rect.max().x,
                    Extremum::Min => bounding_rect.min().x,
                },
                Axis::Y => match extremum {
                    Extremum::Max => bounding_rect.max().y,
                    Extremum::Min => bounding_rect.min().y,
                },
            };
            Some(coord)
        }
    }
}

pub fn count_points(geom: &Geometry) -> usize {
    match geom {
        Geometry::Point(_) => 1,
        Geometry::Line(_) => 2,
        Geometry::LineString(line_string) => line_string.0.len(),
        Geometry::Polygon(polygon) => {
            polygon.exterior().0.len()
                + polygon
                    .interiors()
                    .iter()
                    .map(|line_string| line_string.0.len())
                    .sum::<usize>()
        }
        Geometry::MultiPoint(multi_point) => multi_point.0.len(),
        Geometry::MultiLineString(multi_line_string) => multi_line_string
            .0
            .iter()
            .map(|line_string| line_string.0.len())
            .sum::<usize>(),
        Geometry::MultiPolygon(multi_polygon) => multi_polygon
            .0
            .iter()
            .map(|polygon| count_points(&Geometry::Polygon(polygon.clone())))
            .sum::<usize>(),
        Geometry::GeometryCollection(geometry_collection) => geometry_collection
            .0
            .iter()
            .map(count_points)
            .sum::<usize>(),
        Geometry::Rect(_) => 5,
        Geometry::Triangle(_) => 4,
    }
}

pub fn point_to_geohash(ewkb: &[u8], precision: Option<i32>) -> Result<String> {
    let (geo, _) = ewkb_to_geo(&mut Ewkb(ewkb))?;
    let point = Point::try_from(geo).map_err(|e| ErrorCode::GeometryError(e.to_string()))?;
    encode(point.0, precision.map_or(12, |p| p as usize))
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
}
