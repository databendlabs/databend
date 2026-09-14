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
use geo::LineString;
use geo::Point;
use geo::Polygon;
use geo::Rect;
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
use hex::encode_upper;
use serde::Deserialize;
use serde::Serialize;
use wkt::TryFromWkt;

pub const UNKNOWN_SRID: i32 = 0;

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
/// use databend_common_io::geometry::geometry_from_str;
///
/// // WKT input without SRID
/// let ewkb = geometry_from_str("POINT(125.6 10.1)", None).unwrap();
/// assert_eq!(ewkb.len(), 21); // endian(1) + type(4) + x/y(16)
///
/// // EWKT input with SRID
/// let ewkb = geometry_from_str("SRID=4326;POINT(125.6 10.1)", None).unwrap();
/// assert_eq!(ewkb.len(), 25); // endian(1) + type(4) + srid(4) + x/y(16)
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

pub fn geometry_format(ewkb: &[u8], format_type: GeometryDataType) -> Result<String> {
    let (geo, srid) = ewkb_to_geo(&mut Ewkb(ewkb))?;
    let srid = srid.unwrap_or(UNKNOWN_SRID);
    match format_type {
        GeometryDataType::WKB => geo_to_wkb(geo).map(encode_upper),
        GeometryDataType::EWKB => geo_to_ewkb(geo, Some(srid)).map(encode_upper),
        GeometryDataType::WKT => geo_to_wkt(geo),
        GeometryDataType::EWKT => geo_to_ewkt(geo, Some(srid)),
        GeometryDataType::GEOJSON => geo_to_json(geo),
    }
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

pub fn rect_to_polygon(rect: Rect<f64>) -> Polygon<f64> {
    let min = rect.min();
    let max = rect.max();
    let exterior = LineString::from(vec![
        (min.x, min.y),
        (max.x, min.y),
        (max.x, max.y),
        (min.x, max.y),
        (min.x, min.y),
    ]);
    Polygon::new(exterior, vec![])
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
    geometry.bounding_rect().map(|rect| match axis {
        Axis::X => match extremum {
            Extremum::Max => rect.max().x,
            Extremum::Min => rect.min().x,
        },
        Axis::Y => match extremum {
            Extremum::Max => rect.max().y,
            Extremum::Min => rect.min().y,
        },
    })
}

pub fn geometry_bbox_center(geometry: &Geometry<f64>) -> Option<(f64, f64)> {
    let bbox = geometry.bounding_rect()?;
    let x = bbox.min().x + (bbox.max().x - bbox.min().x) / 2.0;
    let y = bbox.min().y + (bbox.max().y - bbox.min().y) / 2.0;
    Some((x, y))
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bbox {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
}

impl Bbox {
    pub fn new(x: f64, y: f64) -> Self {
        Self {
            min_x: x,
            min_y: y,
            max_x: x,
            max_y: y,
        }
    }

    pub fn from_corners(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Self {
        Self {
            min_x,
            min_y,
            max_x,
            max_y,
        }
    }

    pub fn corners(&self) -> (f64, f64, f64, f64) {
        (self.min_x, self.min_y, self.max_x, self.max_y)
    }

    pub fn extend(&mut self, x: f64, y: f64) {
        if x < self.min_x {
            self.min_x = x;
        }
        if x > self.max_x {
            self.max_x = x;
        }
        if y < self.min_y {
            self.min_y = y;
        }
        if y > self.max_y {
            self.max_y = y;
        }
    }

    pub fn expand(&mut self, distance: f64) {
        self.min_x -= distance;
        self.min_y -= distance;
        self.max_x += distance;
        self.max_y += distance;
    }

    pub fn intersects(&self, other: &Self) -> bool {
        self.max_x >= other.min_x
            && self.min_x <= other.max_x
            && self.max_y >= other.min_y
            && self.min_y <= other.max_y
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EwkbBbox {
    pub bbox: Option<Bbox>,
    pub srid: Option<i32>,
}

pub fn ewkb_to_bbox(ewkb: &[u8]) -> Option<EwkbBbox> {
    let mut processor = BboxProcessor::new();
    Ewkb(ewkb).process_geom(&mut processor).ok()?;
    Some(processor.into_ewkb_bbox())
}

struct BboxProcessor {
    bbox: Option<Bbox>,
    srid: Option<i32>,
}

impl BboxProcessor {
    fn new() -> Self {
        Self {
            bbox: None,
            srid: None,
        }
    }

    fn extend(&mut self, x: f64, y: f64) {
        if let Some(bbox) = self.bbox.as_mut() {
            bbox.extend(x, y);
        } else {
            self.bbox = Some(Bbox::new(x, y));
        }
    }

    fn into_ewkb_bbox(self) -> EwkbBbox {
        EwkbBbox {
            bbox: self.bbox,
            srid: self.srid,
        }
    }
}

impl GeomProcessor for BboxProcessor {
    fn srid(&mut self, srid: Option<i32>) -> geozero::error::Result<()> {
        self.srid = srid;
        Ok(())
    }

    fn xy(&mut self, x: f64, y: f64, _idx: usize) -> geozero::error::Result<()> {
        self.extend(x, y);
        Ok(())
    }

    fn coordinate(
        &mut self,
        x: f64,
        y: f64,
        _z: Option<f64>,
        _m: Option<f64>,
        _t: Option<f64>,
        _tm: Option<u64>,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.xy(x, y, idx)
    }
}
