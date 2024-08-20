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

use std::io::Cursor;

use scroll::Endian;
use scroll::IOread;
use scroll::IOwrite;

const WKB_XDR: u8 = 0;
const WKB_NDR: u8 = 1;
const WKB_Z: u32 = 0x80000000;
const WKB_M: u32 = 0x40000000;
const WKB_SRID: u32 = 0x20000000;
const WKB_TYPE_MASK: u32 = 7;

pub fn read_wkb_header(mut raw: &[u8]) -> Result<WkbInfo, String> {
    let byte_order = raw.ioread::<u8>().map_err(|e| e.to_string())?;
    let endian = match byte_order {
        WKB_XDR => Endian::from(false),
        WKB_NDR => Endian::from(true),
        _ => return Err("Unexpected byte order.".to_string()),
    };

    let type_id = raw.ioread_with::<u32>(endian).map_err(|e| e.to_string())?;

    let srid = if type_id & WKB_SRID != 0 {
        Some(raw.ioread::<i32>().map_err(|e| e.to_string())?)
    } else {
        None
    };

    let base_type = match type_id & WKB_TYPE_MASK {
        1 => WkbType::Point,
        2 => WkbType::LineString,
        3 => WkbType::Polygon,
        4 => WkbType::MultiPoint,
        5 => WkbType::MultiLineString,
        6 => WkbType::MultiPolygon,
        7 => WkbType::GeometryCollection,
        _ => return Err("Unknown Geometry Type".to_string()),
    };

    Ok(WkbInfo {
        endian,
        base_type,
        has_z: type_id & WKB_Z != 0,
        has_m: type_id & WKB_M != 0,
        srid,
    })
}

#[derive(Debug)]
pub enum WkbType {
    Point = 1,
    LineString = 2,
    Polygon = 3,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
    GeometryCollection = 7,
}

#[derive(Debug)]
pub struct WkbInfo {
    pub endian: Endian,
    pub base_type: WkbType,
    pub has_z: bool,
    pub has_m: bool,
    pub srid: Option<i32>,
}

pub const POINT_SIZE: usize = 21;

pub fn make_point(x: f64, y: f64) -> Vec<u8> {
    let bytes = Vec::with_capacity(21);
    let mut bytes = Cursor::new(bytes);
    bytes.iowrite(WKB_NDR).unwrap();
    bytes.iowrite(WkbType::Point as u32).unwrap();
    bytes.iowrite(x).unwrap();
    bytes.iowrite(y).unwrap();
    bytes.into_inner()
}
