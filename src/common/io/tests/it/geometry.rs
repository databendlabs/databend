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

use databend_common_io::geometry::Bbox;
use databend_common_io::geometry::ewkb_to_bbox;
use databend_common_io::geometry::geometry_from_str;
use databend_common_io::wkb::make_point;

#[test]
fn test_extract_bbox_point() {
    let point = make_point(-74.0, 40.7);
    let bbox = ewkb_to_bbox(&point).unwrap().bbox.unwrap();
    assert_eq!(bbox, Bbox::from_corners(-74.0, 40.7, -74.0, 40.7));
}

#[test]
fn test_extract_bbox_point_with_srid() {
    let ewkb = geometry_from_str("SRID=4326;POINT(-122.35 37.55)", None).unwrap();
    let result = ewkb_to_bbox(&ewkb).unwrap();
    assert_eq!(
        result.bbox.unwrap(),
        Bbox::from_corners(-122.35, 37.55, -122.35, 37.55)
    );
    assert_eq!(result.srid, Some(4326));
}

#[test]
fn test_extract_bbox_linestring() {
    let ewkb = geometry_from_str("LINESTRING(0 0, 10 5, 20 3)", None).unwrap();
    let bbox = ewkb_to_bbox(&ewkb).unwrap().bbox.unwrap();
    assert_eq!(bbox, Bbox::from_corners(0.0, 0.0, 20.0, 5.0));
}

#[test]
fn test_extract_bbox_polygon() {
    let ewkb = geometry_from_str(
        "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2))",
        None,
    )
    .unwrap();
    let bbox = ewkb_to_bbox(&ewkb).unwrap().bbox.unwrap();
    assert_eq!(bbox, Bbox::from_corners(0.0, 0.0, 10.0, 10.0));
}

#[test]
fn test_extract_bbox_point_z() {
    // EWKB PointZ(1 2 3), little-endian, Z flag 0x80000000. Databend never stores
    // 3D geometries today (all ingest paths funnel through to_ewkb(xy)), but the
    // EWKB parser must still derive a correct 2D bbox if one ever appears.
    let ewkb: &[u8] = &[
        0x01, // little-endian
        0x01, 0x00, 0x00, 0x80, // type = Point | Z flag
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // z = 3.0
    ];
    let bbox = ewkb_to_bbox(ewkb).unwrap().bbox.unwrap();
    assert_eq!(bbox, Bbox::from_corners(1.0, 2.0, 1.0, 2.0));
}
