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

use databend_common_io::geometry::extract_bbox_from_ewkb;
use databend_common_io::geometry::extract_point_xy_from_ewkb;
use databend_common_io::geometry::geometry_from_str;
use databend_common_io::wkb::make_point;

#[test]
fn test_extract_bbox_point() {
    let point = make_point(-74.0, 40.7);
    let bbox = extract_bbox_from_ewkb(&point).unwrap();
    assert_eq!(bbox, (-74.0, 40.7, -74.0, 40.7));
}

#[test]
fn test_extract_point_xy() {
    let point = make_point(139.7, 35.6);
    let xy = extract_point_xy_from_ewkb(&point).unwrap();
    assert_eq!(xy, (139.7, 35.6));
}

#[test]
fn test_extract_bbox_point_with_srid() {
    let ewkb = geometry_from_str("SRID=4326;POINT(-122.35 37.55)", None).unwrap();
    let bbox = extract_bbox_from_ewkb(&ewkb).unwrap();
    assert_eq!(bbox, (-122.35, 37.55, -122.35, 37.55));
}

#[test]
fn test_extract_bbox_linestring() {
    let ewkb = geometry_from_str("LINESTRING(0 0, 10 5, 20 3)", None).unwrap();
    let bbox = extract_bbox_from_ewkb(&ewkb).unwrap();
    assert_eq!(bbox, (0.0, 0.0, 20.0, 5.0));
}

#[test]
fn test_extract_bbox_polygon() {
    let ewkb = geometry_from_str(
        "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2))",
        None,
    )
    .unwrap();
    let bbox = extract_bbox_from_ewkb(&ewkb).unwrap();
    assert_eq!(bbox, (0.0, 0.0, 10.0, 10.0));
}
