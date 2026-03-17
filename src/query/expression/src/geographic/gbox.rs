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

// Portions of this file are adapted from PostGIS:
// - liblwgeom/lwgeodetic.c
// Licensed under GPL-2.0-or-later.
// Modifications by Databend.

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use geo::BoundingRect;
use geo::Contains;
use geo::Coord;
use geo::CoordsIter;
use geo::Geometry;
use geo::GeometryCollection;
use geo::Intersects;
use geo::Line;
use geo::LineString;
use geo::MultiLineString;
use geo::MultiPoint;
use geo::MultiPolygon;
use geo::Point;
use geo::Polygon;
use geo::Rect;
use geo::ToDegrees;
use geo::Triangle;
use geo::algorithm::bool_ops::BooleanOps;
use geo::algorithm::bool_ops::unary_union;
use geo::algorithm::buffer::Buffer;
use geo::algorithm::line_intersection::LineIntersection;
use geo::algorithm::line_intersection::line_intersection;
use proj4rs::Proj;
use proj4rs::transform::transform;

#[derive(Clone, Copy, Debug)]
pub struct Gbox {
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    zmin: f64,
    zmax: f64,
}

impl Gbox {
    fn new(x: f64, y: f64, z: f64) -> Self {
        Self {
            xmin: x,
            xmax: x,
            ymin: y,
            ymax: y,
            zmin: z,
            zmax: z,
        }
    }

    fn update(&mut self, x: f64, y: f64, z: f64) {
        self.xmin = self.xmin.min(x);
        self.xmax = self.xmax.max(x);
        self.ymin = self.ymin.min(y);
        self.ymax = self.ymax.max(y);
        self.zmin = self.zmin.min(z);
        self.zmax = self.zmax.max(z);
    }
}

pub fn gbox_union(left: &Gbox, right: &Gbox) -> Gbox {
    Gbox {
        xmin: left.xmin.min(right.xmin),
        xmax: left.xmax.max(right.xmax),
        ymin: left.ymin.min(right.ymin),
        ymax: left.ymax.max(right.ymax),
        zmin: left.zmin.min(right.zmin),
        zmax: left.zmax.max(right.zmax),
    }
}

const FP_TOLERANCE: f64 = 1e-12;

#[derive(Clone, Copy)]
struct Point2d {
    x: f64,
    y: f64,
}

#[derive(Clone, Copy)]
struct Point3d {
    x: f64,
    y: f64,
    z: f64,
}

fn fp_is_zero(value: f64) -> bool {
    value.abs() <= FP_TOLERANCE
}

fn fp_equals(left: f64, right: f64) -> bool {
    (left - right).abs() <= FP_TOLERANCE
}

fn signum(value: f64) -> i32 {
    ((value > 0.0) as i32) - ((value < 0.0) as i32)
}

fn lw_segment_side(p1: &Point2d, p2: &Point2d, q: &Point2d) -> i32 {
    let side = (q.x - p1.x) * (p2.y - p1.y) - (p2.x - p1.x) * (q.y - p1.y);
    signum(side)
}

fn dot_product(p1: &Point3d, p2: &Point3d) -> f64 {
    (p1.x * p2.x) + (p1.y * p2.y) + (p1.z * p2.z)
}

fn cross_product(a: &Point3d, b: &Point3d) -> Point3d {
    Point3d {
        x: a.y * b.z - a.z * b.y,
        y: a.z * b.x - a.x * b.z,
        z: a.x * b.y - a.y * b.x,
    }
}

fn vector_sum(a: &Point3d, b: &Point3d) -> Point3d {
    Point3d {
        x: a.x + b.x,
        y: a.y + b.y,
        z: a.z + b.z,
    }
}

fn vector_difference(a: &Point3d, b: &Point3d) -> Point3d {
    Point3d {
        x: a.x - b.x,
        y: a.y - b.y,
        z: a.z - b.z,
    }
}

fn normalize2d(p: &mut Point2d) {
    let d = (p.x * p.x + p.y * p.y).sqrt();
    if fp_is_zero(d) {
        p.x = 0.0;
        p.y = 0.0;
        return;
    }
    p.x /= d;
    p.y /= d;
}

fn normalize(p: &mut Point3d) {
    let d = (p.x * p.x + p.y * p.y + p.z * p.z).sqrt();
    if fp_is_zero(d) {
        p.x = 0.0;
        p.y = 0.0;
        p.z = 0.0;
        return;
    }
    p.x /= d;
    p.y /= d;
    p.z /= d;
}

fn unit_normal(p1: &Point3d, p2: &Point3d) -> Point3d {
    let p_dot = dot_product(p1, p2);
    let p3 = if p_dot < 0.0 {
        let mut p3 = vector_sum(p1, p2);
        normalize(&mut p3);
        p3
    } else if p_dot > 0.95 {
        let mut p3 = vector_difference(p2, p1);
        normalize(&mut p3);
        p3
    } else {
        *p2
    };

    let mut normal = cross_product(p1, &p3);
    normalize(&mut normal);
    normal
}

fn p3d_same(p1: &Point3d, p2: &Point3d) -> bool {
    p1.x == p2.x && p1.y == p2.y && p1.z == p2.z
}

fn gbox_init_point3d(p: &Point3d) -> Gbox {
    Gbox {
        xmin: p.x,
        xmax: p.x,
        ymin: p.y,
        ymax: p.y,
        zmin: p.z,
        zmax: p.z,
    }
}

fn gbox_merge_point3d(gbox: &mut Gbox, p: &Point3d) {
    if gbox.xmin > p.x {
        gbox.xmin = p.x;
    }
    if gbox.ymin > p.y {
        gbox.ymin = p.y;
    }
    if gbox.zmin > p.z {
        gbox.zmin = p.z;
    }
    if gbox.xmax < p.x {
        gbox.xmax = p.x;
    }
    if gbox.ymax < p.y {
        gbox.ymax = p.y;
    }
    if gbox.zmax < p.z {
        gbox.zmax = p.z;
    }
}

fn gbox_merge(merge_box: &mut Gbox, new_box: &Gbox) {
    if new_box.xmin < merge_box.xmin {
        merge_box.xmin = new_box.xmin;
    }
    if new_box.ymin < merge_box.ymin {
        merge_box.ymin = new_box.ymin;
    }
    if new_box.xmax > merge_box.xmax {
        merge_box.xmax = new_box.xmax;
    }
    if new_box.ymax > merge_box.ymax {
        merge_box.ymax = new_box.ymax;
    }
    if new_box.zmin < merge_box.zmin {
        merge_box.zmin = new_box.zmin;
    }
    if new_box.zmax > merge_box.zmax {
        merge_box.zmax = new_box.zmax;
    }
}

fn ll2cart(coord: &Coord<f64>) -> Point3d {
    let x_rad = std::f64::consts::PI * coord.x / 180.0;
    let y_rad = std::f64::consts::PI * coord.y / 180.0;
    let cos_y_rad = y_rad.cos();
    Point3d {
        x: cos_y_rad * x_rad.cos(),
        y: cos_y_rad * x_rad.sin(),
        z: y_rad.sin(),
    }
}

fn edge_calculate_gbox(a1: &Point3d, a2: &Point3d) -> Result<Gbox> {
    // Ported from PostGIS lwgeodetic.c:edge_calculate_gbox.
    let mut gbox = gbox_init_point3d(a1);
    gbox_merge_point3d(&mut gbox, a2);

    if p3d_same(a1, a2) {
        return Ok(gbox);
    }

    if fp_equals(a1.x, -a2.x) && fp_equals(a1.y, -a2.y) && fp_equals(a1.z, -a2.z) {
        return Err(ErrorCode::GeometryError(
            "Antipodal (180 degrees long) edge detected!".to_string(),
        ));
    }

    let an = unit_normal(a1, a2);
    let a3 = unit_normal(&an, a1);

    let r1 = Point2d { x: 1.0, y: 0.0 };
    let r2 = Point2d {
        x: dot_product(a2, a1),
        y: dot_product(a2, &a3),
    };

    let mut x = [Point3d {
        x: 0.0,
        y: 0.0,
        z: 0.0,
    }; 6];
    x[0].x = 1.0;
    x[1].x = -1.0;
    x[2].y = 1.0;
    x[3].y = -1.0;
    x[4].z = 1.0;
    x[5].z = -1.0;

    let o = Point2d { x: 0.0, y: 0.0 };
    let o_side = lw_segment_side(&r1, &r2, &o);

    for axis in &x {
        let mut rx = Point2d {
            x: dot_product(axis, a1),
            y: dot_product(axis, &a3),
        };
        normalize2d(&mut rx);

        if lw_segment_side(&r1, &r2, &rx) != o_side {
            let xn = Point3d {
                x: rx.x * a1.x + rx.y * a3.x,
                y: rx.x * a1.y + rx.y * a3.y,
                z: rx.x * a1.z + rx.y * a3.z,
            };
            gbox_merge_point3d(&mut gbox, &xn);
        }
    }

    Ok(gbox)
}

fn ptarray_calculate_gbox_geodetic(coords: &[Coord<f64>]) -> Result<Option<Gbox>> {
    if coords.is_empty() {
        return Ok(None);
    }

    if coords.len() == 1 {
        let point = ll2cart(&coords[0]);
        return Ok(Some(gbox_init_point3d(&point)));
    }

    let mut gbox: Option<Gbox> = None;
    let mut a1 = ll2cart(&coords[0]);
    for coord in coords.iter().skip(1) {
        let a2 = ll2cart(coord);
        let edge_gbox = edge_calculate_gbox(&a1, &a2)?;
        gbox = Some(match gbox {
            None => edge_gbox,
            Some(mut current) => {
                gbox_merge(&mut current, &edge_gbox);
                current
            }
        });
        a1 = a2;
    }
    Ok(gbox)
}

fn gbox_check_poles(gbox: &mut Gbox) {
    if gbox.xmin < 0.0 && gbox.xmax > 0.0 && gbox.ymin < 0.0 && gbox.ymax > 0.0 {
        if gbox.zmin > 0.0 && gbox.zmax > 0.0 {
            gbox.zmax = 1.0;
        } else if gbox.zmin < 0.0 && gbox.zmax < 0.0 {
            gbox.zmin = -1.0;
        } else {
            gbox.zmin = -1.0;
            gbox.zmax = 1.0;
        }
    }

    if gbox.xmin < 0.0 && gbox.xmax > 0.0 && gbox.zmin < 0.0 && gbox.zmax > 0.0 {
        if gbox.ymin > 0.0 && gbox.ymax > 0.0 {
            gbox.ymax = 1.0;
        } else if gbox.ymin < 0.0 && gbox.ymax < 0.0 {
            gbox.ymin = -1.0;
        } else {
            gbox.ymax = 1.0;
            gbox.ymin = -1.0;
        }
    }

    if gbox.ymin < 0.0 && gbox.ymax > 0.0 && gbox.zmin < 0.0 && gbox.zmax > 0.0 {
        if gbox.xmin > 0.0 && gbox.xmax > 0.0 {
            gbox.xmax = 1.0;
        } else if gbox.xmin < 0.0 && gbox.xmax < 0.0 {
            gbox.xmin = -1.0;
        } else {
            gbox.xmax = 1.0;
            gbox.xmin = -1.0;
        }
    }
}

fn geometry_gbox_geodetic(geo: &Geometry<f64>) -> Result<Option<Gbox>> {
    match geo {
        Geometry::Point(point) => {
            let coords = [point.0];
            ptarray_calculate_gbox_geodetic(&coords)
        }
        Geometry::MultiPoint(multi_point) => {
            let mut gbox: Option<Gbox> = None;
            for point in multi_point.0.iter() {
                let coords = [point.0];
                let point_box = ptarray_calculate_gbox_geodetic(&coords)?;
                if let Some(point_box) = point_box {
                    gbox = Some(match gbox {
                        None => point_box,
                        Some(mut current) => {
                            gbox_merge(&mut current, &point_box);
                            current
                        }
                    });
                }
            }
            Ok(gbox)
        }
        Geometry::Line(line) => {
            let coords = [line.start, line.end];
            ptarray_calculate_gbox_geodetic(&coords)
        }
        Geometry::LineString(line_string) => {
            ptarray_calculate_gbox_geodetic(line_string.0.as_slice())
        }
        Geometry::MultiLineString(multi_line) => {
            let mut gbox: Option<Gbox> = None;
            for line in multi_line.0.iter() {
                let line_box = ptarray_calculate_gbox_geodetic(line.0.as_slice())?;
                if let Some(line_box) = line_box {
                    gbox = Some(match gbox {
                        None => line_box,
                        Some(mut current) => {
                            gbox_merge(&mut current, &line_box);
                            current
                        }
                    });
                }
            }
            Ok(gbox)
        }
        Geometry::Polygon(polygon) => {
            if polygon.exterior().0.is_empty() {
                return Ok(None);
            }
            let mut gbox: Option<Gbox> = None;
            let exterior_box = ptarray_calculate_gbox_geodetic(polygon.exterior().0.as_slice())?
                .ok_or_else(|| {
                    ErrorCode::GeometryError("failed to compute polygon gbox".to_string())
                })?;
            gbox = Some(exterior_box);
            for ring in polygon.interiors() {
                let ring_box =
                    ptarray_calculate_gbox_geodetic(ring.0.as_slice())?.ok_or_else(|| {
                        ErrorCode::GeometryError("failed to compute polygon gbox".to_string())
                    })?;
                if let Some(mut current) = gbox {
                    gbox_merge(&mut current, &ring_box);
                    gbox = Some(current);
                }
            }
            if let Some(mut current) = gbox {
                gbox_check_poles(&mut current);
                Ok(Some(current))
            } else {
                Ok(None)
            }
        }
        Geometry::MultiPolygon(multi_polygon) => {
            let mut gbox: Option<Gbox> = None;
            for polygon in multi_polygon.0.iter() {
                let poly_box = geometry_gbox_geodetic(&Geometry::Polygon(polygon.clone()))?;
                if let Some(poly_box) = poly_box {
                    gbox = Some(match gbox {
                        None => poly_box,
                        Some(mut current) => {
                            gbox_merge(&mut current, &poly_box);
                            current
                        }
                    });
                }
            }
            Ok(gbox)
        }
        Geometry::Triangle(triangle) => {
            let polygon = triangle.to_polygon();
            geometry_gbox_geodetic(&Geometry::Polygon(polygon))
        }
        Geometry::Rect(rect) => {
            let polygon = rect.to_polygon();
            geometry_gbox_geodetic(&Geometry::Polygon(polygon))
        }
        Geometry::GeometryCollection(collection) => {
            let mut gbox: Option<Gbox> = None;
            for geom in collection.0.iter() {
                let geom_box = geometry_gbox_geodetic(geom)?;
                if let Some(geom_box) = geom_box {
                    gbox = Some(match gbox {
                        None => geom_box,
                        Some(mut current) => {
                            gbox_merge(&mut current, &geom_box);
                            current
                        }
                    });
                }
            }
            Ok(gbox)
        }
    }
}

fn coord_in_geodetic(coord: &Coord<f64>) -> bool {
    coord.x >= -180.0 && coord.x <= 180.0 && coord.y >= -90.0 && coord.y <= 90.0
}

fn geometry_check_geodetic(geo: &Geometry<f64>) -> bool {
    for coord in geo.coords_iter() {
        if !coord_in_geodetic(&coord) {
            return false;
        }
    }
    true
}

pub fn geocentric_bounds_from_geo(geo: &Geometry<f64>) -> Result<Option<Gbox>> {
    if !geometry_check_geodetic(geo) {
        return Err(ErrorCode::GeometryError(
            "geography coordinates out of range".to_string(),
        ));
    }
    geometry_gbox_geodetic(geo)
}

pub fn geocentric_bounds_from_geos(geos: &[Geometry<f64>]) -> Result<Option<Gbox>> {
    let mut gbox: Option<Gbox> = None;
    for geo in geos {
        if !geometry_check_geodetic(geo) {
            return Err(ErrorCode::GeometryError(
                "geography coordinates out of range".to_string(),
            ));
        }
        if let Some(next) = geometry_gbox_geodetic(geo)? {
            gbox = Some(match gbox {
                None => next,
                Some(mut current) => {
                    gbox_merge(&mut current, &next);
                    current
                }
            });
        }
    }
    Ok(gbox)
}

fn longitude_degrees_normalize(lon: f64) -> f64 {
    let mut val = lon;
    while val > 180.0 {
        val -= 360.0;
    }
    while val < -180.0 {
        val += 360.0;
    }
    val
}

fn latitude_degrees_normalize(lat: f64) -> f64 {
    let mut val = lat;
    while val > 90.0 {
        val = 180.0 - val;
    }
    while val < -90.0 {
        val = -180.0 - val;
    }
    val
}

pub fn gbox_centroid(gbox: &Gbox) -> (f64, f64) {
    // Ported from PostGIS lwgeodetic.c:gbox_centroid.
    let d = [
        gbox.xmin, gbox.xmax, gbox.ymin, gbox.ymax, gbox.zmin, gbox.zmax,
    ];

    let mut sum = Point3d {
        x: 0.0,
        y: 0.0,
        z: 0.0,
    };
    for i in 0..8 {
        let mut pt = Point3d {
            x: d[i / 4],
            y: d[2 + ((i % 4) / 2)],
            z: d[4 + (i % 2)],
        };
        normalize(&mut pt);
        sum.x += pt.x;
        sum.y += pt.y;
        sum.z += pt.z;
    }

    normalize(&mut sum);
    let lon = sum.y.atan2(sum.x).to_degrees();
    let lat = sum.z.asin().to_degrees();
    (
        longitude_degrees_normalize(lon),
        latitude_degrees_normalize(lat),
    )
}

pub fn gbox_angular_height(gbox: &Gbox) -> f64 {
    // Ported from PostGIS lwgeodetic.c:gbox_angular_height.
    let d = [
        gbox.xmin, gbox.xmax, gbox.ymin, gbox.ymax, gbox.zmin, gbox.zmax,
    ];

    let mut zmin = f64::MAX;
    let mut zmax = f64::MIN;
    for i in 0..8 {
        let mut pt = Point3d {
            x: d[i / 4],
            y: d[2 + ((i % 4) / 2)],
            z: d[4 + (i % 2)],
        };
        normalize(&mut pt);
        zmin = zmin.min(pt.z);
        zmax = zmax.max(pt.z);
    }
    zmax.asin() - zmin.asin()
}

pub fn gbox_angular_width(gbox: &Gbox) -> f64 {
    // Ported from PostGIS lwgeodetic.c:gbox_angular_width.
    let d = [
        gbox.xmin, gbox.xmax, gbox.ymin, gbox.ymax, gbox.zmin, gbox.zmax,
    ];

    let mut pt = [
        Point3d {
            x: gbox.xmin,
            y: gbox.ymin,
            z: 0.0,
        },
        Point3d {
            x: 0.0,
            y: 0.0,
            z: 0.0,
        },
        Point3d {
            x: 0.0,
            y: 0.0,
            z: 0.0,
        },
    ];

    let magnitude = (pt[0].x * pt[0].x + pt[0].y * pt[0].y).sqrt();
    if magnitude == 0.0 {
        return 0.0;
    }
    pt[0].x /= magnitude;
    pt[0].y /= magnitude;

    let mut maxangle = f64::MIN;
    for j in 0..2 {
        maxangle = f64::MIN;
        for i in 0..4 {
            let mut pt_n = Point3d {
                x: d[i / 2],
                y: d[2 + (i % 2)],
                z: 0.0,
            };
            let magnitude = (pt_n.x * pt_n.x + pt_n.y * pt_n.y).sqrt();
            if magnitude == 0.0 {
                continue;
            }
            pt_n.x /= magnitude;
            pt_n.y /= magnitude;

            let dotprod = pt_n.x * pt[j].x + pt_n.y * pt[j].y;
            let angle = (dotprod.min(1.0)).acos();
            if angle > maxangle {
                pt[j + 1] = pt_n;
                maxangle = angle;
            }
        }
    }
    if maxangle == f64::MIN { 0.0 } else { maxangle }
}
