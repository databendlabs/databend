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

// Portions of this file are adapted from GEOS:
// - geos/src/geom/HeuristicOverlay.cpp
// - geos/src/operation/overlayng/OverlayNGRobust.cpp
// Licensed under LGPL-2.1-or-later.
// Modifications by Databend.

use databend_common_exception::Result;
use geo::BoundingRect;
use geo::Contains;
use geo::Coord;
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
use geo::Triangle;
use geo::algorithm::bool_ops::BooleanOps;
use geo::algorithm::bool_ops::unary_union;
use geo::algorithm::line_intersection::LineIntersection;
use geo::algorithm::line_intersection::line_intersection;
use geo::dimensions::Dimensions;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeuristicOverlayMode {
    Intersection,
    Union,
    Difference,
    SymDifference,
}

pub struct HeuristicOverlay {
    mode: HeuristicOverlayMode,
}

impl HeuristicOverlay {
    pub fn new(mode: HeuristicOverlayMode) -> Self {
        Self { mode }
    }

    pub fn apply(&self, g0: &Geometry<f64>, g1: &Geometry<f64>) -> Result<Geometry<f64>> {
        if is_combinable(g0, g1) && self.is_union_like() {
            return Ok(combine_reduced(g0, g1));
        }

        StructuredCollection::overlay(g0, g1, self.mode)
    }

    fn is_union_like(&self) -> bool {
        matches!(
            self.mode,
            HeuristicOverlayMode::Union | HeuristicOverlayMode::SymDifference
        )
    }
}

fn polygonal_geometry(geo: &Geometry<f64>) -> Option<MultiPolygon<f64>> {
    match geo {
        Geometry::Polygon(polygon) => Some(MultiPolygon::from(polygon.clone())),
        Geometry::MultiPolygon(multi_polygon) => Some(multi_polygon.clone()),
        Geometry::Rect(rect) => Some(MultiPolygon::from(rect.to_polygon())),
        Geometry::Triangle(triangle) => Some(MultiPolygon::from(triangle.to_polygon())),
        Geometry::GeometryCollection(collection) => {
            let mut polygons = Vec::new();
            for geom in &collection.0 {
                match geom {
                    Geometry::Polygon(polygon) => polygons.push(polygon.clone()),
                    Geometry::MultiPolygon(multi_polygon) => {
                        polygons.extend(multi_polygon.0.clone());
                    }
                    Geometry::Rect(rect) => polygons.push(rect.to_polygon()),
                    Geometry::Triangle(triangle) => polygons.push(triangle.to_polygon()),
                    _ => return None,
                }
            }
            Some(MultiPolygon(polygons))
        }
        _ => None,
    }
}

fn empty_geometry() -> Geometry<f64> {
    Geometry::GeometryCollection(GeometryCollection::empty())
}

fn is_combinable(g0: &Geometry<f64>, g1: &Geometry<f64>) -> bool {
    if is_empty_geometry(g0) && is_empty_geometry(g1) {
        return false;
    }
    let Some(r0) = g0.bounding_rect() else {
        return false;
    };
    let Some(r1) = g1.bounding_rect() else {
        return false;
    };
    if r0.intersects(&r1) {
        return false;
    }

    has_single_non_empty_element(g0) && has_single_non_empty_element(g1)
}

fn is_empty_geometry(geom: &Geometry<f64>) -> bool {
    match geom {
        Geometry::Point(_) => false,
        Geometry::Line(_) => false,
        Geometry::LineString(line_string) => line_string.0.is_empty(),
        Geometry::Polygon(polygon) => polygon.exterior().0.is_empty(),
        Geometry::MultiPoint(multi_point) => multi_point.0.is_empty(),
        Geometry::MultiLineString(multi_line) => multi_line.0.is_empty(),
        Geometry::MultiPolygon(multi_poly) => multi_poly.0.is_empty(),
        Geometry::Rect(_) => false,
        Geometry::Triangle(_) => false,
        Geometry::GeometryCollection(collection) => collection.0.iter().all(is_empty_geometry),
    }
}

fn has_single_non_empty_element(geom: &Geometry<f64>) -> bool {
    if let Geometry::GeometryCollection(collection) = geom {
        let mut found_non_empty = false;
        for sub in &collection.0 {
            if has_single_non_empty_element(sub) {
                if found_non_empty {
                    return false;
                }
                found_non_empty = true;
            }
        }
        return found_non_empty;
    }
    !is_empty_geometry(geom)
}

fn extract_elements(geom: &Geometry<f64>, out: &mut Vec<Geometry<f64>>) {
    match geom {
        Geometry::GeometryCollection(collection) => {
            for sub in &collection.0 {
                extract_elements(sub, out);
            }
        }
        Geometry::MultiPoint(multi_point) => {
            for point in &multi_point.0 {
                out.push(Geometry::Point(*point));
            }
        }
        Geometry::MultiLineString(multi_line) => {
            for line in &multi_line.0 {
                if !line.0.is_empty() {
                    out.push(Geometry::LineString(line.clone()));
                }
            }
        }
        Geometry::MultiPolygon(multi_polygon) => {
            for polygon in &multi_polygon.0 {
                if !polygon.exterior().0.is_empty() {
                    out.push(Geometry::Polygon(polygon.clone()));
                }
            }
        }
        Geometry::Rect(rect) => {
            out.push(Geometry::Polygon(rect.to_polygon()));
        }
        Geometry::Triangle(triangle) => {
            out.push(Geometry::Polygon(triangle.to_polygon()));
        }
        _ => {
            if !is_empty_geometry(geom) {
                out.push(geom.clone());
            }
        }
    }
}

fn combine_reduced(g0: &Geometry<f64>, g1: &Geometry<f64>) -> Geometry<f64> {
    let mut elements = Vec::new();
    extract_elements(g0, &mut elements);
    extract_elements(g1, &mut elements);
    build_geometry_from_elements(elements)
}

fn build_geometry_from_elements(elements: Vec<Geometry<f64>>) -> Geometry<f64> {
    if elements.is_empty() {
        return Geometry::GeometryCollection(GeometryCollection::empty());
    }

    if elements.iter().all(|geo| matches!(geo, Geometry::Point(_))) {
        let points: Vec<Point<f64>> = elements
            .into_iter()
            .map(|geo| geo.try_into().expect("point geometry"))
            .collect();
        return Geometry::MultiPoint(MultiPoint::from_iter(points));
    }
    if elements
        .iter()
        .all(|geo| matches!(geo, Geometry::LineString(_)))
    {
        let lines: Vec<LineString<f64>> = elements
            .into_iter()
            .map(|geo| geo.try_into().expect("linestring geometry"))
            .collect();
        return Geometry::MultiLineString(MultiLineString::from_iter(lines));
    }
    if elements
        .iter()
        .all(|geo| matches!(geo, Geometry::Polygon(_)))
    {
        let polygons: Vec<Polygon<f64>> = elements
            .into_iter()
            .map(|geo| geo.try_into().expect("polygon geometry"))
            .collect();
        return Geometry::MultiPolygon(MultiPolygon::from_iter(polygons));
    }

    Geometry::GeometryCollection(GeometryCollection::from_iter(elements))
}

const LINE_OVERLAY_EPS: f64 = 1e-9;

struct StructuredCollection {
    points: Vec<Point<f64>>,
    lines: Vec<LineString<f64>>,
    polys: Vec<Polygon<f64>>,
    pt_union: Vec<Point<f64>>,
    line_union: Vec<LineString<f64>>,
    poly_union: Vec<Polygon<f64>>,
    dimension: Dimensions,
}

impl StructuredCollection {
    fn new(geom: &Geometry<f64>) -> Result<Self> {
        let mut collection = Self::empty();
        collection.read_collection(geom);
        collection.union_by_dimension()?;
        Ok(collection)
    }

    fn empty() -> Self {
        Self {
            points: Vec::new(),
            lines: Vec::new(),
            polys: Vec::new(),
            pt_union: Vec::new(),
            line_union: Vec::new(),
            poly_union: Vec::new(),
            dimension: Dimensions::Empty,
        }
    }

    fn overlay(
        g0: &Geometry<f64>,
        g1: &Geometry<f64>,
        mode: HeuristicOverlayMode,
    ) -> Result<Geometry<f64>> {
        let s0 = StructuredCollection::new(g0)?;
        let s1 = StructuredCollection::new(g1)?;
        match mode {
            HeuristicOverlayMode::Union => s0.do_union(&s1),
            HeuristicOverlayMode::Intersection => s0.do_intersection(&s1),
            HeuristicOverlayMode::Difference => s0.do_difference(&s1),
            HeuristicOverlayMode::SymDifference => s0.do_symdifference(&s1),
        }
    }

    fn add_dimension(&mut self, dim: Dimensions) {
        if dim == Dimensions::Empty {
            return;
        }
        if dimension_rank(dim) > dimension_rank(self.dimension) {
            self.dimension = dim;
        }
    }

    fn read_collection(&mut self, geom: &Geometry<f64>) {
        match geom {
            Geometry::GeometryCollection(collection) => {
                for sub in &collection.0 {
                    self.read_collection(sub);
                }
            }
            Geometry::Point(point) => {
                self.points.push(*point);
                self.add_dimension(Dimensions::ZeroDimensional);
            }
            Geometry::MultiPoint(multi_point) => {
                if multi_point.0.is_empty() {
                    return;
                }
                self.points.extend(multi_point.0.iter().copied());
                self.add_dimension(Dimensions::ZeroDimensional);
            }
            Geometry::Line(line) => {
                let line = LineString::from(vec![line.start, line.end]);
                if line.0.is_empty() {
                    return;
                }
                self.lines.push(line);
                self.add_dimension(Dimensions::OneDimensional);
            }
            Geometry::LineString(line_string) => {
                if line_string.0.is_empty() {
                    return;
                }
                self.lines.push(line_string.clone());
                self.add_dimension(Dimensions::OneDimensional);
            }
            Geometry::MultiLineString(multi_line) => {
                if multi_line.0.is_empty() {
                    return;
                }
                for line in &multi_line.0 {
                    if !line.0.is_empty() {
                        self.lines.push(line.clone());
                    }
                }
                if !multi_line.0.is_empty() {
                    self.add_dimension(Dimensions::OneDimensional);
                }
            }
            Geometry::Polygon(polygon) => {
                if polygon.exterior().0.is_empty() {
                    return;
                }
                self.polys.push(polygon.clone());
                self.add_dimension(Dimensions::TwoDimensional);
            }
            Geometry::MultiPolygon(multi_polygon) => {
                if multi_polygon.0.is_empty() {
                    return;
                }
                for polygon in &multi_polygon.0 {
                    if !polygon.exterior().0.is_empty() {
                        self.polys.push(polygon.clone());
                    }
                }
                if !multi_polygon.0.is_empty() {
                    self.add_dimension(Dimensions::TwoDimensional);
                }
            }
            Geometry::Rect(rect) => {
                self.polys.push(rect.to_polygon());
                self.add_dimension(Dimensions::TwoDimensional);
            }
            Geometry::Triangle(triangle) => {
                self.polys.push(triangle.to_polygon());
                self.add_dimension(Dimensions::TwoDimensional);
            }
        }
    }

    fn read_collection_geometry(&mut self, geom: Option<Geometry<f64>>) {
        if let Some(geom) = geom {
            self.read_collection(&geom);
        }
    }

    fn union_by_dimension(&mut self) -> Result<()> {
        self.pt_union = union_points(self.points.clone());
        self.line_union = union_lines(self.lines.clone())?;
        self.poly_union = union_polys(self.polys.clone())?;
        Ok(())
    }

    fn compute_result(&self, coll: &mut StructuredCollection) -> Result<Geometry<f64>> {
        coll.union_by_dimension()?;
        coll.do_unary_union()
    }

    fn do_unary_union(&self) -> Result<Geometry<f64>> {
        let mut points = self.pt_union.clone();
        let mut lines = self.line_union.clone();
        let polys = self.poly_union.clone();

        if !lines.is_empty() {
            points = difference_points_by_lines(points, &lines);
        }
        if !polys.is_empty() {
            points = difference_points_by_polys(points, &polys);
            lines = difference_lines_by_polys(lines, &polys)?;
        }

        let mut outputs = Vec::new();
        if let Some(geom) = geometry_from_polys(&polys) {
            outputs.push(geom);
        }
        if let Some(geom) = geometry_from_lines(&lines) {
            outputs.push(geom);
        }
        if let Some(geom) = geometry_from_points(&points) {
            outputs.push(geom);
        }

        match outputs.len() {
            0 => Ok(empty_geometry()),
            1 => Ok(outputs.remove(0)),
            _ => Ok(build_geometry_from_elements(outputs)),
        }
    }

    fn do_union(&self, other: &StructuredCollection) -> Result<Geometry<f64>> {
        let mut polys = self.poly_union.clone();
        polys.extend(other.poly_union.clone());
        let polys = union_polys(polys)?;

        let mut lines = self.line_union.clone();
        lines.extend(other.line_union.clone());
        let lines = union_lines(lines)?;

        let mut points = self.pt_union.clone();
        points.extend(other.pt_union.clone());
        let points = union_points(points);

        let mut coll = StructuredCollection::empty();
        coll.read_collection_geometry(geometry_from_polys(&polys));
        coll.read_collection_geometry(geometry_from_lines(&lines));
        coll.read_collection_geometry(geometry_from_points(&points));
        self.compute_result(&mut coll)
    }

    fn do_intersection(&self, other: &StructuredCollection) -> Result<Geometry<f64>> {
        let mut coll = StructuredCollection::empty();

        coll.read_collection_geometry(intersection_geometries(
            geometry_from_polys(&self.poly_union),
            geometry_from_polys(&other.poly_union),
        )?);

        coll.read_collection_geometry(intersection_geometries(
            geometry_from_polys(&self.poly_union),
            geometry_from_lines(&other.line_union),
        )?);
        coll.read_collection_geometry(intersection_geometries(
            geometry_from_polys(&self.poly_union),
            geometry_from_points(&other.pt_union),
        )?);

        coll.read_collection_geometry(intersection_geometries(
            geometry_from_lines(&self.line_union),
            geometry_from_polys(&other.poly_union),
        )?);
        coll.read_collection_geometry(intersection_geometries(
            geometry_from_lines(&self.line_union),
            geometry_from_lines(&other.line_union),
        )?);

        coll.read_collection_geometry(intersection_points_lines_geometry(
            &other.pt_union,
            &self.line_union,
        ));

        coll.read_collection_geometry(intersection_geometries(
            geometry_from_points(&self.pt_union),
            geometry_from_polys(&other.poly_union),
        )?);

        coll.read_collection_geometry(intersection_points_lines_geometry(
            &self.pt_union,
            &other.line_union,
        ));

        coll.read_collection_geometry(intersection_points_geometry(
            &self.pt_union,
            &other.pt_union,
        ));

        self.compute_result(&mut coll)
    }

    fn do_difference(&self, other: &StructuredCollection) -> Result<Geometry<f64>> {
        let mut coll = StructuredCollection::empty();

        let poly_diff = difference_polys(&self.poly_union, &other.poly_union)?;
        coll.read_collection_geometry(geometry_from_polys(&poly_diff));

        let line_diff_poly = difference_lines_by_polys(self.line_union.clone(), &other.poly_union)?;
        let line_diff = difference_lines(&line_diff_poly, &other.line_union);
        coll.read_collection_geometry(geometry_from_lines(&line_diff));

        let pt_diff_poly = difference_points_by_polys(self.pt_union.clone(), &other.poly_union);
        let pt_diff_lines = difference_points_by_lines(pt_diff_poly, &line_diff);
        let pt_diff = difference_points(pt_diff_lines, &other.pt_union);
        coll.read_collection_geometry(geometry_from_points(&pt_diff));

        self.compute_result(&mut coll)
    }

    fn do_symdifference(&self, other: &StructuredCollection) -> Result<Geometry<f64>> {
        let mut coll = StructuredCollection::empty();

        let poly_symdiff = symdifference_polys(&self.poly_union, &other.poly_union)?;
        coll.read_collection_geometry(geometry_from_polys(&poly_symdiff));

        let line_symdiff = difference_lines(&self.line_union, &other.line_union);
        coll.read_collection_geometry(geometry_from_lines(&line_symdiff));

        let pt_symdiff = difference_points(self.pt_union.clone(), &other.pt_union);
        coll.read_collection_geometry(geometry_from_points(&pt_symdiff));

        self.compute_result(&mut coll)
    }
}

fn geometry_from_points(points: &[Point<f64>]) -> Option<Geometry<f64>> {
    match points.len() {
        0 => None,
        1 => Some(Geometry::Point(points[0])),
        _ => Some(Geometry::MultiPoint(MultiPoint::from_iter(
            points.iter().copied(),
        ))),
    }
}

fn geometry_from_lines(lines: &[LineString<f64>]) -> Option<Geometry<f64>> {
    match lines.len() {
        0 => None,
        1 => Some(Geometry::LineString(lines[0].clone())),
        _ => Some(Geometry::MultiLineString(MultiLineString::new(
            lines.to_vec(),
        ))),
    }
}

fn geometry_from_polys(polys: &[Polygon<f64>]) -> Option<Geometry<f64>> {
    match polys.len() {
        0 => None,
        1 => Some(Geometry::Polygon(polys[0].clone())),
        _ => Some(Geometry::MultiPolygon(MultiPolygon::new(polys.to_vec()))),
    }
}

fn union_points(points: Vec<Point<f64>>) -> Vec<Point<f64>> {
    dedup_points(points)
}

fn union_lines(lines: Vec<LineString<f64>>) -> Result<Vec<LineString<f64>>> {
    if lines.is_empty() {
        return Ok(Vec::new());
    }
    let merged = merge_collinear_lines(lines);
    let split = split_lines_at_intersections(merged);
    let merged = merge_collinear_lines(split);
    Ok(dedup_lines(merged))
}

fn union_polys(polys: Vec<Polygon<f64>>) -> Result<Vec<Polygon<f64>>> {
    if polys.is_empty() {
        return Ok(Vec::new());
    }
    let merged = unary_union(&polys);
    Ok(merged.into_iter().collect())
}

fn intersection_geometries(
    g0: Option<Geometry<f64>>,
    g1: Option<Geometry<f64>>,
) -> Result<Option<Geometry<f64>>> {
    let (Some(left), Some(right)) = (g0, g1) else {
        return Ok(None);
    };
    intersection_geometry_pair(&left, &right)
}

fn intersection_points_geometry(
    left: &[Point<f64>],
    right: &[Point<f64>],
) -> Option<Geometry<f64>> {
    geometry_from_points(&intersection_points(left, right))
}

fn intersection_points_lines_geometry(
    points: &[Point<f64>],
    lines: &[LineString<f64>],
) -> Option<Geometry<f64>> {
    if points.is_empty() || lines.is_empty() {
        return None;
    }
    let filtered: Vec<Point<f64>> = points
        .iter()
        .copied()
        .filter(|point| lines.iter().any(|line| line.contains(point)))
        .collect();
    geometry_from_points(&dedup_points(filtered))
}

fn intersection_geometry_pair(
    left: &Geometry<f64>,
    right: &Geometry<f64>,
) -> Result<Option<Geometry<f64>>> {
    if let (Some(left_polys), Some(right_polys)) =
        (polygonal_geometry(left), polygonal_geometry(right))
    {
        let inter = left_polys.intersection(&right_polys);
        return Ok(geometry_from_polys(&inter.0));
    }

    let mut left_lines = Vec::new();
    extract_lines(left, &mut left_lines);
    let mut right_lines = Vec::new();
    extract_lines(right, &mut right_lines);

    if let (Some(left_polys), true) = (polygonal_geometry(left), right_lines.is_empty()) {
        let mut right_points = Vec::new();
        extract_points(right, &mut right_points);
        let points: Vec<Point<f64>> = right_points
            .into_iter()
            .filter(|point| left_polys.iter().any(|poly| poly.intersects(point)))
            .collect();
        return Ok(geometry_from_points(&dedup_points(points)));
    }

    if let (Some(right_polys), true) = (polygonal_geometry(right), left_lines.is_empty()) {
        let mut left_points = Vec::new();
        extract_points(left, &mut left_points);
        let points: Vec<Point<f64>> = left_points
            .into_iter()
            .filter(|point| right_polys.iter().any(|poly| poly.intersects(point)))
            .collect();
        return Ok(geometry_from_points(&dedup_points(points)));
    }

    if let (Some(left_polys), false) = (polygonal_geometry(left), right_lines.is_empty()) {
        let lines = clip_lines_to_polys(right_lines, &left_polys.0);
        return Ok(geometry_from_lines(&dedup_lines(lines)));
    }

    if let (Some(right_polys), false) = (polygonal_geometry(right), left_lines.is_empty()) {
        let lines = clip_lines_to_polys(left_lines, &right_polys.0);
        return Ok(geometry_from_lines(&dedup_lines(lines)));
    }

    if !left_lines.is_empty() && !right_lines.is_empty() {
        return Ok(intersection_lines_geometry(&left_lines, &right_lines));
    }

    let mut left_points = Vec::new();
    extract_points(left, &mut left_points);
    let mut right_points = Vec::new();
    extract_points(right, &mut right_points);

    if !left_points.is_empty() && !right_points.is_empty() {
        return Ok(geometry_from_points(&intersection_points(
            &left_points,
            &right_points,
        )));
    }

    if !left_points.is_empty() && !right_lines.is_empty() {
        return Ok(intersection_points_lines_geometry(
            &left_points,
            &right_lines,
        ));
    }

    if !right_points.is_empty() && !left_lines.is_empty() {
        return Ok(intersection_points_lines_geometry(
            &right_points,
            &left_lines,
        ));
    }

    Ok(None)
}

fn difference_polys(left: &[Polygon<f64>], right: &[Polygon<f64>]) -> Result<Vec<Polygon<f64>>> {
    if left.is_empty() {
        return Ok(Vec::new());
    }
    if right.is_empty() {
        return Ok(left.to_vec());
    }
    let left_mp = MultiPolygon::new(left.to_vec());
    let right_mp = MultiPolygon::new(right.to_vec());
    Ok(left_mp.difference(&right_mp).0)
}

fn symdifference_polys(left: &[Polygon<f64>], right: &[Polygon<f64>]) -> Result<Vec<Polygon<f64>>> {
    if left.is_empty() && right.is_empty() {
        return Ok(Vec::new());
    }
    if left.is_empty() {
        return Ok(right.to_vec());
    }
    if right.is_empty() {
        return Ok(left.to_vec());
    }
    let left_mp = MultiPolygon::new(left.to_vec());
    let right_mp = MultiPolygon::new(right.to_vec());
    Ok(left_mp.xor(&right_mp).0)
}

fn difference_lines_by_polys(
    lines: Vec<LineString<f64>>,
    polys: &[Polygon<f64>],
) -> Result<Vec<LineString<f64>>> {
    if lines.is_empty() {
        return Ok(Vec::new());
    }
    if polys.is_empty() {
        return Ok(lines);
    }
    let mut outside = lines;
    for poly in polys {
        let mut next = Vec::new();
        for line in outside {
            let (_, outside_parts) = split_line_by_polygon(&line, poly);
            next.extend(outside_parts);
        }
        outside = next;
        if outside.is_empty() {
            break;
        }
    }
    Ok(outside)
}

fn difference_lines(
    lines: &[LineString<f64>],
    cutters: &[LineString<f64>],
) -> Vec<LineString<f64>> {
    if cutters.is_empty() {
        return lines.to_vec();
    }

    let mut result = Vec::new();
    for line in lines {
        let segments = split_line_by_cutters(line, cutters);
        for segment in segments {
            if segment_is_on_lines(&segment, cutters) {
                continue;
            }
            result.push(segment);
        }
    }

    result
}

fn difference_points(points: Vec<Point<f64>>, cutters: &[Point<f64>]) -> Vec<Point<f64>> {
    if cutters.is_empty() {
        return dedup_points(points);
    }
    let cutters = dedup_points(cutters.to_vec());
    let filtered: Vec<Point<f64>> = points
        .into_iter()
        .filter(|point| !cutters.iter().any(|cut| point_eq(*point, *cut)))
        .collect();
    dedup_points(filtered)
}

fn difference_points_by_lines(
    points: Vec<Point<f64>>,
    lines: &[LineString<f64>],
) -> Vec<Point<f64>> {
    if lines.is_empty() {
        return dedup_points(points);
    }
    let filtered: Vec<Point<f64>> = points
        .into_iter()
        .filter(|point| !lines.iter().any(|line| line.contains(point)))
        .collect();
    dedup_points(filtered)
}

fn difference_points_by_polys(points: Vec<Point<f64>>, polys: &[Polygon<f64>]) -> Vec<Point<f64>> {
    if polys.is_empty() {
        return dedup_points(points);
    }
    let filtered: Vec<Point<f64>> = points
        .into_iter()
        .filter(|point| !polys.iter().any(|poly| poly.contains(point)))
        .collect();
    dedup_points(filtered)
}

fn intersection_points(left: &[Point<f64>], right: &[Point<f64>]) -> Vec<Point<f64>> {
    if left.is_empty() || right.is_empty() {
        return Vec::new();
    }
    let left = dedup_points(left.to_vec());
    let right = dedup_points(right.to_vec());
    left.into_iter()
        .filter(|point| right.iter().any(|other| point_eq(*point, *other)))
        .collect()
}

fn intersection_lines_geometry(
    left: &[LineString<f64>],
    right: &[LineString<f64>],
) -> Option<Geometry<f64>> {
    if left.is_empty() || right.is_empty() {
        return None;
    }

    let mut overlap_segments = Vec::new();
    for line in left {
        let segments = split_line_by_cutters(line, right);
        for segment in segments {
            if segment_is_on_lines(&segment, right) {
                overlap_segments.push(segment);
            }
        }
    }

    let mut points = intersection_points_from_lines(left, right);
    points = dedup_points(points);

    let mut outputs = Vec::new();
    if let Some(geom) = geometry_from_lines(&dedup_lines(overlap_segments)) {
        outputs.push(geom);
    }
    if let Some(geom) = geometry_from_points(&points) {
        outputs.push(geom);
    }

    match outputs.len() {
        0 => None,
        1 => Some(outputs.remove(0)),
        _ => Some(build_geometry_from_elements(outputs)),
    }
}

fn intersection_points_from_lines(
    left: &[LineString<f64>],
    right: &[LineString<f64>],
) -> Vec<Point<f64>> {
    let mut points = Vec::new();
    for line in left {
        let segments_left = line_segments(line);
        for other in right {
            let segments_right = line_segments(other);
            for seg_a in &segments_left {
                for seg_b in &segments_right {
                    let line_a = Line::new(seg_a.0, seg_a.1);
                    let line_b = Line::new(seg_b.0, seg_b.1);
                    let Some(intersection) = line_intersection(line_a, line_b) else {
                        continue;
                    };
                    if let LineIntersection::SinglePoint { intersection, .. } = intersection {
                        points.push(Point(intersection));
                    }
                }
            }
        }
    }
    points
}

fn merge_collinear_lines(lines: Vec<LineString<f64>>) -> Vec<LineString<f64>> {
    let mut merged_lines = Vec::new();
    let mut non_linear_lines = Vec::new();
    let mut groups: Vec<LineGroup> = Vec::new();

    for line in lines {
        let (origin, dir) = match line_direction(&line) {
            Some(res) => res,
            None => {
                non_linear_lines.push(line);
                continue;
            }
        };

        if !line_is_collinear(&line, origin, dir) {
            non_linear_lines.push(line);
            continue;
        }

        let mut inserted = false;
        for group in groups.iter_mut() {
            if line_belongs_to_group(&origin, &dir, group) {
                group.add_line(&line);
                inserted = true;
                break;
            }
        }
        if !inserted {
            let mut group = LineGroup::new(origin, dir);
            group.add_line(&line);
            groups.push(group);
        }
    }

    for group in groups {
        merged_lines.extend(group.merge());
    }

    merged_lines.extend(non_linear_lines);
    merged_lines
}

fn split_lines_at_intersections(lines: Vec<LineString<f64>>) -> Vec<LineString<f64>> {
    if lines.len() < 2 {
        return lines;
    }

    let mut segments = Vec::new();
    for line in lines.iter() {
        if line.0.len() < 2 {
            continue;
        }
        for seg_idx in 0..(line.0.len() - 1) {
            let start = line.0[seg_idx];
            let end = line.0[seg_idx + 1];
            if !coord_eq(start, end) {
                segments.push(SegmentInfo { start, end });
            }
        }
    }

    let mut split_params: Vec<Vec<f64>> = vec![Vec::new(); segments.len()];
    for params in split_params.iter_mut() {
        params.push(0.0);
        params.push(1.0);
    }

    for i in 0..segments.len() {
        for j in (i + 1)..segments.len() {
            let seg_i = &segments[i];
            let seg_j = &segments[j];
            let line_i = Line::new(seg_i.start, seg_i.end);
            let line_j = Line::new(seg_j.start, seg_j.end);
            let Some(intersection) = line_intersection(line_i, line_j) else {
                continue;
            };

            let point = match intersection {
                LineIntersection::SinglePoint { intersection, .. } => intersection,
                LineIntersection::Collinear { .. } => continue,
            };

            if let Some(t_i) = line_param(seg_i.start, seg_i.end, point) {
                if (-LINE_OVERLAY_EPS..=1.0 + LINE_OVERLAY_EPS).contains(&t_i) {
                    split_params[i].push(t_i.clamp(0.0, 1.0));
                }
            }
            if let Some(t_j) = line_param(seg_j.start, seg_j.end, point) {
                if (-LINE_OVERLAY_EPS..=1.0 + LINE_OVERLAY_EPS).contains(&t_j) {
                    split_params[j].push(t_j.clamp(0.0, 1.0));
                }
            }
        }
    }

    let mut out = Vec::new();
    for (seg_idx, seg) in segments.iter().enumerate() {
        let mut params = split_params[seg_idx].clone();
        params.sort_by(|a, b| a.partial_cmp(b).unwrap());
        params.dedup_by(|a, b| (*a - *b).abs() <= LINE_OVERLAY_EPS);

        for win in params.windows(2) {
            let t0 = win[0];
            let t1 = win[1];
            if t1 - t0 <= LINE_OVERLAY_EPS {
                continue;
            }
            let start = interp(seg.start, seg.end, t0);
            let end = interp(seg.start, seg.end, t1);
            if coord_eq(start, end) {
                continue;
            }
            out.push(LineString::from(vec![start, end]));
        }
    }

    if out.is_empty() {
        return lines;
    }

    out
}

fn clip_lines_to_polys(
    lines: Vec<LineString<f64>>,
    polys: &[Polygon<f64>],
) -> Vec<LineString<f64>> {
    if polys.is_empty() {
        return Vec::new();
    }
    let mut inside = Vec::new();
    for line in lines {
        for poly in polys {
            let (inside_parts, _) = split_line_by_polygon(&line, poly);
            inside.extend(inside_parts);
        }
    }
    inside
}

fn extract_points(geom: &Geometry<f64>, out: &mut Vec<Point<f64>>) {
    match geom {
        Geometry::Point(point) => out.push(*point),
        Geometry::MultiPoint(multi_point) => out.extend(multi_point.0.iter().copied()),
        Geometry::GeometryCollection(collection) => {
            for sub in &collection.0 {
                extract_points(sub, out);
            }
        }
        _ => {}
    }
}

fn extract_polys(geom: &Geometry<f64>, out: &mut Vec<Polygon<f64>>) {
    match geom {
        Geometry::Polygon(polygon) => out.push(polygon.clone()),
        Geometry::MultiPolygon(multi_polygon) => out.extend(multi_polygon.0.iter().cloned()),
        Geometry::Rect(rect) => out.push(rect.to_polygon()),
        Geometry::Triangle(triangle) => out.push(triangle.to_polygon()),
        Geometry::GeometryCollection(collection) => {
            for sub in &collection.0 {
                extract_polys(sub, out);
            }
        }
        _ => {}
    }
}

fn extract_lines(geom: &Geometry<f64>, out: &mut Vec<LineString<f64>>) {
    match geom {
        Geometry::Line(line) => {
            let line = LineString::from(vec![line.start, line.end]);
            out.push(line);
        }
        Geometry::LineString(line_string) => out.push(line_string.clone()),
        Geometry::MultiLineString(multi_line) => out.extend(multi_line.0.iter().cloned()),
        Geometry::GeometryCollection(collection) => {
            for sub in &collection.0 {
                extract_lines(sub, out);
            }
        }
        _ => {}
    }
}

struct LineGroup {
    origin: Coord<f64>,
    dir: Coord<f64>,
    intervals: Vec<(f64, f64)>,
}

impl LineGroup {
    fn new(origin: Coord<f64>, dir: Coord<f64>) -> Self {
        Self {
            origin,
            dir,
            intervals: Vec::new(),
        }
    }

    fn add_line(&mut self, line: &LineString<f64>) {
        let denom = self.dir.x * self.dir.x + self.dir.y * self.dir.y;
        if denom == 0.0 {
            return;
        }
        let mut min_t = f64::INFINITY;
        let mut max_t = f64::NEG_INFINITY;
        for coord in &line.0 {
            let t = ((coord.x - self.origin.x) * self.dir.x
                + (coord.y - self.origin.y) * self.dir.y)
                / denom;
            min_t = min_t.min(t);
            max_t = max_t.max(t);
        }
        if min_t.is_finite() && max_t.is_finite() {
            self.intervals.push((min_t, max_t));
        }
    }

    fn merge(mut self) -> Vec<LineString<f64>> {
        if self.intervals.is_empty() {
            return Vec::new();
        }
        self.intervals
            .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let mut merged = Vec::new();
        let mut current = self.intervals[0];
        for interval in self.intervals.into_iter().skip(1) {
            if interval.0 <= current.1 + LINE_OVERLAY_EPS {
                current.1 = current.1.max(interval.1);
            } else {
                merged.push(current);
                current = interval;
            }
        }
        merged.push(current);

        merged
            .into_iter()
            .map(|(start, end)| {
                let start = Coord {
                    x: self.origin.x + self.dir.x * start,
                    y: self.origin.y + self.dir.y * start,
                };
                let end = Coord {
                    x: self.origin.x + self.dir.x * end,
                    y: self.origin.y + self.dir.y * end,
                };
                LineString::from(vec![start, end])
            })
            .collect()
    }
}

fn line_direction(line: &LineString<f64>) -> Option<(Coord<f64>, Coord<f64>)> {
    if line.0.len() < 2 {
        return None;
    }
    let origin = line.0[0];
    let mut idx = 1;
    while idx < line.0.len() {
        let coord = line.0[idx];
        if !coord_eq(origin, coord) {
            let dir = Coord {
                x: coord.x - origin.x,
                y: coord.y - origin.y,
            };
            if dir.x != 0.0 || dir.y != 0.0 {
                return Some((origin, dir));
            }
        }
        idx += 1;
    }
    None
}

fn line_is_collinear(line: &LineString<f64>, origin: Coord<f64>, dir: Coord<f64>) -> bool {
    line.0
        .iter()
        .all(|coord| point_on_line(origin, dir, *coord))
}

fn line_belongs_to_group(origin: &Coord<f64>, dir: &Coord<f64>, group: &LineGroup) -> bool {
    is_parallel(*dir, group.dir) && point_on_line(group.origin, group.dir, *origin)
}

fn point_on_line(origin: Coord<f64>, dir: Coord<f64>, coord: Coord<f64>) -> bool {
    let dx = coord.x - origin.x;
    let dy = coord.y - origin.y;
    let cross = dx * dir.y - dy * dir.x;
    let len = (dir.x * dir.x + dir.y * dir.y).sqrt().max(1.0);
    cross.abs() <= LINE_OVERLAY_EPS * len
}

fn is_parallel(a: Coord<f64>, b: Coord<f64>) -> bool {
    let cross = a.x * b.y - a.y * b.x;
    let len = (a.x * a.x + a.y * a.y).sqrt().max(1.0) * (b.x * b.x + b.y * b.y).sqrt().max(1.0);
    cross.abs() <= LINE_OVERLAY_EPS * len
}

fn line_segments(line: &LineString<f64>) -> Vec<(Coord<f64>, Coord<f64>)> {
    if line.0.len() < 2 {
        return Vec::new();
    }
    let mut segments = Vec::with_capacity(line.0.len() - 1);
    for idx in 0..(line.0.len() - 1) {
        let start = line.0[idx];
        let end = line.0[idx + 1];
        if !coord_eq(start, end) {
            segments.push((start, end));
        }
    }
    segments
}

fn dedup_lines(lines: Vec<LineString<f64>>) -> Vec<LineString<f64>> {
    let mut output = Vec::new();
    for line in lines {
        if output.iter().any(|existing| line_eq(existing, &line)) {
            continue;
        }
        output.push(line);
    }
    output
}

fn line_eq(a: &LineString<f64>, b: &LineString<f64>) -> bool {
    if a.0.len() != b.0.len() {
        return false;
    }
    if a.0.iter().zip(&b.0).all(|(ca, cb)| coord_eq(*ca, *cb)) {
        return true;
    }
    a.0.iter()
        .zip(b.0.iter().rev())
        .all(|(ca, cb)| coord_eq(*ca, *cb))
}

struct SegmentInfo {
    start: Coord<f64>,
    end: Coord<f64>,
}

fn split_line_by_cutters(
    line: &LineString<f64>,
    cutters: &[LineString<f64>],
) -> Vec<LineString<f64>> {
    if line.0.len() < 2 {
        return Vec::new();
    }

    let mut output = Vec::new();
    for idx in 0..(line.0.len() - 1) {
        let start = line.0[idx];
        let end = line.0[idx + 1];
        if coord_eq(start, end) {
            continue;
        }
        let mut params = vec![0.0_f64, 1.0_f64];
        collect_line_intersections(start, end, cutters, &mut params);
        params.sort_by(|a, b| a.partial_cmp(b).unwrap());
        params.dedup_by(|a, b| (*a - *b).abs() <= LINE_OVERLAY_EPS);

        for win in params.windows(2) {
            let t0 = win[0];
            let t1 = win[1];
            if t1 - t0 <= LINE_OVERLAY_EPS {
                continue;
            }
            let s = interp(start, end, t0);
            let e = interp(start, end, t1);
            if coord_eq(s, e) {
                continue;
            }
            output.push(LineString::from(vec![s, e]));
        }
    }

    output
}

fn split_line_by_polygon(
    line: &LineString<f64>,
    polygon: &Polygon<f64>,
) -> (Vec<LineString<f64>>, Vec<LineString<f64>>) {
    if line.0.len() < 2 {
        return (Vec::new(), Vec::new());
    }

    let mut inside = Vec::new();
    let mut outside = Vec::new();

    for idx in 0..(line.0.len() - 1) {
        let start = line.0[idx];
        let end = line.0[idx + 1];
        if coord_eq(start, end) {
            continue;
        }
        let mut params = vec![0.0_f64, 1.0_f64];
        collect_polygon_intersections(start, end, polygon, &mut params);
        params.sort_by(|a, b| a.partial_cmp(b).unwrap());
        params.dedup_by(|a, b| (*a - *b).abs() <= LINE_OVERLAY_EPS);

        for win in params.windows(2) {
            let t0 = win[0];
            let t1 = win[1];
            if t1 - t0 <= LINE_OVERLAY_EPS {
                continue;
            }
            let s = interp(start, end, t0);
            let e = interp(start, end, t1);
            if coord_eq(s, e) {
                continue;
            }
            let mid = interp(start, end, (t0 + t1) * 0.5);
            let point = Point(mid);
            if polygon.intersects(&point) {
                inside.push(LineString::from(vec![s, e]));
            } else {
                outside.push(LineString::from(vec![s, e]));
            }
        }
    }

    (inside, outside)
}

fn collect_polygon_intersections(
    start: Coord<f64>,
    end: Coord<f64>,
    polygon: &Polygon<f64>,
    params: &mut Vec<f64>,
) {
    collect_ring_intersections(start, end, polygon.exterior(), params);
    for ring in polygon.interiors() {
        collect_ring_intersections(start, end, ring, params);
    }
}

fn collect_ring_intersections(
    start: Coord<f64>,
    end: Coord<f64>,
    ring: &LineString<f64>,
    params: &mut Vec<f64>,
) {
    if ring.0.len() < 2 {
        return;
    }
    let line_a = Line::new(start, end);
    for idx in 0..(ring.0.len() - 1) {
        let a = ring.0[idx];
        let b = ring.0[idx + 1];
        let line_b = Line::new(a, b);
        let Some(intersection) = line_intersection(line_a, line_b) else {
            continue;
        };
        match intersection {
            LineIntersection::SinglePoint { intersection, .. } => {
                if let Some(t) = line_param(start, end, intersection) {
                    params.push(t);
                }
            }
            LineIntersection::Collinear { intersection } => {
                if let Some(t1) = line_param(start, end, intersection.start) {
                    params.push(t1);
                }
                if let Some(t2) = line_param(start, end, intersection.end) {
                    params.push(t2);
                }
            }
        }
    }
}

fn collect_line_intersections(
    start: Coord<f64>,
    end: Coord<f64>,
    cutters: &[LineString<f64>],
    params: &mut Vec<f64>,
) {
    let line_a = Line::new(start, end);
    for cutter in cutters {
        if cutter.0.len() < 2 {
            continue;
        }
        for idx in 0..(cutter.0.len() - 1) {
            let a = cutter.0[idx];
            let b = cutter.0[idx + 1];
            let line_b = Line::new(a, b);
            let Some(intersection) = line_intersection(line_a, line_b) else {
                continue;
            };
            match intersection {
                LineIntersection::SinglePoint { intersection, .. } => {
                    if let Some(t) = line_param(start, end, intersection) {
                        params.push(t);
                    }
                }
                LineIntersection::Collinear { intersection } => {
                    if let Some(t) = line_param(start, end, intersection.start) {
                        params.push(t);
                    }
                    if let Some(t) = line_param(start, end, intersection.end) {
                        params.push(t);
                    }
                }
            }
        }
    }
}

fn segment_is_on_lines(segment: &LineString<f64>, lines: &[LineString<f64>]) -> bool {
    if segment.0.len() < 2 {
        return true;
    }
    let mid = interp(segment.0[0], segment.0[1], 0.5);
    let point = Point(mid);
    lines.iter().any(|line| line.contains(&point))
}

fn line_param(start: Coord<f64>, end: Coord<f64>, point: Coord<f64>) -> Option<f64> {
    let dx = end.x - start.x;
    let dy = end.y - start.y;
    let denom = dx * dx + dy * dy;
    if denom == 0.0 {
        return None;
    }
    Some(((point.x - start.x) * dx + (point.y - start.y) * dy) / denom)
}

fn interp(start: Coord<f64>, end: Coord<f64>, t: f64) -> Coord<f64> {
    Coord {
        x: start.x + (end.x - start.x) * t,
        y: start.y + (end.y - start.y) * t,
    }
}

fn coord_eq(a: Coord<f64>, b: Coord<f64>) -> bool {
    (a.x - b.x).abs() <= LINE_OVERLAY_EPS && (a.y - b.y).abs() <= LINE_OVERLAY_EPS
}

fn point_eq(a: Point<f64>, b: Point<f64>) -> bool {
    coord_eq(a.0, b.0)
}

fn dedup_points(mut points: Vec<Point<f64>>) -> Vec<Point<f64>> {
    points.sort_by(|a, b| {
        let dx = a.x().partial_cmp(&b.x()).unwrap();
        if dx == std::cmp::Ordering::Equal {
            a.y().partial_cmp(&b.y()).unwrap()
        } else {
            dx
        }
    });
    points.dedup_by(|a, b| point_eq(*a, *b));
    points
}

fn dimension_rank(dim: Dimensions) -> i32 {
    match dim {
        Dimensions::Empty => -1,
        Dimensions::ZeroDimensional => 0,
        Dimensions::OneDimensional => 1,
        Dimensions::TwoDimensional => 2,
    }
}
