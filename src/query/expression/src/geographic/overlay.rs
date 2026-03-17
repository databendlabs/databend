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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OverlayMode {
    Intersection,
    Union,
    Difference,
    SymDifference,
}

pub struct GeometryOverlay {
    mode: OverlayMode,
}

#[derive(Clone, Default)]
struct OverlayParts {
    points: Vec<Point<f64>>,
    lines: Vec<LineString<f64>>,
    polys: Vec<Polygon<f64>>,
}

struct LineGroup {
    origin: Coord<f64>,
    dir: Coord<f64>,
    intervals: Vec<(f64, f64)>,
}

struct SegmentInfo {
    start: Coord<f64>,
    end: Coord<f64>,
}

impl GeometryOverlay {
    const LINE_OVERLAY_EPS: f64 = 1e-9;

    pub fn new(mode: OverlayMode) -> Self {
        Self { mode }
    }

    pub fn apply(&self, g0: &Geometry<f64>, g1: &Geometry<f64>) -> Result<Option<Geometry<f64>>> {
        if self.is_union_like() && Self::can_fast_merge(g0, g1) {
            let units = vec![g0.clone(), g1.clone()];
            let result = Self::build_geometry_from_elements(units);
            return Ok(result);
        }

        let left = Self::prepare_operand(g0)?;
        let right = Self::prepare_operand(g1)?;

        match self.mode {
            OverlayMode::Union => self.do_union(&left, &right),
            OverlayMode::Intersection => self.do_intersection(&left, &right),
            OverlayMode::Difference => self.do_difference(&left, &right),
            OverlayMode::SymDifference => self.do_symdifference(&left, &right),
        }
    }

    pub fn apply_iter<I>(&self, geos: I) -> Result<Option<Geometry<f64>>>
    where I: IntoIterator<Item = Geometry<f64>> {
        match self.mode {
            OverlayMode::Union => self.apply_union_iter(geos),
            OverlayMode::Intersection => self.apply_intersection_iter(geos),
            OverlayMode::Difference | OverlayMode::SymDifference => {
                let mut iter = geos.into_iter();
                let Some(mut acc) = iter.next() else {
                    return Ok(None);
                };

                for geo in iter {
                    let Some(next) = self.apply(&acc, &geo)? else {
                        return Ok(None);
                    };
                    acc = next;
                }

                Ok(Some(acc))
            }
        }
    }

    fn is_union_like(&self) -> bool {
        matches!(self.mode, OverlayMode::Union | OverlayMode::SymDifference)
    }

    fn empty_geometry() -> Geometry<f64> {
        Geometry::GeometryCollection(GeometryCollection::empty())
    }

    fn can_fast_merge(g0: &Geometry<f64>, g1: &Geometry<f64>) -> bool {
        let (Some(r0), Some(r1)) = (g0.bounding_rect(), g1.bounding_rect()) else {
            return false;
        };
        if r0.intersects(&r1) {
            return false;
        }

        Self::is_single_geometry(g0) && Self::is_single_geometry(g1)
    }

    fn is_single_geometry(geom: &Geometry<f64>) -> bool {
        matches!(
            geom,
            Geometry::Point(_) | Geometry::LineString(_) | Geometry::Polygon(_)
        )
    }

    fn build_geometry_from_elements(elements: Vec<Geometry<f64>>) -> Option<Geometry<f64>> {
        if elements.is_empty() {
            return None;
        }

        if elements.iter().all(|geo| matches!(geo, Geometry::Point(_))) {
            let points: Vec<Point<f64>> = elements
                .into_iter()
                .map(|geo| geo.try_into().expect("point geometry"))
                .collect();
            return Some(Geometry::MultiPoint(MultiPoint::from_iter(points)));
        }

        if elements
            .iter()
            .all(|geo| matches!(geo, Geometry::LineString(_)))
        {
            let lines: Vec<LineString<f64>> = elements
                .into_iter()
                .map(|geo| geo.try_into().expect("linestring geometry"))
                .collect();
            return Some(Geometry::MultiLineString(MultiLineString::from_iter(lines)));
        }

        if elements
            .iter()
            .all(|geo| matches!(geo, Geometry::Polygon(_)))
        {
            let polygons: Vec<Polygon<f64>> = elements
                .into_iter()
                .map(|geo| geo.try_into().expect("polygon geometry"))
                .collect();
            return Some(Geometry::MultiPolygon(MultiPolygon::from_iter(polygons)));
        }

        Some(Geometry::GeometryCollection(GeometryCollection::from_iter(
            elements,
        )))
    }

    fn apply_union_iter<I>(&self, geos: I) -> Result<Option<Geometry<f64>>>
    where I: IntoIterator<Item = Geometry<f64>> {
        let mut merged = OverlayParts::default();
        let mut has_input = false;

        for geo in geos {
            let parts = Self::prepare_operand(&geo)?;
            Self::extend_parts(&mut merged, parts);
            has_input = true;
        }

        if !has_input {
            return Ok(None);
        }

        Self::assemble_result(merged.polys, merged.lines, merged.points)
    }

    fn apply_intersection_iter<I>(&self, geos: I) -> Result<Option<Geometry<f64>>>
    where I: IntoIterator<Item = Geometry<f64>> {
        let mut iter = geos.into_iter();
        let Some(first) = iter.next() else {
            return Ok(None);
        };

        let mut acc = Self::prepare_operand(&first)?;
        for geo in iter {
            let next = Self::prepare_operand(&geo)?;
            acc = self.intersect_parts(acc, next)?;
            if Self::parts_are_empty(&acc) {
                return Ok(None);
            }
        }

        Self::assemble_result(acc.polys, acc.lines, acc.points)
    }

    fn prepare_operand(geom: &Geometry<f64>) -> Result<OverlayParts> {
        let mut parts = OverlayParts::default();
        Self::collect_overlay_parts(geom, &mut parts);
        // Normalize each dimension independently before binary overlay:
        // - points are deduplicated
        // - lines are split/merged into a stable line set
        // - polygons are unary-unioned into a stable polygon set
        parts.points = Self::union_points(parts.points);
        parts.lines = Self::union_lines(parts.lines)?;
        parts.polys = Self::union_polys(parts.polys)?;
        Ok(parts)
    }

    fn normalize_parts(mut parts: OverlayParts) -> Result<OverlayParts> {
        parts.points = Self::union_points(parts.points);
        parts.lines = Self::union_lines(parts.lines)?;
        parts.polys = Self::union_polys(parts.polys)?;
        Ok(parts)
    }

    fn extend_parts(target: &mut OverlayParts, mut source: OverlayParts) {
        target.points.append(&mut source.points);
        target.lines.append(&mut source.lines);
        target.polys.append(&mut source.polys);
    }

    fn parts_are_empty(parts: &OverlayParts) -> bool {
        parts.points.is_empty() && parts.lines.is_empty() && parts.polys.is_empty()
    }

    fn collect_overlay_parts(geom: &Geometry<f64>, parts: &mut OverlayParts) {
        match geom {
            Geometry::GeometryCollection(collection) => {
                for sub in &collection.0 {
                    Self::collect_overlay_parts(sub, parts);
                }
            }
            Geometry::Point(point) => {
                parts.points.push(*point);
            }
            Geometry::MultiPoint(multi_point) => {
                parts.points.extend(multi_point.0.iter().copied());
            }
            Geometry::Line(line) => {
                let line = LineString::from(vec![line.start, line.end]);
                if !line.0.is_empty() {
                    parts.lines.push(line);
                }
            }
            Geometry::LineString(line_string) => {
                if !line_string.0.is_empty() {
                    parts.lines.push(line_string.clone());
                }
            }
            Geometry::MultiLineString(multi_line) => {
                for line in &multi_line.0 {
                    if !line.0.is_empty() {
                        parts.lines.push(line.clone());
                    }
                }
            }
            Geometry::Polygon(polygon) => {
                if !polygon.exterior().0.is_empty() {
                    parts.polys.push(polygon.clone());
                }
            }
            Geometry::MultiPolygon(multi_polygon) => {
                for polygon in &multi_polygon.0 {
                    if !polygon.exterior().0.is_empty() {
                        parts.polys.push(polygon.clone());
                    }
                }
            }
            Geometry::Rect(rect) => {
                parts.polys.push(rect.to_polygon());
            }
            Geometry::Triangle(triangle) => {
                parts.polys.push(triangle.to_polygon());
            }
        }
    }

    // Final assembly always applies the same dimensional rules:
    // 1. multiple polygons are merged with unary union
    // 2. multiple lines are split/merged/deduplicated into a normalized line set
    // 3. multiple points are deduplicated
    // 4. points covered by lines are removed
    // 5. points covered by polygons are removed
    // 6. line segments covered by polygons are removed
    fn assemble_result(
        polys: Vec<Polygon<f64>>,
        lines: Vec<LineString<f64>>,
        points: Vec<Point<f64>>,
    ) -> Result<Option<Geometry<f64>>> {
        let polys = Self::union_polys(polys)?;
        let mut lines = Self::union_lines(lines)?;
        let mut points = Self::union_points(points);

        if !polys.is_empty() {
            lines = Self::difference_lines_by_polys(lines, &polys)?;
            points = Self::difference_points_by_polys(points, &polys);
        }

        if !lines.is_empty() {
            points = Self::difference_points_by_lines(points, &lines);
        }

        let mut outputs = Vec::new();
        if let Some(geom) = Self::geometry_from_polys(&polys) {
            outputs.push(geom);
        }
        if let Some(geom) = Self::geometry_from_lines(&lines) {
            outputs.push(geom);
        }
        if let Some(geom) = Self::geometry_from_points(&points) {
            outputs.push(geom);
        }

        match outputs.len() {
            0 => Ok(None),
            1 => Ok(Some(outputs.remove(0))),
            _ => Ok(Self::build_geometry_from_elements(outputs)),
        }
    }

    fn do_union(&self, left: &OverlayParts, right: &OverlayParts) -> Result<Option<Geometry<f64>>> {
        let mut polys = left.polys.clone();
        polys.extend(right.polys.iter().cloned());

        let mut lines = left.lines.clone();
        lines.extend(right.lines.iter().cloned());

        let mut points = left.points.clone();
        points.extend(right.points.iter().copied());

        Self::assemble_result(polys, lines, points)
    }

    fn do_intersection(
        &self,
        left: &OverlayParts,
        right: &OverlayParts,
    ) -> Result<Option<Geometry<f64>>> {
        let parts = self.intersect_parts(left.clone(), right.clone())?;
        Self::assemble_result(parts.polys, parts.lines, parts.points)
    }

    fn intersect_parts(&self, left: OverlayParts, right: OverlayParts) -> Result<OverlayParts> {
        let polys = Self::intersection_polys(&left.polys, &right.polys)?;

        let mut lines = Self::clip_lines_to_polys(left.lines.clone(), &right.polys);
        lines.extend(Self::clip_lines_to_polys(right.lines.clone(), &left.polys));
        let line_parts = Self::intersection_lines_parts(&left.lines, &right.lines);
        lines.extend(line_parts.lines);

        let mut points = line_parts.points;
        points.extend(Self::intersection_points_in_polys(
            &left.points,
            &right.polys,
        ));
        points.extend(Self::intersection_points_in_polys(
            &right.points,
            &left.polys,
        ));
        points.extend(Self::intersection_points_on_lines(
            &left.points,
            &right.lines,
        ));
        points.extend(Self::intersection_points_on_lines(
            &right.points,
            &left.lines,
        ));
        points.extend(Self::intersection_points(&left.points, &right.points));

        Self::normalize_parts(OverlayParts {
            points,
            lines,
            polys,
        })
    }

    fn do_difference(
        &self,
        left: &OverlayParts,
        right: &OverlayParts,
    ) -> Result<Option<Geometry<f64>>> {
        let parts = self.difference_parts(left, right)?;
        Self::assemble_result(parts.polys, parts.lines, parts.points)
    }

    fn do_symdifference(
        &self,
        left: &OverlayParts,
        right: &OverlayParts,
    ) -> Result<Option<Geometry<f64>>> {
        if left.points.is_empty()
            && right.points.is_empty()
            && left.polys.is_empty()
            && right.polys.is_empty()
        {
            let shared = Self::intersection_lines_parts(&left.lines, &right.lines);
            if shared.lines.is_empty() {
                let mut lines = left.lines.clone();
                lines.extend(right.lines.iter().cloned());
                return Self::assemble_result(Vec::new(), lines, Vec::new());
            }
        }

        let left_only = self.difference_parts(left, right)?;
        let right_only = self.difference_parts(right, left)?;

        let mut polys = left_only.polys;
        polys.extend(right_only.polys);

        let mut lines = left_only.lines;
        lines.extend(right_only.lines);

        let mut points = left_only.points;
        points.extend(right_only.points);

        Self::assemble_result(polys, lines, points)
    }

    fn difference_parts(&self, left: &OverlayParts, right: &OverlayParts) -> Result<OverlayParts> {
        let polys = Self::difference_polys(&left.polys, &right.polys)?;
        let line_diff_poly = Self::difference_lines_by_polys(left.lines.clone(), &right.polys)?;
        let lines = Self::difference_lines(&line_diff_poly, &right.lines);

        let mut points = Self::difference_points_by_polys(left.points.clone(), &right.polys);
        points = Self::difference_points_by_lines(points, &right.lines);
        points = Self::difference_points(points, &right.points);

        Ok(OverlayParts {
            points,
            lines,
            polys,
        })
    }

    fn intersection_polys(
        left: &[Polygon<f64>],
        right: &[Polygon<f64>],
    ) -> Result<Vec<Polygon<f64>>> {
        if left.is_empty() || right.is_empty() {
            return Ok(Vec::new());
        }

        let left_mp = MultiPolygon::new(left.to_vec());
        let right_mp = MultiPolygon::new(right.to_vec());
        Ok(left_mp.intersection(&right_mp).0)
    }

    fn intersection_lines_parts(
        left: &[LineString<f64>],
        right: &[LineString<f64>],
    ) -> OverlayParts {
        if left.is_empty() || right.is_empty() {
            return OverlayParts::default();
        }

        let mut lines = Vec::new();
        for line in left {
            let segments = Self::split_line_by_cutters(line, right);
            for segment in segments {
                if Self::segment_is_on_lines(&segment, right) {
                    lines.push(segment);
                }
            }
        }

        let points = Self::dedup_points(Self::intersection_points_from_lines(left, right));

        OverlayParts {
            points,
            lines,
            polys: Vec::new(),
        }
    }

    fn intersection_points_in_polys(
        points: &[Point<f64>],
        polys: &[Polygon<f64>],
    ) -> Vec<Point<f64>> {
        if points.is_empty() || polys.is_empty() {
            return Vec::new();
        }

        points
            .iter()
            .copied()
            .filter(|point| polys.iter().any(|poly| poly.intersects(point)))
            .collect()
    }

    fn intersection_points_on_lines(
        points: &[Point<f64>],
        lines: &[LineString<f64>],
    ) -> Vec<Point<f64>> {
        if points.is_empty() || lines.is_empty() {
            return Vec::new();
        }

        points
            .iter()
            .copied()
            .filter(|point| lines.iter().any(|line| line.contains(point)))
            .collect()
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
        Self::dedup_points(points)
    }

    fn union_lines(lines: Vec<LineString<f64>>) -> Result<Vec<LineString<f64>>> {
        if lines.is_empty() {
            return Ok(Vec::new());
        }
        let merged = Self::merge_collinear_lines(lines);
        let split = Self::split_lines_at_intersections(merged);
        let merged = Self::merge_collinear_lines(split);
        Ok(Self::dedup_lines(merged))
    }

    fn union_polys(polys: Vec<Polygon<f64>>) -> Result<Vec<Polygon<f64>>> {
        if polys.is_empty() {
            return Ok(Vec::new());
        }
        let merged = unary_union(&polys);
        Ok(merged.into_iter().collect())
    }

    fn difference_polys(
        left: &[Polygon<f64>],
        right: &[Polygon<f64>],
    ) -> Result<Vec<Polygon<f64>>> {
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
                let (_, outside_parts) = Self::split_line_by_polygon(&line, poly);
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
            let segments = Self::split_line_by_cutters(line, cutters);
            for segment in segments {
                if Self::segment_is_on_lines(&segment, cutters) {
                    continue;
                }
                result.push(segment);
            }
        }
        result
    }

    fn difference_points(points: Vec<Point<f64>>, cutters: &[Point<f64>]) -> Vec<Point<f64>> {
        if cutters.is_empty() {
            return Self::dedup_points(points);
        }
        let cutters = Self::dedup_points(cutters.to_vec());
        let filtered: Vec<Point<f64>> = points
            .into_iter()
            .filter(|point| !cutters.iter().any(|cut| Self::point_eq(*point, *cut)))
            .collect();
        Self::dedup_points(filtered)
    }

    fn difference_points_by_lines(
        points: Vec<Point<f64>>,
        lines: &[LineString<f64>],
    ) -> Vec<Point<f64>> {
        if lines.is_empty() {
            return Self::dedup_points(points);
        }
        let filtered: Vec<Point<f64>> = points
            .into_iter()
            .filter(|point| !lines.iter().any(|line| line.contains(point)))
            .collect();
        Self::dedup_points(filtered)
    }

    fn difference_points_by_polys(
        points: Vec<Point<f64>>,
        polys: &[Polygon<f64>],
    ) -> Vec<Point<f64>> {
        if polys.is_empty() {
            return Self::dedup_points(points);
        }
        let filtered: Vec<Point<f64>> = points
            .into_iter()
            .filter(|point| !polys.iter().any(|poly| poly.intersects(point)))
            .collect();
        Self::dedup_points(filtered)
    }

    fn intersection_points(left: &[Point<f64>], right: &[Point<f64>]) -> Vec<Point<f64>> {
        if left.is_empty() || right.is_empty() {
            return Vec::new();
        }
        let left = Self::dedup_points(left.to_vec());
        let right = Self::dedup_points(right.to_vec());
        left.into_iter()
            .filter(|point| right.iter().any(|other| Self::point_eq(*point, *other)))
            .collect()
    }

    fn intersection_points_from_lines(
        left: &[LineString<f64>],
        right: &[LineString<f64>],
    ) -> Vec<Point<f64>> {
        let mut points = Vec::new();
        for line in left {
            let segments_left = Self::line_segments(line);
            for other in right {
                let segments_right = Self::line_segments(other);
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
        let mut groups = Vec::new();

        for line in lines {
            let (origin, dir) = match Self::line_direction(&line) {
                Some(res) => res,
                None => {
                    non_linear_lines.push(line);
                    continue;
                }
            };

            if !Self::line_is_collinear(&line, origin, dir) {
                non_linear_lines.push(line);
                continue;
            }

            let mut inserted = false;
            for group in &mut groups {
                if Self::line_belongs_to_group(&origin, &dir, group) {
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
        for line in &lines {
            if line.0.len() < 2 {
                continue;
            }
            for seg_idx in 0..(line.0.len() - 1) {
                let start = line.0[seg_idx];
                let end = line.0[seg_idx + 1];
                if !Self::coord_eq(start, end) {
                    segments.push(SegmentInfo { start, end });
                }
            }
        }

        let mut split_params: Vec<Vec<f64>> = vec![vec![0.0, 1.0]; segments.len()];
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

                if let Some(t_i) = Self::line_param(seg_i.start, seg_i.end, point) {
                    if (-Self::LINE_OVERLAY_EPS..=1.0 + Self::LINE_OVERLAY_EPS).contains(&t_i) {
                        split_params[i].push(t_i.clamp(0.0, 1.0));
                    }
                }
                if let Some(t_j) = Self::line_param(seg_j.start, seg_j.end, point) {
                    if (-Self::LINE_OVERLAY_EPS..=1.0 + Self::LINE_OVERLAY_EPS).contains(&t_j) {
                        split_params[j].push(t_j.clamp(0.0, 1.0));
                    }
                }
            }
        }

        let mut out = Vec::new();
        for (seg_idx, seg) in segments.iter().enumerate() {
            let mut params = split_params[seg_idx].clone();
            params.sort_by(|a, b| a.partial_cmp(b).unwrap());
            params.dedup_by(|a, b| (*a - *b).abs() <= Self::LINE_OVERLAY_EPS);

            for win in params.windows(2) {
                let t0 = win[0];
                let t1 = win[1];
                if t1 - t0 <= Self::LINE_OVERLAY_EPS {
                    continue;
                }
                let start = Self::interp(seg.start, seg.end, t0);
                let end = Self::interp(seg.start, seg.end, t1);
                if Self::coord_eq(start, end) {
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
                let (inside_parts, _) = Self::split_line_by_polygon(&line, poly);
                inside.extend(inside_parts);
            }
        }
        inside
    }

    fn line_direction(line: &LineString<f64>) -> Option<(Coord<f64>, Coord<f64>)> {
        if line.0.len() < 2 {
            return None;
        }

        let origin = line.0[0];
        let mut idx = 1;
        while idx < line.0.len() {
            let coord = line.0[idx];
            if !Self::coord_eq(origin, coord) {
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
            .all(|coord| Self::point_on_line(origin, dir, *coord))
    }

    fn line_belongs_to_group(origin: &Coord<f64>, dir: &Coord<f64>, group: &LineGroup) -> bool {
        Self::is_parallel(*dir, group.dir) && Self::point_on_line(group.origin, group.dir, *origin)
    }

    fn point_on_line(origin: Coord<f64>, dir: Coord<f64>, coord: Coord<f64>) -> bool {
        let dx = coord.x - origin.x;
        let dy = coord.y - origin.y;
        let cross = dx * dir.y - dy * dir.x;
        let len = (dir.x * dir.x + dir.y * dir.y).sqrt().max(1.0);
        cross.abs() <= Self::LINE_OVERLAY_EPS * len
    }

    fn is_parallel(a: Coord<f64>, b: Coord<f64>) -> bool {
        let cross = a.x * b.y - a.y * b.x;
        let len = (a.x * a.x + a.y * a.y).sqrt().max(1.0) * (b.x * b.x + b.y * b.y).sqrt().max(1.0);
        cross.abs() <= Self::LINE_OVERLAY_EPS * len
    }

    fn line_segments(line: &LineString<f64>) -> Vec<(Coord<f64>, Coord<f64>)> {
        if line.0.len() < 2 {
            return Vec::new();
        }

        let mut segments = Vec::with_capacity(line.0.len() - 1);
        for idx in 0..(line.0.len() - 1) {
            let start = line.0[idx];
            let end = line.0[idx + 1];
            if !Self::coord_eq(start, end) {
                segments.push((start, end));
            }
        }
        segments
    }

    fn dedup_lines(lines: Vec<LineString<f64>>) -> Vec<LineString<f64>> {
        let mut output = Vec::new();
        for line in lines {
            if output.iter().any(|existing| Self::line_eq(existing, &line)) {
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
        if a.0
            .iter()
            .zip(&b.0)
            .all(|(ca, cb)| Self::coord_eq(*ca, *cb))
        {
            return true;
        }
        a.0.iter()
            .zip(b.0.iter().rev())
            .all(|(ca, cb)| Self::coord_eq(*ca, *cb))
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
            if Self::coord_eq(start, end) {
                continue;
            }

            let mut params = vec![0.0_f64, 1.0_f64];
            Self::collect_line_intersections(start, end, cutters, &mut params);
            params.sort_by(|a, b| a.partial_cmp(b).unwrap());
            params.dedup_by(|a, b| (*a - *b).abs() <= Self::LINE_OVERLAY_EPS);

            for win in params.windows(2) {
                let t0 = win[0];
                let t1 = win[1];
                if t1 - t0 <= Self::LINE_OVERLAY_EPS {
                    continue;
                }
                let s = Self::interp(start, end, t0);
                let e = Self::interp(start, end, t1);
                if Self::coord_eq(s, e) {
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
            if Self::coord_eq(start, end) {
                continue;
            }

            let mut params = vec![0.0_f64, 1.0_f64];
            Self::collect_polygon_intersections(start, end, polygon, &mut params);
            params.sort_by(|a, b| a.partial_cmp(b).unwrap());
            params.dedup_by(|a, b| (*a - *b).abs() <= Self::LINE_OVERLAY_EPS);

            for win in params.windows(2) {
                let t0 = win[0];
                let t1 = win[1];
                if t1 - t0 <= Self::LINE_OVERLAY_EPS {
                    continue;
                }

                let s = Self::interp(start, end, t0);
                let e = Self::interp(start, end, t1);
                if Self::coord_eq(s, e) {
                    continue;
                }

                let mid = Self::interp(start, end, (t0 + t1) * 0.5);
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
        Self::collect_ring_intersections(start, end, polygon.exterior(), params);
        for ring in polygon.interiors() {
            Self::collect_ring_intersections(start, end, ring, params);
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
                    if let Some(t) = Self::line_param(start, end, intersection) {
                        params.push(t);
                    }
                }
                LineIntersection::Collinear { intersection } => {
                    if let Some(t1) = Self::line_param(start, end, intersection.start) {
                        params.push(t1);
                    }
                    if let Some(t2) = Self::line_param(start, end, intersection.end) {
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
                        if let Some(t) = Self::line_param(start, end, intersection) {
                            params.push(t);
                        }
                    }
                    LineIntersection::Collinear { intersection } => {
                        if let Some(t) = Self::line_param(start, end, intersection.start) {
                            params.push(t);
                        }
                        if let Some(t) = Self::line_param(start, end, intersection.end) {
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
        let mid = Self::interp(segment.0[0], segment.0[1], 0.5);
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
        (a.x - b.x).abs() <= Self::LINE_OVERLAY_EPS && (a.y - b.y).abs() <= Self::LINE_OVERLAY_EPS
    }

    fn point_eq(a: Point<f64>, b: Point<f64>) -> bool {
        Self::coord_eq(a.0, b.0)
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
        points.dedup_by(|a, b| Self::point_eq(*a, *b));
        points
    }
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
            if interval.0 <= current.1 + GeometryOverlay::LINE_OVERLAY_EPS {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn point(x: f64, y: f64) -> Geometry<f64> {
        Geometry::Point(Point::new(x, y))
    }

    fn line(coords: &[(f64, f64)]) -> Geometry<f64> {
        Geometry::LineString(LineString::from(
            coords
                .iter()
                .map(|(x, y)| Coord { x: *x, y: *y })
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn symdifference_keeps_points_from_both_sides() {
        let overlay = GeometryOverlay::new(OverlayMode::SymDifference);
        let result = overlay.apply(&point(0.0, 0.0), &point(1.0, 1.0)).unwrap();
        assert!(result.is_some());
        let result = result.unwrap();

        match result {
            Geometry::MultiPoint(multi_point) => {
                assert_eq!(multi_point.0.len(), 2);
                assert!(
                    multi_point
                        .0
                        .iter()
                        .any(|p| GeometryOverlay::point_eq(*p, Point::new(0.0, 0.0)))
                );
                assert!(
                    multi_point
                        .0
                        .iter()
                        .any(|p| GeometryOverlay::point_eq(*p, Point::new(1.0, 1.0)))
                );
            }
            other => panic!("unexpected geometry: {other:?}"),
        }
    }

    #[test]
    fn difference_removes_point_covered_by_right_line() {
        let overlay = GeometryOverlay::new(OverlayMode::Difference);
        let result = overlay
            .apply(&point(1.0, 0.0), &line(&[(0.0, 0.0), (2.0, 0.0)]))
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn difference_removes_point_on_polygon_boundary() {
        let overlay = GeometryOverlay::new(OverlayMode::Difference);
        let polygon = Geometry::Polygon(Polygon::new(
            LineString::from(vec![
                Coord { x: 1.0, y: 1.0 },
                Coord { x: 2.0, y: 1.0 },
                Coord { x: 2.0, y: 2.0 },
                Coord { x: 1.0, y: 2.0 },
                Coord { x: 1.0, y: 1.0 },
            ]),
            vec![],
        ));
        let result = overlay.apply(&point(1.0, 1.0), &polygon).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn symdifference_returns_exclusive_line_segments() {
        let overlay = GeometryOverlay::new(OverlayMode::SymDifference);
        let result = overlay
            .apply(
                &line(&[(0.0, 0.0), (4.0, 0.0)]),
                &line(&[(2.0, 0.0), (6.0, 0.0)]),
            )
            .unwrap();
        assert!(result.is_some());
        let result = result.unwrap();

        match result {
            Geometry::MultiLineString(multi_line) => {
                assert_eq!(multi_line.0.len(), 2);
                assert!(multi_line.0.iter().any(|line| GeometryOverlay::line_eq(
                    line,
                    &LineString::from(vec![Coord { x: 0.0, y: 0.0 }, Coord { x: 2.0, y: 0.0 },])
                )));
                assert!(multi_line.0.iter().any(|line| GeometryOverlay::line_eq(
                    line,
                    &LineString::from(vec![Coord { x: 4.0, y: 0.0 }, Coord { x: 6.0, y: 0.0 },])
                )));
            }
            other => panic!("unexpected geometry: {other:?}"),
        }
    }

    #[test]
    fn symdifference_merges_lines_touching_at_endpoint() {
        let overlay = GeometryOverlay::new(OverlayMode::SymDifference);
        let result = overlay
            .apply(
                &line(&[(0.0, 0.0), (2.0, 2.0)]),
                &line(&[(2.0, 2.0), (4.0, 4.0)]),
            )
            .unwrap();
        assert!(result.is_some());
        let result = result.unwrap();

        match result {
            Geometry::LineString(line) => {
                assert!(GeometryOverlay::line_eq(
                    &line,
                    &LineString::from(vec![Coord { x: 0.0, y: 0.0 }, Coord { x: 4.0, y: 4.0 },]),
                ));
            }
            other => panic!("unexpected geometry: {other:?}"),
        }
    }
}
