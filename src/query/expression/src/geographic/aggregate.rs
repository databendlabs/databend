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
use databend_common_io::geometry::rect_to_polygon;
use geo::BoundingRect;
use geo::Coord;
use geo::Geometry;
use geo::GeometryCollection;
use geo::LineString;
use geo::MultiLineString;
use geo::MultiPoint;
use geo::MultiPolygon;
use geo::Point;
use geo::Polygon;
use geo::Rect;

use crate::geographic::HeuristicOverlay;
use crate::geographic::HeuristicOverlayMode;
use crate::geographic::srid::geography_best_srid_for_geos;
use crate::geographic::srid::project_geography_to_best;
use crate::geographic::srid::unproject_geometry_from_best;

pub trait GeoAggOp: Send + Sync + 'static {
    fn compute(geos: Vec<Geometry<f64>>) -> Result<Option<Geometry<f64>>>;
}

pub struct GeometryUnionAggOp;

impl GeoAggOp for GeometryUnionAggOp {
    fn compute(geos: Vec<Geometry<f64>>) -> Result<Option<Geometry<f64>>> {
        apply_geometry_overlay(geos, HeuristicOverlayMode::Union)
    }
}

pub struct GeometryIntersectionAggOp;

impl GeoAggOp for GeometryIntersectionAggOp {
    fn compute(geos: Vec<Geometry<f64>>) -> Result<Option<Geometry<f64>>> {
        apply_geometry_overlay(geos, HeuristicOverlayMode::Intersection)
    }
}

pub struct GeometryDifferenceAggOp;

impl GeoAggOp for GeometryDifferenceAggOp {
    fn compute(geos: Vec<Geometry<f64>>) -> Result<Option<Geometry<f64>>> {
        apply_geometry_overlay(geos, HeuristicOverlayMode::Difference)
    }
}

pub struct GeometrySymDifferenceAggOp;

impl GeoAggOp for GeometrySymDifferenceAggOp {
    fn compute(geos: Vec<Geometry<f64>>) -> Result<Option<Geometry<f64>>> {
        apply_geometry_overlay(geos, HeuristicOverlayMode::SymDifference)
    }
}

pub struct GeographyUnionAggOp;

impl GeoAggOp for GeographyUnionAggOp {
    fn compute(geos: Vec<Geometry<f64>>) -> Result<Option<Geometry<f64>>> {
        apply_geography_overlay(geos, HeuristicOverlayMode::Union)
    }
}

pub struct GeographyIntersectionAggOp;

impl GeoAggOp for GeographyIntersectionAggOp {
    fn compute(geos: Vec<Geometry<f64>>) -> Result<Option<Geometry<f64>>> {
        apply_geography_overlay(geos, HeuristicOverlayMode::Intersection)
    }
}

pub struct GeographyDifferenceAggOp;

impl GeoAggOp for GeographyDifferenceAggOp {
    fn compute(geos: Vec<Geometry<f64>>) -> Result<Option<Geometry<f64>>> {
        apply_geography_overlay(geos, HeuristicOverlayMode::Difference)
    }
}

pub struct GeographySymDifferenceAggOp;

impl GeoAggOp for GeographySymDifferenceAggOp {
    fn compute(geos: Vec<Geometry<f64>>) -> Result<Option<Geometry<f64>>> {
        apply_geography_overlay(geos, HeuristicOverlayMode::SymDifference)
    }
}

pub struct CollectAggOp;

impl GeoAggOp for CollectAggOp {
    fn compute(geos: Vec<Geometry<f64>>) -> Result<Option<Geometry<f64>>> {
        if geos.is_empty() {
            return Ok(None);
        }

        if geos.iter().all(|geo| matches!(geo, Geometry::Point(_))) {
            let points: Vec<Point<f64>> = geos
                .into_iter()
                .map(|geo| geo.try_into().expect("point geometry"))
                .collect();
            let multi_point = MultiPoint::from_iter(points);
            return Ok(Some(Geometry::MultiPoint(multi_point)));
        }
        if geos
            .iter()
            .all(|geo| matches!(geo, Geometry::LineString(_)))
        {
            let lines: Vec<LineString<f64>> = geos
                .into_iter()
                .map(|geo| geo.try_into().expect("linestring geometry"))
                .collect();
            let multi_line_string = MultiLineString::from_iter(lines);
            return Ok(Some(Geometry::MultiLineString(multi_line_string)));
        }
        if geos.iter().all(|geo| matches!(geo, Geometry::Polygon(_))) {
            let polygons: Vec<Polygon<f64>> = geos
                .into_iter()
                .map(|geo| geo.try_into().expect("polygon geometry"))
                .collect();
            let multi_polygon = MultiPolygon::from_iter(polygons);
            return Ok(Some(Geometry::MultiPolygon(multi_polygon)));
        }

        let collection = GeometryCollection::from_iter(geos);
        Ok(Some(Geometry::GeometryCollection(collection)))
    }
}

pub struct EnvelopeAggOp;

impl GeoAggOp for EnvelopeAggOp {
    fn compute(geos: Vec<Geometry<f64>>) -> Result<Option<Geometry<f64>>> {
        let mut has_rect = false;
        let mut min_x = 0.0_f64;
        let mut min_y = 0.0_f64;
        let mut max_x = 0.0_f64;
        let mut max_y = 0.0_f64;

        for geo in geos {
            if let Some(rect) = geo.bounding_rect() {
                let min = rect.min();
                let max = rect.max();
                if !has_rect {
                    min_x = min.x;
                    min_y = min.y;
                    max_x = max.x;
                    max_y = max.y;
                    has_rect = true;
                } else {
                    min_x = min_x.min(min.x);
                    min_y = min_y.min(min.y);
                    max_x = max_x.max(max.x);
                    max_y = max_y.max(max.y);
                }
            }
        }

        if !has_rect {
            return Ok(None);
        }

        let rect = Rect::new(Coord { x: min_x, y: min_y }, Coord { x: max_x, y: max_y });
        Ok(Some(Geometry::Polygon(rect_to_polygon(rect))))
    }
}

fn apply_geography_overlay(
    geos: Vec<Geometry<f64>>,
    mode: HeuristicOverlayMode,
) -> Result<Option<Geometry<f64>>> {
    if geos.is_empty() {
        return Ok(None);
    }

    let best = match geography_best_srid_for_geos(&geos)? {
        Some(best) => best,
        None => {
            return Err(ErrorCode::GeometryError(
                "Can't find best SRID for geos".to_string(),
            ));
        }
    };

    let projected_geos = geos
        .into_iter()
        .map(|geo| project_geography_to_best(geo, &best))
        .collect::<Result<Vec<_>>>()?;

    let result = apply_geometry_overlay(projected_geos, mode)?;
    let output = result
        .map(|geo| unproject_geometry_from_best(geo, &best))
        .transpose()?;

    Ok(output)
}

fn apply_geometry_overlay(
    geos: Vec<Geometry<f64>>,
    mode: HeuristicOverlayMode,
) -> Result<Option<Geometry<f64>>> {
    if geos.is_empty() {
        return Ok(None);
    }

    let mut iter = geos.into_iter();
    let Some(first) = iter.next() else {
        return Ok(None);
    };

    let overlay = HeuristicOverlay::new(mode);
    let mut acc = first;
    for geo in iter {
        acc = overlay.apply(&acc, &geo)?;
    }

    Ok(Some(acc))
}
