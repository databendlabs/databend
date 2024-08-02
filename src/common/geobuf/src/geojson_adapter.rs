use geojson::Feature;
use geojson::GeoJson;
use geojson::Geometry;
use geojson::Value;

use super::geo_buf;
use super::Coord;
use super::Element;
use super::Visitor;

impl<V: Visitor> Element<V> for GeoJson {
    fn accept(&self, visitor: &mut V) -> Result<(), anyhow::Error> {
        fn visit_points(
            points: &[Vec<f64>],
            visitor: &mut impl Visitor,
            multi: bool,
        ) -> Result<(), anyhow::Error> {
            visitor.visit_points_start(points.len())?;
            for p in points.iter() {
                visitor.visit_point(&normalize_point(p)?, true)?;
            }
            visitor.visit_points_end(multi)
        }

        fn accept_geom(
            visitor: &mut impl Visitor,
            geom: &Geometry,
            feature: Option<&Feature>,
        ) -> Result<(), anyhow::Error> {
            match &geom.value {
                Value::Point(point) => {
                    visitor.visit_point(&normalize_point(point)?, false)?;
                    visitor.finish(geo_buf::ObjectKind::Point)
                }
                Value::MultiPoint(points) => {
                    visitor.visit_points_start(points.len())?;
                    for point in points {
                        visitor.visit_point(&normalize_point(point)?, true)?;
                    }
                    visitor.visit_points_end(false)?;
                    visitor.finish(geo_buf::ObjectKind::MultiPoint)
                }
                Value::LineString(line) => {
                    visit_points(line, visitor, false)?;
                    visitor.finish(geo_buf::ObjectKind::LineString)
                }
                Value::MultiLineString(lines) => {
                    visitor.visit_lines_start(lines.len())?;
                    for line in lines.iter() {
                        visit_points(line, visitor, true)?;
                    }
                    visitor.visit_lines_end()?;
                    visitor.finish(geo_buf::ObjectKind::MultiLineString)
                }
                Value::Polygon(polygon) => {
                    visitor.visit_polygon_start(polygon.len())?;
                    for ring in polygon {
                        visit_points(ring, visitor, true)?;
                    }
                    visitor.visit_polygon_end(false)?;
                    visitor.finish(geo_buf::ObjectKind::Polygon)
                }
                Value::MultiPolygon(polygons) => {
                    visitor.visit_polygons_start(polygons.len())?;
                    for polygon in polygons {
                        visitor.visit_polygon_start(polygon.len())?;
                        for ring in polygon {
                            visit_points(ring, visitor, true)?;
                        }
                        visitor.visit_polygon_end(true)?;
                    }
                    visitor.visit_polygons_end()?;
                    visitor.finish(geo_buf::ObjectKind::MultiPolygon)
                }
                Value::GeometryCollection(collection) => {
                    visitor.visit_collection_start(collection.len())?;
                    for geom in collection {
                        accept_geom(visitor, geom, None)?;
                    }
                    visitor.visit_collection_end()?;
                    visitor.finish(geo_buf::ObjectKind::Collection)
                }
            }
        }

        match self {
            GeoJson::Geometry(geom) => accept_geom(visitor, geom, None),
            GeoJson::Feature(feature) => accept_geom(
                visitor,
                feature.geometry.as_ref().ok_or(anyhow::Error::msg("aaa"))?,
                Some(feature),
            ),
            GeoJson::FeatureCollection(collection) => {
                visitor.visit_collection_start(collection.features.len())?;
                for featrue in collection {
                    accept_geom(
                        visitor,
                        featrue.geometry.as_ref().ok_or(anyhow::Error::msg("aaa"))?,
                        Some(featrue),
                    )?;
                }
                visitor.visit_collection_end()?;
                visitor.finish(geo_buf::ObjectKind::Collection)
            }
        }
    }
}

fn normalize_point(point: &[f64]) -> Result<Coord, anyhow::Error> {
    if point.len() != 2 {
        Err(anyhow::Error::msg("z m")) // todo
    } else {
        Ok(Coord {
            x: point[0],
            y: point[1],
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::GeometryBuilder;

    #[test]
    fn test_from_wkt() {}
}
