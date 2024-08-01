use geo::Coord;

use super::geo_buf;
use super::Element;
use super::Visitor;

impl<V: Visitor> Element<V> for geo::Geometry {
    fn accept(&self, visitor: &mut V) -> Result<(), anyhow::Error> {
        fn visit_points<V: Visitor>(
            points: &[Coord],
            visitor: &mut V,
            multi: bool,
        ) -> Result<(), anyhow::Error> {
            visitor.visit_points_start(points.len())?;
            for p in points.iter() {
                visitor.visit_point(&p, true)?;
            }
            visitor.visit_points_end(multi)
        }
        match self {
            geo::Geometry::Point(point) => {
                visitor.visit_point(&point.0, false)?;
                visitor.finish(geo_buf::ObjectKind::Point)
            }
            geo::Geometry::MultiPoint(points) => {
                visitor.visit_points_start(points.len())?;
                for p in points.iter() {
                    visitor.visit_point(&p.0, true)?;
                }
                visitor.visit_points_end(false)?;
                visitor.finish(geo_buf::ObjectKind::MultiPoint)
            }
            geo::Geometry::LineString(line) => {
                visit_points(&line.0, visitor, false)?;
                visitor.finish(geo_buf::ObjectKind::LineString)
            }
            geo::Geometry::MultiLineString(lines) => {
                visitor.visit_lines_start(lines.0.len())?;
                for line in lines.0.iter() {
                    visit_points(&line.0, visitor, true)?;
                }
                visitor.visit_lines_end()?;
                visitor.finish(geo_buf::ObjectKind::MultiLineString)
            }
            geo::Geometry::Polygon(polygon) => {
                visitor.visit_polygon_start(polygon.interiors().len() + 1)?;
                visit_points(&polygon.exterior().0, visitor, true)?;
                for ring in polygon.interiors() {
                    visit_points(&ring.0, visitor, true)?;
                }
                visitor.visit_polygon_end(false)?;
                visitor.finish(geo_buf::ObjectKind::Polygon)
            }
            geo::Geometry::MultiPolygon(polygons) => {
                visitor.visit_polygons_start(polygons.0.len())?;
                for polygon in polygons {
                    visitor.visit_polygon_start(polygon.interiors().len() + 1)?;
                    visit_points(&polygon.exterior().0, visitor, true)?;
                    for ring in polygon.interiors() {
                        visit_points(&ring.0, visitor, true)?;
                    }
                    visitor.visit_polygon_end(true)?;
                }
                visitor.visit_polygons_end()?;
                visitor.finish(geo_buf::ObjectKind::MultiPolygon)
            }
            geo::Geometry::GeometryCollection(collection) => {
                visitor.visit_collection_start(collection.len())?;
                for geom in collection {
                    geom.accept(visitor)?;
                }
                visitor.visit_collection_end()?;
                visitor.finish(geo_buf::ObjectKind::Collection)
            }
            _ => unreachable!(),
        }
    }
}
