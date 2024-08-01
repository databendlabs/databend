use core::f64;

use wkt::types::Coord as wktCoord;
use wkt::types::*;
use wkt::Geometry;
use wkt::Wkt;

use super::geo_buf;
use super::Coord;
use super::Element;
use super::Visitor;

impl<V: Visitor> Element<V> for Wkt<f64> {
    fn accept(&self, visitor: &mut V) -> Result<(), anyhow::Error> {
        fn visit_points(
            points: &[wktCoord<f64>],
            visitor: &mut impl Visitor,
            multi: bool,
        ) -> Result<(), anyhow::Error> {
            visitor.visit_points_start(points.len())?;
            for p in points.iter() {
                visitor.visit_point(&normalize_coord(p)?, true)?;
            }
            visitor.visit_points_end(multi)
        }

        fn accept_geom(
            geom: &Geometry<f64>,
            visitor: &mut impl Visitor,
        ) -> Result<(), anyhow::Error> {
            match geom {
                Geometry::Point(point) => {
                    visitor.visit_point(&normalize_point(point)?, false)?;
                    visitor.finish(geo_buf::ObjectKind::Point)
                }
                Geometry::MultiPoint(MultiPoint(points)) => {
                    visitor.visit_points_start(points.len())?;
                    for point in points {
                        visitor.visit_point(&normalize_point(point)?, true)?;
                    }
                    visitor.visit_points_end(false)?;
                    visitor.finish(geo_buf::ObjectKind::MultiPoint)
                }
                Geometry::LineString(LineString(line)) => {
                    visit_points(&line, visitor, false)?;
                    visitor.finish(geo_buf::ObjectKind::LineString)
                }
                Geometry::MultiLineString(MultiLineString(lines)) => {
                    visitor.visit_lines_start(lines.len())?;
                    for line in lines.iter() {
                        visit_points(&line.0, visitor, true)?;
                    }
                    visitor.visit_lines_end()?;
                    visitor.finish(geo_buf::ObjectKind::MultiLineString)
                }
                Geometry::Polygon(Polygon(polygon)) => {
                    visitor.visit_polygon_start(polygon.len())?;
                    for ring in polygon {
                        visit_points(&ring.0, visitor, true)?;
                    }
                    visitor.visit_polygon_end(false)?;
                    visitor.finish(geo_buf::ObjectKind::Polygon)
                }
                Geometry::MultiPolygon(MultiPolygon(polygons)) => {
                    visitor.visit_polygons_start(polygons.len())?;
                    for polygon in polygons {
                        visitor.visit_polygon_start(polygon.0.len())?;
                        for ring in &polygon.0 {
                            visit_points(&ring.0, visitor, true)?;
                        }
                        visitor.visit_polygon_end(true)?;
                    }
                    visitor.visit_polygons_end()?;
                    visitor.finish(geo_buf::ObjectKind::MultiPolygon)
                }
                Geometry::GeometryCollection(GeometryCollection(collection)) => {
                    visitor.visit_collection_start(collection.len())?;
                    for geom in collection {
                        accept_geom(geom, visitor)?;
                    }
                    visitor.visit_collection_end()?;
                    visitor.finish(geo_buf::ObjectKind::Collection)
                }
            }
        }

        accept_geom(&self.item, visitor)
    }
}

fn normalize_coord(coord: &wktCoord<f64>) -> Result<Coord, anyhow::Error> {
    if coord.z.is_some() || coord.m.is_some() {
        Err(anyhow::Error::msg("z m")) // todo
    } else {
        Ok(Coord {
            x: coord.x,
            y: coord.y,
        })
    }
}

fn normalize_point(point: &Point<f64>) -> Result<Coord, anyhow::Error> {
    match &point.0 {
        Some(c) => normalize_coord(c),
        None => Ok(Coord {
            x: f64::NAN,
            y: f64::NAN,
        }),
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {

    use std::str::FromStr;

    use geozero::ToWkt;

    use super::*;
    use crate::GeometryBuilder;

    #[test]
    fn test_from_wkt() {
        let mut builder = GeometryBuilder::new();

        let want = &"GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),GEOMETRYCOLLECTION(LINESTRING(40 60,50 50,60 40),POINT(99 11)),POINT(50 70))";
        let wkt_ins = wkt::Wkt::<f64>::from_str(want).unwrap();
        wkt_ins.accept(&mut builder).unwrap();
        let geom = builder.build();

        let got = geom.try_to_geo().unwrap().to_wkt().unwrap();

        assert_eq!(want, &got);
    }
}
