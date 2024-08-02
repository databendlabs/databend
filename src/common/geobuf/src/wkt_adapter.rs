use super::geo_buf;
use super::Element;
use super::Geometry;
use super::GeometryBuilder;
use super::Visitor;

pub struct Wkt<S: AsRef<str>>(pub S);

impl<V: Visitor> Element<V> for wkt::Wkt<f64> {
    fn accept(&self, visitor: &mut V) -> Result<(), anyhow::Error> {
        use wkt::types::*;
        use wkt::Geometry;

        fn visit_points(
            points: &[Coord<f64>],
            visitor: &mut impl Visitor,
            multi: bool,
        ) -> Result<(), anyhow::Error> {
            visitor.visit_points_start(points.len())?;
            for p in points.iter() {
                let (x, y) = normalize_coord(p)?;
                visitor.visit_point(x, y, true)?;
            }
            visitor.visit_points_end(multi)
        }

        fn accept_geom(
            geom: &Geometry<f64>,
            visitor: &mut impl Visitor,
        ) -> Result<(), anyhow::Error> {
            match geom {
                Geometry::Point(point) => {
                    let (x, y) = normalize_point(point)?;
                    visitor.visit_point(x, y, false)?;
                    visitor.finish(geo_buf::ObjectKind::Point)
                }
                Geometry::MultiPoint(MultiPoint(points)) => {
                    visitor.visit_points_start(points.len())?;
                    for point in points {
                        let (x, y) = normalize_point(point)?;
                        visitor.visit_point(x, y, true)?;
                    }
                    visitor.visit_points_end(false)?;
                    visitor.finish(geo_buf::ObjectKind::MultiPoint)
                }
                Geometry::LineString(LineString(line)) => {
                    visit_points(line, visitor, false)?;
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

fn normalize_coord(coord: &wkt::types::Coord<f64>) -> Result<(f64, f64), anyhow::Error> {
    if coord.z.is_some() || coord.m.is_some() {
        Err(anyhow::Error::msg(
            "coordinates higher than two dimensions are not supported",
        ))
    } else {
        Ok((coord.x, coord.y))
    }
}

fn normalize_point(point: &wkt::types::Point<f64>) -> Result<(f64, f64), anyhow::Error> {
    match &point.0 {
        Some(c) => normalize_coord(c),
        None => Ok((f64::NAN, f64::NAN)),
    }
}

impl<S: AsRef<str>> TryFrom<Wkt<S>> for Geometry {
    type Error = anyhow::Error;

    fn try_from(str: Wkt<S>) -> Result<Self, Self::Error> {
        let wkt_struct: wkt::Wkt<f64> = str.0.as_ref().parse().map_err(anyhow::Error::msg)?;
        let mut builder = GeometryBuilder::new();
        wkt_struct.accept(&mut builder)?;
        Ok(builder.build())
    }
}

impl TryInto<Wkt<String>> for &Geometry {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Wkt<String>, Self::Error> {
        use geozero::ToWkt;

        Ok(Wkt(self.to_wkt()?))
    }
}

#[cfg(test)]
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

        let got = TryInto::<geo::Geometry>::try_into(&geom)
            .unwrap()
            .to_wkt()
            .unwrap();

        assert_eq!(want, &got);
    }
}
