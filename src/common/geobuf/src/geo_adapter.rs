use anyhow::Ok;
use flatbuffers::Vector;
use geo::GeometryCollection;
use geo::LineString;
use geo::MultiPoint;
use geo::MultiPolygon;
use geo::Point;
use geo::Polygon;

use super::geo_buf;
use super::geo_buf::InnerObject;
use super::Coord;
use super::Element;
use super::Geometry;
use super::GeometryBuilder;
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
                visitor.visit_point(p, true)?;
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

impl TryFrom<&geo::Geometry<f64>> for Geometry {
    type Error = anyhow::Error;

    fn try_from(geo: &geo::Geometry<f64>) -> Result<Self, Self::Error> {
        let mut builder = GeometryBuilder::new();
        geo.accept(&mut builder)?;
        Ok(builder.build())
    }
}

impl TryInto<geo::Geometry<f64>> for &Geometry {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<geo::Geometry<f64>, Self::Error> {
        debug_assert!(self.column_x.len() == self.column_y.len());
        match geo_buf::ObjectKind(self.buf[0]) {
            geo_buf::ObjectKind::Point => {
                debug_assert!(self.column_x.len() == 1);
                Ok(geo::Geometry::Point(Point(Coord {
                    x: self.column_x[0],
                    y: self.column_y[0],
                })))
            }
            geo_buf::ObjectKind::MultiPoint => {
                let points = self
                    .column_x
                    .iter()
                    .cloned()
                    .zip(self.column_y.iter().cloned())
                    .map(|(x, y)| Point(Coord { x, y }))
                    .collect::<Vec<_>>();
                Ok(geo::Geometry::MultiPoint(MultiPoint(points)))
            }
            geo_buf::ObjectKind::LineString => {
                let points = self
                    .column_x
                    .iter()
                    .cloned()
                    .zip(self.column_y.iter().cloned())
                    .map(|(x, y)| Coord { x, y })
                    .collect::<Vec<_>>();
                Ok(geo::Geometry::LineString(LineString(points)))
            }
            geo_buf::ObjectKind::MultiLineString => {
                let object = geo_buf::root_as_object(&self.buf[1..])?;
                self.lines_from_offsets(&object.point_offsets())
            }
            geo_buf::ObjectKind::Polygon => {
                let object = geo_buf::root_as_object(&self.buf[1..])?;
                self.polygon_from_offsets(&object.point_offsets())
            }
            geo_buf::ObjectKind::MultiPolygon => {
                let object = geo_buf::root_as_object(&self.buf[1..])?;
                self.polygons_from_offsets(&object.point_offsets(), &object.ring_offsets())
            }
            geo_buf::ObjectKind::Collection => {
                let object = geo_buf::root_as_object(&self.buf[1..])?;

                let collection = object
                    .collection()
                    .ok_or(anyhow::Error::msg("invalid data"))?;

                let geoms = collection
                    .iter()
                    .map(|geom| self.inner_to_geo(&geom))
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(geo::Geometry::GeometryCollection(GeometryCollection(geoms)))
            }
            geo_buf::ObjectKind(geo_buf::ObjectKind::ENUM_MAX..) => unreachable!(),
        }
    }
}

impl Geometry {
    fn inner_to_geo(&self, object: &InnerObject) -> Result<geo::Geometry, anyhow::Error> {
        let geom = match geo_buf::InnerObjectKind(object.wkb_type() as u8) {
            geo_buf::InnerObjectKind::Point => {
                let pos = object
                    .point_offsets()
                    .ok_or(anyhow::Error::msg("invalid data"))?
                    .get(0);

                geo::Geometry::Point(Point(Coord {
                    x: self.column_x[pos as usize],
                    y: self.column_y[pos as usize],
                }))
            }
            geo_buf::InnerObjectKind::MultiPoint => {
                let point_offsets = object
                    .point_offsets()
                    .ok_or(anyhow::Error::msg("invalid data"))?;

                let start = point_offsets.get(0);
                let end = point_offsets.get(1);

                let points = (start as usize..end as usize)
                    .map(|pos| {
                        Point(Coord {
                            x: self.column_x[pos],
                            y: self.column_y[pos],
                        })
                    })
                    .collect::<Vec<_>>();

                geo::Geometry::MultiPoint(MultiPoint(points))
            }
            geo_buf::InnerObjectKind::LineString => {
                let point_offsets = object
                    .point_offsets()
                    .ok_or(anyhow::Error::msg("invalid data"))?;

                let start = point_offsets.get(0);
                let end = point_offsets.get(1);

                let points = (start as usize..end as usize)
                    .map(|pos| Coord {
                        x: self.column_x[pos],
                        y: self.column_y[pos],
                    })
                    .collect::<Vec<_>>();

                geo::Geometry::LineString(LineString(points))
            }
            geo_buf::InnerObjectKind::MultiLineString => {
                self.lines_from_offsets(&object.point_offsets())?
            }
            geo_buf::InnerObjectKind::Polygon => {
                self.polygon_from_offsets(&object.point_offsets())?
            }
            geo_buf::InnerObjectKind::MultiPolygon => {
                self.polygons_from_offsets(&object.point_offsets(), &object.ring_offsets())?
            }
            geo_buf::InnerObjectKind::Collection => {
                let collection = object
                    .collection()
                    .ok_or(anyhow::Error::msg("invalid data"))?;

                let geoms = collection
                    .iter()
                    .map(|geom| self.inner_to_geo(&geom))
                    .collect::<Result<Vec<_>, _>>()?;

                geo::Geometry::GeometryCollection(GeometryCollection(geoms))
            }
            _ => unreachable!(),
        };
        Ok(geom)
    }

    fn lines_from_offsets(
        &self,
        point_offsets: &Option<Vector<u32>>,
    ) -> Result<geo::Geometry, anyhow::Error> {
        let lines = point_offsets
            .ok_or(anyhow::Error::msg("invalid data"))?
            .iter()
            .map_windows(|[start, end]| {
                LineString(
                    (*start..*end)
                        .map(|pos| Coord {
                            x: self.column_x[pos as usize],
                            y: self.column_y[pos as usize],
                        })
                        .collect(),
                )
            })
            .collect();

        Ok(geo::Geometry::MultiLineString(lines))
    }

    fn polygon_from_offsets(
        &self,
        point_offsets: &Option<Vector<u32>>,
    ) -> Result<geo::Geometry, anyhow::Error> {
        let mut rings_iter = point_offsets
            .ok_or(anyhow::Error::msg("invalid data"))?
            .iter()
            .map_windows(|[start, end]| {
                LineString(
                    (*start..*end)
                        .map(|pos| Coord {
                            x: self.column_x[pos as usize],
                            y: self.column_y[pos as usize],
                        })
                        .collect(),
                )
            });
        let exterior = rings_iter
            .next()
            .ok_or(anyhow::Error::msg("invalid data"))?;
        let interiors = rings_iter.collect();
        Ok(geo::Geometry::Polygon(Polygon::new(exterior, interiors)))
    }

    fn polygons_from_offsets(
        &self,
        point_offsets: &Option<Vector<u32>>,
        ring_offsets: &Option<Vector<u32>>,
    ) -> Result<geo::Geometry, anyhow::Error> {
        let point_offsets = point_offsets.ok_or(anyhow::Error::msg("invalid data"))?;

        let polygons = ring_offsets
            .ok_or(anyhow::Error::msg("invalid data"))?
            .iter()
            .map_windows(|[start, end]| {
                let mut rings_iter = (*start..=*end)
                    .map(|i| point_offsets.get(i as usize))
                    .map_windows(|[start, end]| {
                        LineString(
                            (*start..*end)
                                .map(|pos| Coord {
                                    x: self.column_x[pos as usize],
                                    y: self.column_y[pos as usize],
                                })
                                .collect(),
                        )
                    });

                let exterior = rings_iter
                    .next()
                    .ok_or(anyhow::Error::msg("invalid data"))?;
                let interiors = rings_iter.collect();

                Ok(Polygon::new(exterior, interiors))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(geo::Geometry::MultiPolygon(MultiPolygon::new(polygons)))
    }
}
