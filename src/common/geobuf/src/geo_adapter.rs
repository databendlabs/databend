use anyhow::Ok;
use flatbuffers::Vector;
use geozero::error::GeozeroError;
use geozero::ToGeo;

use super::geo_buf;
use super::geo_buf::InnerObject;
use super::Element;
use super::Geometry;
use super::GeometryBuilder;
use super::Visitor;

impl<V: Visitor> Element<V> for geo::Geometry {
    fn accept(&self, visitor: &mut V) -> Result<(), anyhow::Error> {
        fn visit_points<V: Visitor>(
            points: &[geo::Coord],
            visitor: &mut V,
            multi: bool,
        ) -> Result<(), anyhow::Error> {
            visitor.visit_points_start(points.len())?;
            for p in points.iter() {
                visitor.visit_point(p.x, p.y, true)?;
            }
            visitor.visit_points_end(multi)
        }
        match self {
            geo::Geometry::Point(point) => {
                visitor.visit_point(point.0.x, point.0.y, false)?;
                visitor.finish(geo_buf::ObjectKind::Point)
            }
            geo::Geometry::MultiPoint(points) => {
                visitor.visit_points_start(points.len())?;
                for p in points.iter() {
                    visitor.visit_point(p.0.x, p.0.y, true)?;
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
    type Error = geozero::error::GeozeroError;

    fn try_into(self) -> Result<geo::Geometry<f64>, Self::Error> {
        debug_assert!(self.column_x.len() == self.column_y.len());

        self.to_geo()
    }
}

impl geozero::GeozeroGeometry for Geometry {
    fn srid(&self) -> Option<i32> {
        None
    }

    fn process_geom<P>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        P: geozero::GeomProcessor,
        Self: Sized,
    {
        debug_assert!(self.column_x.len() == self.column_y.len());
        const OUT_OF_RANGE: u8 = geo_buf::ObjectKind::ENUM_MAX + 1;
        match geo_buf::ObjectKind(self.buf[0]) {
            geo_buf::ObjectKind::Point => {
                debug_assert!(self.column_x.len() == 1);
                processor.point_begin(0)?;
                processor.xy(self.column_x[0], self.column_y[0], 0)?;
                processor.point_end(0)
            }
            geo_buf::ObjectKind::MultiPoint => {
                processor.multipoint_begin(self.column_x.len(), 0)?;
                for (idxc, (x, y)) in self
                    .column_x
                    .iter()
                    .cloned()
                    .zip(self.column_y.iter().cloned())
                    .enumerate()
                {
                    processor.xy(x, y, idxc)?;
                }
                processor.multipoint_end(0)
            }
            geo_buf::ObjectKind::LineString => {
                processor.linestring_begin(true, self.column_x.len(), 0)?;
                for (idxc, (x, y)) in self
                    .column_x
                    .iter()
                    .cloned()
                    .zip(self.column_y.iter().cloned())
                    .enumerate()
                {
                    processor.xy(x, y, idxc)?;
                }
                processor.linestring_end(true, 0)
            }
            geo_buf::ObjectKind::MultiLineString => {
                let object = geo_buf::root_as_object(&self.buf[1..])
                    .map_err(|_| GeozeroError::GeometryFormat)?;
                let point_offsets =
                    object
                        .point_offsets()
                        .ok_or(GeozeroError::Geometry(String::from(
                            "invalid data, point_offsets missed",
                        )))?;
                processor.multilinestring_begin(point_offsets.len() - 1, 0)?;
                self.process_lines(processor, &point_offsets)?;
                processor.multilinestring_end(0)
            }
            geo_buf::ObjectKind::Polygon => {
                let object = geo_buf::root_as_object(&self.buf[1..])
                    .map_err(|_| GeozeroError::GeometryFormat)?;
                let point_offsets =
                    object
                        .point_offsets()
                        .ok_or(GeozeroError::Geometry(String::from(
                            "invalid data, point_offsets missed",
                        )))?;
                processor.polygon_begin(true, point_offsets.len() - 1, 0)?;
                self.process_lines(processor, &point_offsets)?;
                processor.polygon_end(true, 0)
            }
            geo_buf::ObjectKind::MultiPolygon => {
                let object = geo_buf::root_as_object(&self.buf[1..])
                    .map_err(|_| GeozeroError::GeometryFormat)?;
                let point_offsets = object.point_offsets().ok_or(GeozeroError::GeometryFormat)?;
                let ring_offsets = object.ring_offsets().ok_or(GeozeroError::GeometryFormat)?;
                processor.multipolygon_begin(ring_offsets.len() - 1, 0)?;
                self.process_polygons(processor, &point_offsets, &ring_offsets)?;
                processor.multipolygon_end(0)
            }
            geo_buf::ObjectKind::Collection => {
                let object = geo_buf::root_as_object(&self.buf[1..])
                    .map_err(|_| GeozeroError::GeometryFormat)?;
                let collection = object.collection().ok_or(GeozeroError::GeometryFormat)?;
                processor.geometrycollection_begin(collection.len(), 0)?;
                for (idx2, geometry) in collection.iter().enumerate() {
                    self.process_inner(processor, &geometry, idx2)?;
                }
                processor.geometrycollection_end(0)
            }
            geo_buf::ObjectKind(OUT_OF_RANGE..) => unreachable!(),
        }
    }
}

impl Geometry {
    fn process_lines<P>(
        &self,
        processor: &mut P,
        point_offsets: &Vector<u32>,
    ) -> geozero::error::Result<()>
    where
        P: geozero::GeomProcessor,
    {
        point_offsets
            .iter()
            .map_windows(|[start, end]| *start as usize..*end as usize)
            .enumerate()
            .try_for_each(|(idx, range)| {
                processor.linestring_begin(false, range.len(), idx)?;
                for (idxc, pos) in range.enumerate() {
                    processor.xy(self.column_x[pos], self.column_y[pos], idxc)?;
                }
                processor.linestring_end(false, idx)
            })
    }

    fn process_polygons<P>(
        &self,
        processor: &mut P,
        point_offsets: &Vector<u32>,
        ring_offsets: &Vector<u32>,
    ) -> geozero::error::Result<()>
    where
        P: geozero::GeomProcessor,
    {
        ring_offsets
            .iter()
            .map_windows(|[start, end]| *start as usize..=*end as usize)
            .enumerate()
            .try_for_each(|(idx, range)| {
                processor.polygon_begin(false, range.end() - range.start(), idx)?;
                range
                    .map(|i| point_offsets.get(i))
                    .map_windows(|[start, end]| *start as usize..*end as usize)
                    .enumerate()
                    .try_for_each(|(idx, range)| {
                        processor.linestring_begin(false, range.len(), idx)?;
                        for (idxc, pos) in range.enumerate() {
                            processor.xy(self.column_x[pos], self.column_y[pos], idxc)?;
                        }
                        processor.linestring_end(false, idx)
                    })?;
                processor.polygon_end(false, idx)
            })
    }

    fn process_inner<P>(
        &self,
        processor: &mut P,
        object: &InnerObject,
        idx: usize,
    ) -> geozero::error::Result<()>
    where
        P: geozero::GeomProcessor,
    {
        match geo_buf::InnerObjectKind(object.wkb_type() as u8) {
            geo_buf::InnerObjectKind::Point => {
                let pos = object
                    .point_offsets()
                    .ok_or(GeozeroError::GeometryFormat)?
                    .get(0);
                processor.point_begin(idx)?;
                processor.xy(self.column_x[pos as usize], self.column_y[pos as usize], 0)?;
                processor.point_end(idx)
            }
            geo_buf::InnerObjectKind::MultiPoint => {
                let point_offsets = object.point_offsets().ok_or(GeozeroError::GeometryFormat)?;

                let start = point_offsets.get(0);
                let end = point_offsets.get(1);

                let range = start as usize..end as usize;
                processor.multipoint_begin(range.len(), idx)?;
                for (idxc, pos) in range.enumerate() {
                    processor.xy(self.column_x[pos], self.column_y[pos], idxc)?;
                }
                processor.multipoint_end(idx)
            }
            geo_buf::InnerObjectKind::LineString => {
                let point_offsets = object.point_offsets().ok_or(GeozeroError::GeometryFormat)?;

                let start = point_offsets.get(0);
                let end = point_offsets.get(1);
                let range = start as usize..end as usize;
                processor.linestring_begin(true, range.len(), idx)?;
                for (idxc, pos) in range.enumerate() {
                    processor.xy(self.column_x[pos], self.column_y[pos], idxc)?;
                }
                processor.linestring_end(true, idx)
            }
            geo_buf::InnerObjectKind::MultiLineString => {
                let point_offsets =
                    object
                        .point_offsets()
                        .ok_or(GeozeroError::Geometry(String::from(
                            "invalid data, point_offsets missed",
                        )))?;
                processor.multilinestring_begin(point_offsets.len() - 1, idx)?;
                self.process_lines(processor, &point_offsets)?;
                processor.multilinestring_end(idx)
            }
            geo_buf::InnerObjectKind::Polygon => {
                let point_offsets =
                    object
                        .point_offsets()
                        .ok_or(GeozeroError::Geometry(String::from(
                            "invalid data, point_offsets missed",
                        )))?;
                processor.polygon_begin(true, point_offsets.len() - 1, idx)?;
                self.process_lines(processor, &point_offsets)?;
                processor.polygon_end(true, idx)
            }
            geo_buf::InnerObjectKind::MultiPolygon => {
                let point_offsets = object.point_offsets().ok_or(GeozeroError::GeometryFormat)?;
                let ring_offsets = object.ring_offsets().ok_or(GeozeroError::GeometryFormat)?;
                processor.multipolygon_begin(ring_offsets.len() - 1, idx)?;
                self.process_polygons(processor, &point_offsets, &ring_offsets)?;
                processor.multipoint_end(idx)
            }
            geo_buf::InnerObjectKind::Collection => {
                let collection = object.collection().ok_or(GeozeroError::GeometryFormat)?;
                processor.geometrycollection_begin(collection.len(), idx)?;
                for (idx2, geometry) in collection.iter().enumerate() {
                    self.process_inner(processor, &geometry, idx2)?;
                }
                processor.geometrycollection_end(idx)
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use geozero::CoordDimensions;
    use geozero::ToWkb;
    use geozero::ToWkt;

    use super::*;
    use crate::Wkt;

    #[test]
    fn test_from_wkt() {
        run_from_wkt(&"POINT(-122.35 37.55)");
        run_from_wkt(&"MULTIPOINT(-122.35 37.55,0 -90)");

        run_from_wkt(&"LINESTRING(-124.2 42,-120.01 41.99)");
        run_from_wkt(&"LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)");

        run_from_wkt(&"MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))");
        run_from_wkt(
            &"MULTILINESTRING((-124.2 42,-120.01 41.99),(-124.2 42,-120.01 41.99,-122.5 42.01,-122.5 42.01),(-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))",
        );
        run_from_wkt(&"POLYGON((17 17,17 30,30 30,30 17,17 17))");
        run_from_wkt(
            &"POLYGON((100 0,101 0,101 1,100 1,100 0),(100.8 0.8,100.8 0.2,100.2 0.2,100.2 0.8,100.8 0.8))",
        );
        run_from_wkt(&"MULTIPOLYGON(((-10 0,0 10,10 0,-10 0)),((-10 40,10 40,0 20,-10 40)))");
        run_from_wkt(
            &"GEOMETRYCOLLECTION(POINT(99 11),LINESTRING(40 60,50 50,60 40),POINT(99 10))",
        );
        run_from_wkt(
            &"GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),LINESTRING(40 60,50 50,60 40),POINT(99 11))",
        );
        run_from_wkt(
            &"GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),GEOMETRYCOLLECTION(LINESTRING(40 60,50 50,60 40),POINT(99 11)),POINT(50 70))",
        );
    }

    fn run_from_wkt(want: &str) {
        let geom = Geometry::try_from(Wkt(want)).unwrap();

        let mut builder = crate::GeometryBuilder::new();
        geozero::GeozeroGeometry::process_geom(&geom, &mut builder).unwrap();
        let got = builder.build().to_wkt().unwrap();

        assert_eq!(want, got)
    }
}
