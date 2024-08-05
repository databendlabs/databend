use flatbuffers::Vector;
use geozero::error::GeozeroError;
use geozero::ColumnValue;
use geozero::GeozeroGeometry;
use geozero::ToGeo;

use super::geo_buf;
use super::geo_buf::InnerObject;
use super::geo_buf::Object;
use super::Geometry;
use super::GeometryBuilder;
use super::JsonObject;
use super::ObjectKind;
use crate::FeatureKind;

impl TryFrom<&geo::Geometry<f64>> for Geometry {
    type Error = GeozeroError;

    fn try_from(geo: &geo::Geometry<f64>) -> Result<Self, Self::Error> {
        let mut builder = GeometryBuilder::new();
        geo.process_geom(&mut builder)?;
        Ok(builder.build())
    }
}

impl TryInto<geo::Geometry<f64>> for &Geometry {
    type Error = GeozeroError;

    fn try_into(self) -> Result<geo::Geometry<f64>, Self::Error> {
        debug_assert!(self.column_x.len() == self.column_y.len());

        self.to_geo()
    }
}

impl geozero::GeozeroGeometry for Geometry {
    fn srid(&self) -> Option<i32> {
        if self.buf.len() > 1 {
            self.read_object()
                .map_or(None, |object| Some(object.srid()))
        } else {
            None
        }
    }

    fn process_geom<P>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        P: geozero::GeomProcessor,
        Self: Sized,
    {
        debug_assert!(self.column_x.len() == self.column_y.len());
        match self.kind()? {
            ObjectKind::Point => {
                debug_assert!(self.column_x.len() == 1);
                processor.point_begin(0)?;
                processor.xy(self.column_x[0], self.column_y[0], 0)?;
                processor.point_end(0)
            }
            ObjectKind::MultiPoint => {
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
            ObjectKind::LineString => {
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
            ObjectKind::MultiLineString => {
                let object = self.read_object()?;
                let point_offsets = read_point_offsets(object.point_offsets())?;
                processor.multilinestring_begin(point_offsets.len() - 1, 0)?;
                self.process_lines(processor, &point_offsets)?;
                processor.multilinestring_end(0)
            }
            ObjectKind::Polygon => {
                let object = self.read_object()?;
                let point_offsets = read_point_offsets(object.point_offsets())?;
                processor.polygon_begin(true, point_offsets.len() - 1, 0)?;
                self.process_lines(processor, &point_offsets)?;
                processor.polygon_end(true, 0)
            }
            ObjectKind::MultiPolygon => {
                let object = self.read_object()?;
                let point_offsets = read_point_offsets(object.point_offsets())?;
                let ring_offsets = read_ring_offsets(object.ring_offsets())?;
                processor.multipolygon_begin(ring_offsets.len() - 1, 0)?;
                self.process_polygons(processor, &point_offsets, &ring_offsets)?;
                processor.multipolygon_end(0)
            }
            ObjectKind::GeometryCollection => {
                let object = self.read_object()?;
                let collection = object.collection().ok_or(GeozeroError::Geometry(
                    "Invalid Collection, collection missing".to_string(),
                ))?;
                processor.geometrycollection_begin(collection.len(), 0)?;
                for (idx2, geometry) in collection.iter().enumerate() {
                    self.process_inner(processor, &geometry, idx2)?;
                }
                processor.geometrycollection_end(0)
            }
        }
    }
}

fn process_properties<P>(
    properties: &Option<Vector<u8>>,
    processor: &mut P,
) -> geozero::error::Result<()>
where
    P: geozero::FeatureProcessor,
{
    if let Some(data) = properties {
        let json: JsonObject = flexbuffers::from_slice(data.bytes())
            .map_err(|e| GeozeroError::Geometry(e.to_string()))?;
        for (idx, (name, value)) in json.iter().enumerate() {
            processor.property(
                idx,
                name,
                &ColumnValue::Json(
                    &serde_json::to_string(value)
                        .map_err(|e| GeozeroError::Geometry(e.to_string()))?,
                ),
            )?;
        }
    }
    Ok(())
}

impl Geometry {
    fn read_object(&self) -> Result<Object, GeozeroError> {
        geo_buf::root_as_object(&self.buf[1..]).map_err(|e| GeozeroError::Geometry(e.to_string()))
    }

    fn process_lines<P>(
        &self,
        processor: &mut P,
        point_offsets: &flexbuffers::VectorReader<&[u8]>,
    ) -> geozero::error::Result<()>
    where
        P: geozero::GeomProcessor,
    {
        point_offsets
            .iter()
            .map_windows(|[start, end]| start.as_u32() as usize..end.as_u32() as usize)
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
        point_offsets: &flexbuffers::VectorReader<&[u8]>,
        ring_offsets: &flexbuffers::VectorReader<&[u8]>,
    ) -> geozero::error::Result<()>
    where
        P: geozero::GeomProcessor,
    {
        ring_offsets
            .iter()
            .map_windows(|[start, end]| start.as_u32() as usize..=end.as_u32() as usize)
            .enumerate()
            .try_for_each(|(idx, range)| {
                processor.polygon_begin(false, range.end() - range.start(), idx)?;
                range
                    .map(|i| point_offsets.index(i).unwrap().as_u32())
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
        match object.wkb_type() {
            geo_buf::InnerObjectKind::Point => {
                let point_offsets = read_point_offsets(object.point_offsets())?;
                let pos = point_offsets.index(0).unwrap().as_u32() as usize;

                processor.point_begin(idx)?;
                processor.xy(self.column_x[pos], self.column_y[pos], 0)?;
                processor.point_end(idx)
            }
            geo_buf::InnerObjectKind::MultiPoint => {
                let point_offsets = read_point_offsets(object.point_offsets())?;
                let start = point_offsets.index(0).unwrap().as_u32() as usize;
                let end = point_offsets.index(1).unwrap().as_u32() as usize;
                let range = start..end;

                processor.multipoint_begin(range.len(), idx)?;
                for (idxc, pos) in range.enumerate() {
                    processor.xy(self.column_x[pos], self.column_y[pos], idxc)?;
                }
                processor.multipoint_end(idx)
            }
            geo_buf::InnerObjectKind::LineString => {
                let point_offsets = read_point_offsets(object.point_offsets())?;
                let start = point_offsets.index(0).unwrap().as_u32() as usize;
                let end = point_offsets.index(1).unwrap().as_u32() as usize;
                let range = start..end;

                processor.linestring_begin(true, range.len(), idx)?;
                for (idxc, pos) in range.enumerate() {
                    processor.xy(self.column_x[pos], self.column_y[pos], idxc)?;
                }
                processor.linestring_end(true, idx)
            }
            geo_buf::InnerObjectKind::MultiLineString => {
                let point_offsets = read_point_offsets(object.point_offsets())?;
                processor.multilinestring_begin(point_offsets.len() - 1, idx)?;
                self.process_lines(processor, &point_offsets)?;
                processor.multilinestring_end(idx)
            }
            geo_buf::InnerObjectKind::Polygon => {
                let point_offsets = read_point_offsets(object.point_offsets())?;
                processor.polygon_begin(true, point_offsets.len() - 1, idx)?;
                self.process_lines(processor, &point_offsets)?;
                processor.polygon_end(true, idx)
            }
            geo_buf::InnerObjectKind::MultiPolygon => {
                let point_offsets = read_point_offsets(object.point_offsets())?;
                let ring_offsets = read_ring_offsets(object.ring_offsets())?;
                processor.multipolygon_begin(ring_offsets.len() - 1, idx)?;
                self.process_polygons(processor, &point_offsets, &ring_offsets)?;
                processor.multipoint_end(idx)
            }
            geo_buf::InnerObjectKind::Collection => {
                let collection = object.collection().ok_or(GeozeroError::Geometry(
                    "Invalid Collection, collection missing".to_string(),
                ))?;
                processor.geometrycollection_begin(collection.len(), idx)?;
                for (idx2, geometry) in collection.iter().enumerate() {
                    self.process_inner(processor, &geometry, idx2)?;
                }
                processor.geometrycollection_end(idx)
            }
            _ => unreachable!(),
        }
    }

    fn kind(&self) -> Result<ObjectKind, GeozeroError> {
        let kind: FeatureKind = self.buf[0]
            .try_into()
            .map_err(|_| GeozeroError::Geometry("Invalid data".to_string()))?;

        match kind {
            FeatureKind::Geometry(o) | FeatureKind::Feature(o) => Ok(o),
            FeatureKind::FeatureCollection => Ok(ObjectKind::GeometryCollection),
        }
    }

    pub fn process_features<P>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
        P: geozero::FeatureProcessor,
    {
        let kind: FeatureKind = self.buf[0]
            .try_into()
            .map_err(|_| GeozeroError::Geometry("Invalid data".to_string()))?;

        match kind {
            FeatureKind::Geometry(_) => self.process_geom(processor),
            FeatureKind::Feature(_) => {
                let object = self.read_object()?;
                processor.feature_begin(0)?;
                processor.properties_begin()?;
                process_properties(&object.properties(), processor)?;
                processor.properties_end()?;
                processor.geometry_begin()?;
                self.process_geom(processor)?;
                processor.geometry_end()?;
                processor.feature_end(0)
            }
            FeatureKind::FeatureCollection => {
                processor.dataset_begin(None)?;
                for (idx, feature) in self
                    .read_object()
                    .unwrap()
                    .collection()
                    .unwrap()
                    .iter()
                    .enumerate()
                {
                    processor.feature_begin(idx as u64)?;
                    processor.properties_begin()?;
                    process_properties(&feature.properties(), processor)?;
                    processor.properties_end()?;
                    processor.geometry_begin()?;
                    self.process_inner(processor, &feature, 0)?;
                    processor.geometry_end()?;
                    processor.feature_end(idx as u64)?;
                }
                processor.dataset_end()
            }
        }
    }
}

fn read_point_offsets(
    point_offsets: Option<flatbuffers::Vector<'_, u8>>,
) -> Result<flexbuffers::VectorReader<&[u8]>, GeozeroError> {
    let data = point_offsets.ok_or(GeozeroError::Geometry(
        "Invalid MultiLineString, point_offsets missing".to_string(),
    ))?;
    let offsets = flexbuffers::Reader::get_root(data.bytes())
        .map_err(|e| GeozeroError::Geometry(e.to_string()))?
        .as_vector();
    Ok(offsets)
}

fn read_ring_offsets(
    ring_offsets: Option<flatbuffers::Vector<'_, u8>>,
) -> Result<flexbuffers::VectorReader<&[u8]>, GeozeroError> {
    let data = ring_offsets.ok_or(GeozeroError::Geometry(
        "Invalid MultiLineString, ring_offsets missing".to_string(),
    ))?;
    let offsets = flexbuffers::Reader::get_root(data.bytes())
        .map_err(|e| GeozeroError::Geometry(e.to_string()))?
        .as_vector();
    Ok(offsets)
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
