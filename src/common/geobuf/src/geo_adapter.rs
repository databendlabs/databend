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

use geozero::error::GeozeroError;
use geozero::GeozeroGeometry;

use crate::geo_buf;
use crate::geo_buf::InnerObject;
use crate::Geometry;
use crate::GeometryBuilder;
use crate::ObjectKind;

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
        geozero::ToGeo::to_geo(self)
    }
}

impl geozero::GeozeroGeometry for Geometry {
    fn srid(&self) -> Option<i32> {
        self.srid()
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

impl Geometry {
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

    pub(crate) fn process_inner<P>(
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
    use geozero::ToGeo;
    use geozero::ToWkt;

    use super::*;

    #[test]
    fn test_cast_geom() {
        run_geo(&"POINT(-122.35 37.55)");
        run_geo(&"MULTIPOINT(-122.35 37.55,0 -90)");

        run_geo(&"LINESTRING(-124.2 42,-120.01 41.99)");
        run_geo(&"LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)");

        run_geo(&"MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))");
        run_geo(
            &"MULTILINESTRING((-124.2 42,-120.01 41.99),(-124.2 42,-120.01 41.99,-122.5 42.01,-122.5 42.01),(-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))",
        );
        run_geo(&"POLYGON((17 17,17 30,30 30,30 17,17 17))");
        run_geo(
            &"POLYGON((100 0,101 0,101 1,100 1,100 0),(100.8 0.8,100.8 0.2,100.2 0.2,100.2 0.8,100.8 0.8))",
        );
        run_geo(&"MULTIPOLYGON(((-10 0,0 10,10 0,-10 0)),((-10 40,10 40,0 20,-10 40)))");
        run_geo(&"GEOMETRYCOLLECTION(POINT(99 11),LINESTRING(40 60,50 50,60 40),POINT(99 10))");
        run_geo(
            &"GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),LINESTRING(40 60,50 50,60 40),POINT(99 11))",
        );
        run_geo(
            &"GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),GEOMETRYCOLLECTION(LINESTRING(40 60,50 50,60 40),POINT(99 11)),POINT(50 70))",
        );
    }

    fn run_geo(want: &str) {
        let geo_geom = geozero::wkt::Wkt(want).to_geo().unwrap();

        let geom = Geometry::try_from(&geo_geom).unwrap();
        let geo_geom: geo::Geometry<f64> = (&geom).try_into().unwrap();

        let got = geo_geom.to_wkt().unwrap();
        assert_eq!(want, got)
    }
}
