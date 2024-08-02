use flatbuffers::FlatBufferBuilder;
use flatbuffers::Vector;
use flatbuffers::WIPOffset;
use geozero::error::Result as GeoResult;

use super::geo_buf;
use super::geo_buf::InnerObject;
use super::geo_buf::Object;
use super::Geometry;
use super::Visitor;

#[derive(Default)]
pub struct GeometryBuilder<'fbb> {
    fbb: flatbuffers::FlatBufferBuilder<'fbb>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
    point_offsets: Vec<u32>,
    ring_offsets: Vec<u32>,
    temp_kind: Option<geo_buf::ObjectKind>,

    kind: Option<geo_buf::ObjectKind>,
    stack: Vec<Vec<WIPOffset<InnerObject<'fbb>>>>,
    object: Option<WIPOffset<Object<'fbb>>>,
}

impl<'fbb> GeometryBuilder<'fbb> {
    pub fn new() -> Self {
        Self {
            fbb: FlatBufferBuilder::new(),
            column_x: Vec::new(),
            column_y: Vec::new(),
            point_offsets: Vec::new(),
            ring_offsets: Vec::new(),
            kind: None,
            stack: Vec::new(),
            object: None,
            temp_kind: None,
        }
    }

    pub fn point_len(&self) -> usize {
        self.column_x.len()
    }

    pub fn push_point(&mut self, x: f64, y: f64) {
        self.column_x.push(x);
        self.column_y.push(y);
    }

    fn create_point_offsets(&mut self) -> Option<WIPOffset<Vector<'fbb, u32>>> {
        let v = Some(self.fbb.create_vector(&self.point_offsets));
        self.point_offsets.clear();
        v
    }

    fn create_ring_offsets(&mut self) -> Option<WIPOffset<Vector<'fbb, u32>>> {
        let v = Some(self.fbb.create_vector(&self.ring_offsets));
        self.ring_offsets.clear();
        v
    }

    pub fn build(self) -> Geometry {
        let GeometryBuilder {
            mut fbb,
            column_x,
            column_y,
            kind,
            object,
            ..
        } = self;

        match kind.unwrap() {
            kind @ geo_buf::ObjectKind::Point
            | kind @ geo_buf::ObjectKind::MultiPoint
            | kind @ geo_buf::ObjectKind::LineString => Geometry {
                buf: vec![kind.0],
                column_x,
                column_y,
            },
            kind => {
                geo_buf::finish_object_buffer(&mut fbb, object.unwrap());

                let kind = kind.0;
                let (mut buf, index) = fbb.collapse();
                let buf = if index > 0 {
                    let index = index - 1;
                    buf[index] = kind;
                    buf[index..].to_vec()
                } else {
                    [vec![kind], buf].concat()
                };

                Geometry {
                    buf,
                    column_x,
                    column_y,
                }
            }
        }
    }
}

impl<'fbb> Visitor for GeometryBuilder<'fbb> {
    fn visit_point(&mut self, x: f64, y: f64, multi: bool) -> Result<(), anyhow::Error> {
        if self.stack.is_empty() {
            self.push_point(x, y);
            return Ok(());
        }

        if multi {
            self.push_point(x, y);
        } else {
            if self.point_offsets.is_empty() {
                self.point_offsets.push(self.point_len() as u32)
            }
            self.push_point(x, y);
            self.point_offsets.push(self.point_len() as u32);
        }

        Ok(())
    }

    fn visit_points_start(&mut self, _: usize) -> Result<(), anyhow::Error> {
        if !self.stack.is_empty() && self.point_offsets.is_empty() {
            self.point_offsets.push(self.point_len() as u32)
        }
        Ok(())
    }

    fn visit_points_end(&mut self, multi: bool) -> Result<(), anyhow::Error> {
        if multi || !self.stack.is_empty() {
            self.point_offsets.push(self.point_len() as u32);
        }
        Ok(())
    }

    fn visit_lines_start(&mut self, _: usize) -> Result<(), anyhow::Error> {
        if self.point_offsets.is_empty() {
            self.point_offsets.push(self.point_len() as u32)
        }
        Ok(())
    }

    fn visit_lines_end(&mut self) -> Result<(), anyhow::Error> {
        if !self.stack.is_empty() {
            self.ring_offsets.push(self.point_offsets.len() as u32 - 1);
        }
        Ok(())
    }

    fn visit_polygon_start(&mut self, _: usize) -> Result<(), anyhow::Error> {
        if self.point_offsets.is_empty() {
            self.point_offsets.push(self.point_len() as u32)
        }
        Ok(())
    }

    fn visit_polygon_end(&mut self, multi: bool) -> Result<(), anyhow::Error> {
        if multi || !self.stack.is_empty() {
            self.ring_offsets.push(self.point_offsets.len() as u32 - 1);
        }
        Ok(())
    }

    fn visit_polygons_start(&mut self, _: usize) -> Result<(), anyhow::Error> {
        if self.ring_offsets.is_empty() {
            self.ring_offsets.push(self.point_len() as u32)
        }
        Ok(())
    }

    fn visit_polygons_end(&mut self) -> Result<(), anyhow::Error> {
        Ok(())
    }

    fn visit_collection_start(&mut self, n: usize) -> Result<(), anyhow::Error> {
        self.stack.push(Vec::with_capacity(n));
        Ok(())
    }

    fn visit_collection_end(&mut self) -> Result<(), anyhow::Error> {
        Ok(())
    }

    fn finish(&mut self, kind: geo_buf::ObjectKind) -> Result<(), anyhow::Error> {
        if self.stack.is_empty() {
            let object = match kind {
                geo_buf::ObjectKind::Point => {
                    let point_offsets = self.create_point_offsets();
                    geo_buf::Object::create(&mut self.fbb, &geo_buf::ObjectArgs {
                        point_offsets,
                        ..Default::default()
                    })
                }
                geo_buf::ObjectKind::MultiPoint => {
                    let point_offsets = self.create_point_offsets();
                    geo_buf::Object::create(&mut self.fbb, &geo_buf::ObjectArgs {
                        point_offsets,
                        ..Default::default()
                    })
                }
                geo_buf::ObjectKind::LineString => {
                    let point_offsets = self.create_point_offsets();
                    geo_buf::Object::create(&mut self.fbb, &geo_buf::ObjectArgs {
                        point_offsets,
                        ..Default::default()
                    })
                }
                geo_buf::ObjectKind::MultiLineString => {
                    let point_offsets = self.create_point_offsets();
                    let ring_offsets = self.create_ring_offsets();
                    geo_buf::Object::create(&mut self.fbb, &geo_buf::ObjectArgs {
                        point_offsets,
                        ring_offsets,
                        ..Default::default()
                    })
                }
                geo_buf::ObjectKind::Polygon => {
                    let point_offsets = self.create_point_offsets();
                    let ring_offsets = self.create_ring_offsets();
                    geo_buf::Object::create(&mut self.fbb, &geo_buf::ObjectArgs {
                        point_offsets,
                        ring_offsets,
                        ..Default::default()
                    })
                }
                geo_buf::ObjectKind::MultiPolygon => {
                    let point_offsets = self.create_point_offsets();
                    let ring_offsets = self.create_ring_offsets();
                    geo_buf::Object::create(&mut self.fbb, &geo_buf::ObjectArgs {
                        point_offsets,
                        ring_offsets,
                        ..Default::default()
                    })
                }
                _ => unreachable!(),
            };
            self.kind = Some(kind);
            self.object = Some(object);
            return Ok(());
        }

        if self.stack.len() == 1 && kind == geo_buf::ObjectKind::Collection {
            let geometrys = self.stack.pop().unwrap();
            let collection = Some(self.fbb.create_vector(&geometrys));
            let object = geo_buf::Object::create(&mut self.fbb, &geo_buf::ObjectArgs {
                collection,
                ..Default::default()
            });
            self.kind = Some(kind);
            self.object = Some(object);
            return Ok(());
        }

        let object = match kind {
            geo_buf::ObjectKind::Point => {
                let point_offsets = self.create_point_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::Point.0 as u32,
                    point_offsets,
                    ..Default::default()
                })
            }
            geo_buf::ObjectKind::MultiPoint => {
                let point_offsets = self.create_point_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::MultiPoint.0 as u32,
                    point_offsets,
                    ..Default::default()
                })
            }
            geo_buf::ObjectKind::LineString => {
                let point_offsets = self.create_point_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::LineString.0 as u32,
                    point_offsets,
                    ..Default::default()
                })
            }
            geo_buf::ObjectKind::MultiLineString => {
                let point_offsets = self.create_point_offsets();
                let ring_offsets = self.create_ring_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::MultiLineString.0 as u32,
                    point_offsets,
                    ring_offsets,
                    ..Default::default()
                })
            }
            geo_buf::ObjectKind::Polygon => {
                let point_offsets = self.create_point_offsets();
                let ring_offsets = self.create_ring_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::Polygon.0 as u32,
                    point_offsets,
                    ring_offsets,
                    ..Default::default()
                })
            }
            geo_buf::ObjectKind::MultiPolygon => {
                let point_offsets = self.create_point_offsets();
                let ring_offsets = self.create_ring_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::MultiPolygon.0 as u32,
                    point_offsets,
                    ring_offsets,
                    ..Default::default()
                })
            }
            geo_buf::ObjectKind::Collection => {
                let geometrys = self.stack.pop().unwrap();
                let collection = Some(self.fbb.create_vector(&geometrys));
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::Collection.0 as u32,
                    collection,
                    ..Default::default()
                })
            }
            _ => unreachable!(),
        };

        self.stack.last_mut().unwrap().push(object);

        Ok(())
    }
}

impl<'fbb> geozero::GeomProcessor for GeometryBuilder<'fbb> {
    fn multi_dim(&self) -> bool {
        false
    }

    fn srid(&mut self, _srid: Option<i32>) -> GeoResult<()> {
        Ok(())
    }

    fn xy(&mut self, x: f64, y: f64, _: usize) -> GeoResult<()> {
        let multi = !matches!(self.temp_kind, Some(geo_buf::ObjectKind::Point));
        self.visit_point(x, y, multi).map_err(map_anyhow)?;
        Ok(())
    }

    fn point_begin(&mut self, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(geo_buf::ObjectKind::Point);
        Ok(())
    }

    fn point_end(&mut self, _: usize) -> GeoResult<()> {
        if let Some(kind @ geo_buf::ObjectKind::Point) = self.temp_kind {
            self.finish(kind).map_err(map_anyhow)?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn multipoint_begin(&mut self, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind
            .get_or_insert(geo_buf::ObjectKind::MultiPoint);
        self.visit_points_start(size).map_err(map_anyhow)?;
        Ok(())
    }

    fn multipoint_end(&mut self, _: usize) -> GeoResult<()> {
        let multi = !matches!(self.temp_kind, Some(geo_buf::ObjectKind::MultiPoint));
        self.visit_points_end(multi).map_err(map_anyhow)?;
        if let Some(kind @ geo_buf::ObjectKind::MultiPoint) = self.temp_kind {
            self.finish(kind).map_err(map_anyhow)?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn linestring_begin(&mut self, _: bool, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind
            .get_or_insert(geo_buf::ObjectKind::LineString);
        self.visit_points_start(size).map_err(map_anyhow)
    }

    fn linestring_end(&mut self, tagged: bool, _: usize) -> GeoResult<()> {
        self.visit_points_end(!tagged).map_err(map_anyhow)?;
        if let Some(kind @ geo_buf::ObjectKind::LineString) = self.temp_kind {
            self.finish(kind).map_err(map_anyhow)?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn multilinestring_begin(&mut self, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind
            .get_or_insert(geo_buf::ObjectKind::MultiLineString);
        self.visit_lines_start(size).map_err(map_anyhow)
    }

    fn multilinestring_end(&mut self, _: usize) -> GeoResult<()> {
        self.visit_lines_end().map_err(map_anyhow)?;
        if let Some(kind @ geo_buf::ObjectKind::MultiLineString) = self.temp_kind {
            self.finish(kind).map_err(map_anyhow)?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn polygon_begin(&mut self, _: bool, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(geo_buf::ObjectKind::Polygon);
        self.visit_polygon_start(size).map_err(map_anyhow)
    }

    fn polygon_end(&mut self, tagged: bool, _: usize) -> GeoResult<()> {
        self.visit_polygon_end(!tagged).map_err(map_anyhow)?;
        if let Some(kind @ geo_buf::ObjectKind::Polygon) = self.temp_kind {
            self.finish(kind).map_err(map_anyhow)?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn multipolygon_begin(&mut self, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind
            .get_or_insert(geo_buf::ObjectKind::MultiPolygon);
        self.visit_polygons_start(size).map_err(map_anyhow)
    }

    fn multipolygon_end(&mut self, _: usize) -> GeoResult<()> {
        self.visit_polygons_end().map_err(map_anyhow)?;
        if let Some(kind @ geo_buf::ObjectKind::MultiPolygon) = self.temp_kind {
            self.finish(kind).map_err(map_anyhow)?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn geometrycollection_begin(&mut self, size: usize, _: usize) -> GeoResult<()> {
        self.visit_collection_start(size).map_err(map_anyhow)
    }

    fn geometrycollection_end(&mut self, _: usize) -> GeoResult<()> {
        self.visit_collection_end().map_err(map_anyhow)?;
        self.finish(geo_buf::ObjectKind::Collection)
            .map_err(map_anyhow)
    }
}

fn map_anyhow(value: anyhow::Error) -> geozero::error::GeozeroError {
    geozero::error::GeozeroError::Geometry(value.to_string())
}

#[cfg(test)]
mod tests {

    use geozero::GeozeroGeometry;

    use crate::GeometryBuilder;

    #[test]
    fn test_from_wkt() {
        run_from_wkt(&"POINT(-122.35 37.55)");
        run_from_wkt(&"MULTIPOINT(-122.35 37.55,0 -90)");
        run_from_wkt(&"LINESTRING(-124.2 42,-120.01 41.99)");
        run_from_wkt(&"LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)");
        run_from_wkt(&"MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))");
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
        let mut b = GeometryBuilder::new();
        geozero::wkt::Wkt(want).process_geom(&mut b).unwrap();
        let geom = b.build();
        let crate::Wkt::<String>(got) = (&geom).try_into().unwrap();
        assert_eq!(want, got);
    }
}
