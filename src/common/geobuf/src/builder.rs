use flatbuffers::FlatBufferBuilder;
use flatbuffers::ForwardsUOffset;
use flatbuffers::Vector;
use flatbuffers::WIPOffset;
use geozero::error::Result as GeoResult;
use geozero::ColumnValue;

use super::geo_buf;
use super::geo_buf::InnerObject;
use super::geo_buf::Object;
use super::geo_buf::Property;
use super::Geometry;
use super::ObjectKind;
use super::Visitor;

#[derive(Default)]
pub struct GeometryBuilder<'fbb> {
    fbb: flatbuffers::FlatBufferBuilder<'fbb>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
    point_offsets: Vec<u32>,
    ring_offsets: Vec<u32>,
    properties: Vec<WIPOffset<Property<'fbb>>>,
    temp_kind: Option<ObjectKind>,
    is_feature: bool,

    kind: Option<ObjectKind>,
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
            properties: Vec::new(),
            temp_kind: None,
            is_feature: false,

            kind: None,
            stack: Vec::new(),
            object: None,
        }
    }

    pub fn point_len(&self) -> usize {
        self.column_x.len()
    }

    pub fn push_point(&mut self, x: f64, y: f64) {
        self.column_x.push(x);
        self.column_y.push(y);
    }

    fn create_point_offsets(&mut self) -> Option<WIPOffset<Vector<'fbb, u8>>> {
        let data = flexbuffers::singleton(&self.point_offsets);
        let v = Some(self.fbb.create_vector(&data));
        self.point_offsets.clear();
        v
    }

    fn create_ring_offsets(&mut self) -> Option<WIPOffset<Vector<'fbb, u8>>> {
        if self.ring_offsets.is_empty() {
            None
        } else {
            let data = flexbuffers::singleton(&self.ring_offsets);
            let v = Some(self.fbb.create_vector(&data));
            self.ring_offsets.clear();
            v
        }
    }

    fn create_properties(
        &mut self,
    ) -> Option<WIPOffset<Vector<'fbb, ForwardsUOffset<Property<'fbb>>>>> {
        if self.properties.is_empty() {
            None
        } else {
            Some(self.fbb.create_vector(&self.properties))
        }
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
            kind @ ObjectKind::Point
            | kind @ ObjectKind::MultiPoint
            | kind @ ObjectKind::LineString => Geometry {
                buf: vec![kind as u8],
                column_x,
                column_y,
            },
            kind => {
                geo_buf::finish_object_buffer(&mut fbb, object.unwrap());

                let kind = kind as u8;
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
    fn visit_point(&mut self, x: f64, y: f64, multi: bool) -> GeoResult<()> {
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

    fn visit_points_start(&mut self, _: usize) -> GeoResult<()> {
        if !self.stack.is_empty() && self.point_offsets.is_empty() {
            self.point_offsets.push(self.point_len() as u32)
        }
        Ok(())
    }

    fn visit_points_end(&mut self, multi: bool) -> GeoResult<()> {
        if multi || !self.stack.is_empty() {
            self.point_offsets.push(self.point_len() as u32);
        }
        Ok(())
    }

    fn visit_lines_start(&mut self, _: usize) -> GeoResult<()> {
        if self.point_offsets.is_empty() {
            self.point_offsets.push(self.point_len() as u32)
        }
        Ok(())
    }

    fn visit_lines_end(&mut self) -> GeoResult<()> {
        if !self.stack.is_empty() {
            self.ring_offsets.push(self.point_offsets.len() as u32 - 1);
        }
        Ok(())
    }

    fn visit_polygon_start(&mut self, _: usize) -> GeoResult<()> {
        if self.point_offsets.is_empty() {
            self.point_offsets.push(self.point_len() as u32)
        }
        Ok(())
    }

    fn visit_polygon_end(&mut self, multi: bool) -> GeoResult<()> {
        if multi || !self.stack.is_empty() {
            self.ring_offsets.push(self.point_offsets.len() as u32 - 1);
        }
        Ok(())
    }

    fn visit_polygons_start(&mut self, _: usize) -> GeoResult<()> {
        if self.ring_offsets.is_empty() {
            self.ring_offsets.push(self.point_len() as u32)
        }
        Ok(())
    }

    fn visit_polygons_end(&mut self) -> GeoResult<()> {
        Ok(())
    }

    fn visit_collection_start(&mut self, n: usize) -> GeoResult<()> {
        self.stack.push(Vec::with_capacity(n));
        Ok(())
    }

    fn visit_collection_end(&mut self) -> GeoResult<()> {
        Ok(())
    }

    fn finish(&mut self, kind: ObjectKind) -> GeoResult<()> {
        if self.stack.is_empty() {
            let object = match kind {
                ObjectKind::Point | ObjectKind::MultiPoint | ObjectKind::LineString => {
                    let point_offsets = self.create_point_offsets();
                    let properties = self.create_properties();
                    geo_buf::Object::create(&mut self.fbb, &geo_buf::ObjectArgs {
                        point_offsets,
                        properties,
                        ..Default::default()
                    })
                }
                ObjectKind::MultiLineString | ObjectKind::Polygon | ObjectKind::MultiPolygon => {
                    let point_offsets = self.create_point_offsets();
                    let ring_offsets = self.create_ring_offsets();
                    let properties = self.create_properties();
                    geo_buf::Object::create(&mut self.fbb, &geo_buf::ObjectArgs {
                        point_offsets,
                        ring_offsets,
                        properties,
                        ..Default::default()
                    })
                }
                _ => unreachable!(),
            };
            self.kind = Some(kind);
            self.object = Some(object);
            return Ok(());
        }

        if self.stack.len() == 1 && kind == ObjectKind::Collection {
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
            ObjectKind::Point => {
                let point_offsets = self.create_point_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: kind.into(),
                    point_offsets,
                    ..Default::default()
                })
            }
            ObjectKind::MultiPoint => {
                let point_offsets = self.create_point_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: kind.into(),
                    point_offsets,
                    ..Default::default()
                })
            }
            ObjectKind::LineString => {
                let point_offsets = self.create_point_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: kind.into(),
                    point_offsets,
                    ..Default::default()
                })
            }
            ObjectKind::MultiLineString => {
                let point_offsets = self.create_point_offsets();
                let ring_offsets = self.create_ring_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: kind.into(),
                    point_offsets,
                    ring_offsets,
                    ..Default::default()
                })
            }
            ObjectKind::Polygon => {
                let point_offsets = self.create_point_offsets();
                let ring_offsets = self.create_ring_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: kind.into(),
                    point_offsets,
                    ring_offsets,
                    ..Default::default()
                })
            }
            ObjectKind::MultiPolygon => {
                let point_offsets = self.create_point_offsets();
                let ring_offsets = self.create_ring_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: kind.into(),
                    point_offsets,
                    ring_offsets,
                    ..Default::default()
                })
            }
            ObjectKind::Collection => {
                let geometrys = self.stack.pop().unwrap();
                let collection = Some(self.fbb.create_vector(&geometrys));
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: kind.into(),
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

    fn empty_point(&mut self, idx: usize) -> GeoResult<()> {
        self.xy(f64::NAN, f64::NAN, idx)
    }

    fn xy(&mut self, x: f64, y: f64, _: usize) -> GeoResult<()> {
        let multi = !matches!(self.temp_kind, Some(ObjectKind::Point));
        self.visit_point(x, y, multi)?;
        Ok(())
    }

    fn point_begin(&mut self, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(ObjectKind::Point);
        Ok(())
    }

    fn point_end(&mut self, _: usize) -> GeoResult<()> {
        if let Some(kind @ ObjectKind::Point) = self.temp_kind {
            self.finish(kind)?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn multipoint_begin(&mut self, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(ObjectKind::MultiPoint);
        self.visit_points_start(size)?;
        Ok(())
    }

    fn multipoint_end(&mut self, _: usize) -> GeoResult<()> {
        let multi = !matches!(self.temp_kind, Some(ObjectKind::MultiPoint));
        self.visit_points_end(multi)?;
        if let Some(kind @ ObjectKind::MultiPoint) = self.temp_kind {
            self.finish(kind)?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn linestring_begin(&mut self, _: bool, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(ObjectKind::LineString);
        self.visit_points_start(size)
    }

    fn linestring_end(&mut self, tagged: bool, _: usize) -> GeoResult<()> {
        self.visit_points_end(!tagged)?;
        if let Some(kind @ ObjectKind::LineString) = self.temp_kind {
            self.finish(kind)?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn multilinestring_begin(&mut self, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(ObjectKind::MultiLineString);
        self.visit_lines_start(size)
    }

    fn multilinestring_end(&mut self, _: usize) -> GeoResult<()> {
        self.visit_lines_end()?;
        if let Some(kind @ ObjectKind::MultiLineString) = self.temp_kind {
            self.finish(kind)?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn polygon_begin(&mut self, _: bool, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(ObjectKind::Polygon);
        self.visit_polygon_start(size)
    }

    fn polygon_end(&mut self, tagged: bool, _: usize) -> GeoResult<()> {
        self.visit_polygon_end(!tagged)?;
        if let Some(kind @ ObjectKind::Polygon) = self.temp_kind {
            self.finish(kind)?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn multipolygon_begin(&mut self, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(ObjectKind::MultiPolygon);
        self.visit_polygons_start(size)
    }

    fn multipolygon_end(&mut self, _: usize) -> GeoResult<()> {
        self.visit_polygons_end()?;
        if let Some(kind @ ObjectKind::MultiPolygon) = self.temp_kind {
            self.finish(kind)?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn geometrycollection_begin(&mut self, size: usize, _: usize) -> GeoResult<()> {
        self.visit_collection_start(size)
    }

    fn geometrycollection_end(&mut self, _: usize) -> GeoResult<()> {
        self.visit_collection_end()?;
        self.finish(ObjectKind::Collection)
    }
}

impl<'fbb> geozero::PropertyProcessor for GeometryBuilder<'fbb> {
    fn property(&mut self, _: usize, name: &str, value: &geozero::ColumnValue) -> GeoResult<bool> {
        let name = Some(self.fbb.create_shared_string(name));
        let value = match value {
            ColumnValue::Byte(v) => flexbuffers::singleton(*v),
            ColumnValue::UByte(v) => flexbuffers::singleton(*v),
            ColumnValue::Bool(v) => flexbuffers::singleton(*v),
            ColumnValue::Short(v) => flexbuffers::singleton(*v),
            ColumnValue::UShort(v) => flexbuffers::singleton(*v),
            ColumnValue::Int(v) => flexbuffers::singleton(*v),
            ColumnValue::UInt(v) => flexbuffers::singleton(*v),
            ColumnValue::Long(v) => flexbuffers::singleton(*v),
            ColumnValue::ULong(v) => flexbuffers::singleton(*v),
            ColumnValue::Float(v) => flexbuffers::singleton(*v),
            ColumnValue::Double(v) => flexbuffers::singleton(*v),
            ColumnValue::String(v) => flexbuffers::singleton(*v),
            ColumnValue::Json(v) => flexbuffers::singleton(*v),
            ColumnValue::DateTime(v) => flexbuffers::singleton(*v),
            ColumnValue::Binary(v) => flexbuffers::singleton(*v),
        };
        let value = Some(self.fbb.create_vector(&value));

        let property =
            geo_buf::Property::create(&mut self.fbb, &geo_buf::PropertyArgs { name, value });
        self.properties.push(property);
        Ok(true)
    }
}

impl<'fbb> geozero::FeatureProcessor for GeometryBuilder<'fbb> {
    fn dataset_begin(&mut self, name: Option<&str>) -> GeoResult<()> {
        Ok(())
    }

    fn dataset_end(&mut self) -> GeoResult<()> {
        Ok(())
    }

    fn feature_begin(&mut self, idx: u64) -> GeoResult<()> {
        self.is_feature = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use geozero::GeozeroDatasource;
    use geozero::GeozeroGeometry;

    use crate::geo_generated::geo_buf;
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

    #[test]
    fn test_feature() {
        let geojson = r#"{
                "type": "Feature",
                "properties": {
                    "id": "NZL",
                    "name": "New Zealand"
                },
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [[[
                        [173.020375,-40.919052],[173.247234,-41.331999],[173.958405,-40.926701],[174.247587,-41.349155],[174.248517,-41.770008],[173.876447,-42.233184],[173.22274,-42.970038],[172.711246,-43.372288],[173.080113,-43.853344],[172.308584,-43.865694],[171.452925,-44.242519],[171.185138,-44.897104],[170.616697,-45.908929],[169.831422,-46.355775],[169.332331,-46.641235],[168.411354,-46.619945],[167.763745,-46.290197],[166.676886,-46.219917],[166.509144,-45.852705],[167.046424,-45.110941],[168.303763,-44.123973],[168.949409,-43.935819],[169.667815,-43.555326],[170.52492,-43.031688],[171.12509,-42.512754],[171.569714,-41.767424],[171.948709,-41.514417],[172.097227,-40.956104],[172.79858,-40.493962],[173.020375,-40.919052]
                    ]],[[
                        [174.612009,-36.156397],[175.336616,-37.209098],[175.357596,-36.526194],[175.808887,-36.798942],[175.95849,-37.555382],[176.763195,-37.881253],[177.438813,-37.961248],[178.010354,-37.579825],[178.517094,-37.695373],[178.274731,-38.582813],[177.97046,-39.166343],[177.206993,-39.145776],[176.939981,-39.449736],[177.032946,-39.879943],[176.885824,-40.065978],[176.508017,-40.604808],[176.01244,-41.289624],[175.239567,-41.688308],[175.067898,-41.425895],[174.650973,-41.281821],[175.22763,-40.459236],[174.900157,-39.908933],[173.824047,-39.508854],[173.852262,-39.146602],[174.574802,-38.797683],[174.743474,-38.027808],[174.697017,-37.381129],[174.292028,-36.711092],[174.319004,-36.534824],[173.840997,-36.121981],[173.054171,-35.237125],[172.636005,-34.529107],[173.007042,-34.450662],[173.551298,-35.006183],[174.32939,-35.265496],[174.612009,-36.156397]
                    ]]]
                }
        }"#;

        let mut builder = GeometryBuilder::new();
        geozero::geojson::GeoJson(&geojson)
            .process(&mut builder)
            .unwrap();
        let x = builder.build();
        let y = geo_buf::root_as_object(&x.buf[1..]).unwrap();
        let z = y.properties().unwrap();
        for x in z.iter() {
            println!("{:?}", x.name());
            let data = flexbuffers::Reader::get_root(x.value().unwrap().bytes()).unwrap();
            println!("{:?}", data.as_str());
        }
    }

    fn run_from_wkt(want: &str) {
        let mut b = GeometryBuilder::new();
        geozero::wkt::Wkt(want).process_geom(&mut b).unwrap();
        let geom = b.build();
        let crate::Wkt::<String>(got) = (&geom).try_into().unwrap();
        assert_eq!(want, got);
    }
}
