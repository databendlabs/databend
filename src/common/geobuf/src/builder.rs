use flatbuffers::FlatBufferBuilder;
use flatbuffers::Vector;
use flatbuffers::WIPOffset;

use super::geo_buf;
use super::geo_buf::InnerObject;
use super::geo_buf::Object;
use super::Coord;
use super::Geometry;
use super::Visitor;

#[derive(Default)]
pub struct GeometryBuilder<'fbb> {
    fbb: flatbuffers::FlatBufferBuilder<'fbb>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
    point_offsets: Vec<u32>,
    ring_offsets: Vec<u32>,

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
        }
    }

    pub fn point_len(&self) -> usize {
        self.column_x.len()
    }

    pub fn push_point(&mut self, point: &Coord) {
        self.column_x.push(point.x);
        self.column_y.push(point.y);
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
    fn visit_point(&mut self, point: &Coord, multi: bool) -> Result<(), anyhow::Error> {
        if self.stack.is_empty() {
            self.push_point(point);
            return Ok(());
        }

        if multi {
            self.push_point(point);
        } else {
            if self.point_offsets.is_empty() {
                self.point_offsets.push(self.point_len() as u32)
            }
            self.push_point(point);
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
