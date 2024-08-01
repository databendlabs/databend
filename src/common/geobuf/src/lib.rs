#![feature(iter_map_windows)]

mod geo_adapter;
mod geo_generated;
mod wkt_adapter;

use std::str::FromStr;

use anyhow;
use anyhow::Ok;
use flatbuffers::FlatBufferBuilder;
use flatbuffers::Vector;
use flatbuffers::WIPOffset;
use geo;
use geo::Coord;
use geo::GeometryCollection;
use geo::LineString;
use geo::MultiLineString;
use geo::MultiPoint;
use geo::MultiPolygon;
use geo::Point;
use geo::Polygon;
use geo_generated::geo_buf;
use geo_generated::geo_buf::InnerObject;
use geo_generated::geo_buf::Object;
use geozero::wkt::Wkt;
use geozero::ToGeo;
use geozero::ToWkt;
use ordered_float::OrderedFloat;

// A columnar Geometry/Geography format is proposed here to provide support for the storage and computation of geospatial features.
//
// This column format has five columns, namely the point column, which consists of the x column, the y column, and the point_offset column,
// and the rest of the information is serialized in a binary column, which consists of the data column and the offset column, see Column
//
// Format compatibility
//
// EWKB supports up to 4 dimensions, here only 2 dimensions are supported, in line with snowflake, and new types can be added to support higher dimensions.
// https://docs.snowflake.com/en/sql-reference/data-types-geospatial#geometry-data-type
//
// See geo.fbs for details on other compatibility designs.
//
// References WKT WKB EWKT EWKB GeoJSON Spec.
// https://libgeos.org/specifications/wkb/#standard-wkb
// https://datatracker.ietf.org/doc/html/rfc7946
//
// Why the columnar Geometry/Geography format?
//
// * For smaller storage volumes, see the test below.
// * The problem of compression coding of floating-point columns has a large number of readily available research results that can be introduced at low cost,
// which is conducive to compression of storage space and improvement of io efficiency.
// * Floating-point columns have min max sparse indexes, very similar to the R tree indexes widely used in geospatial features, and are very cheap to implement and maintain.
// * Facilitates the implementation of filtered push-down to the storage layer, and vectorized computation.
// * On the downside, geospatial functions become significantly more expensive to implement and maintain, requiring a significant amount of work.
//
// Why use flatbuffer?
//
// flatbuffer provides delayed deserialization , and partial deserialization capabilities , to facilitate the writing of high-performance implementation .
// In addition, code generation is easy to adapt to the design.
// However, flatbuffer has its drawbacks. Similar to protobuffer, flatbuffer is an extensible, compatible format, and in order to provide extensibility and compatibility, a vtable is deposited in the data, resulting in an inflated size.
// Once the design is stabilized, consider implementing it in other ways.
//

pub struct Column {
    buf: Vec<u8>,
    buf_offsets: Vec<u64>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
    point_offsets: Vec<u64>,
}

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

pub struct Geometry {
    buf: Vec<u8>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
}

pub struct BoundingBox {
    pub xmin: f64,
    pub xmax: f64,
    pub ymin: f64,
    pub ymax: f64,
}

impl Geometry {
    pub fn bounding_box(&self) -> BoundingBox {
        fn cmp(a: &f64, b: &f64) -> std::cmp::Ordering {
            std::cmp::Ord::cmp(&OrderedFloat(*a), &OrderedFloat(*b))
        }
        BoundingBox {
            xmin: self
                .column_x
                .iter()
                .copied()
                .min_by(cmp)
                .unwrap_or(f64::NAN),
            xmax: self
                .column_x
                .iter()
                .copied()
                .max_by(cmp)
                .unwrap_or(f64::NAN),
            ymin: self
                .column_y
                .iter()
                .copied()
                .min_by(cmp)
                .unwrap_or(f64::NAN),
            ymax: self
                .column_y
                .iter()
                .copied()
                .max_by(cmp)
                .unwrap_or(f64::NAN),
        }
    }
}

pub struct GeometryRef<'a> {
    buf: &'a [u8],
    column_x: &'a [f64],
    column_y: &'a [f64],
}

impl Geometry {
    pub fn try_to_geo(&self) -> Result<geo::Geometry, anyhow::Error> {
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
                            x: self.column_x[pos as usize],
                            y: self.column_y[pos as usize],
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
                        x: self.column_x[pos as usize],
                        y: self.column_y[pos as usize],
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

    pub fn memory_size(&self) -> usize {
        self.buf.len() + self.column_x.len() * 16
    }
}

impl TryFrom<Wkt<&str>> for Geometry {
    type Error = anyhow::Error;

    fn try_from(wkt: Wkt<&str>) -> Result<Self, Self::Error> {
        let wkt = wkt::Wkt::<f64>::from_str(wkt.0).map_err(|e| anyhow::Error::msg(e))?;
        let mut builder = GeometryBuilder::new();
        wkt.accept(&mut builder)?;
        Ok(builder.build())
        // Geometry::try_from(wkt.to_geo()?)
    }
}

impl TryFrom<geo::Geometry<f64>> for Geometry {
    type Error = anyhow::Error;

    fn try_from(geo: geo::Geometry<f64>) -> Result<Self, Self::Error> {
        let mut builder = GeometryBuilder::new();
        geo.accept(&mut builder)?;
        Ok(builder.build())
    }
}

impl TryInto<Wkt<String>> for Geometry {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Wkt<String>, Self::Error> {
        Ok(Wkt(self.try_to_geo()?.to_wkt()?))
    }
}

pub struct Geography {
    buf: Vec<u8>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
}

pub struct GeographyRef<'a> {
    buf: &'a [u8],
    column_x: &'a [f64],
    column_y: &'a [f64],
}

trait Visitor {
    fn visit_point(&mut self, point: &Coord, multi: bool) -> Result<(), anyhow::Error>;

    fn visit_points_start(&mut self, n: usize) -> Result<(), anyhow::Error>;

    fn visit_points_end(&mut self, multi: bool) -> Result<(), anyhow::Error>;

    fn visit_lines_start(&mut self, n: usize) -> Result<(), anyhow::Error>;

    fn visit_lines_end(&mut self) -> Result<(), anyhow::Error>;

    fn visit_polygon_start(&mut self, n: usize) -> Result<(), anyhow::Error>;

    fn visit_polygon_end(&mut self, multi: bool) -> Result<(), anyhow::Error>;

    fn visit_polygons_start(&mut self, n: usize) -> Result<(), anyhow::Error>;

    fn visit_polygons_end(&mut self) -> Result<(), anyhow::Error>;

    fn visit_collection_start(&mut self, n: usize) -> Result<(), anyhow::Error>;

    fn visit_collection_end(&mut self) -> Result<(), anyhow::Error>;

    fn finish(&mut self, kind: geo_buf::ObjectKind) -> Result<(), anyhow::Error>;
}

trait Element<V: Visitor> {
    fn accept(&self, visitor: &mut V) -> Result<(), anyhow::Error>;
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use geozero::CoordDimensions;
    use geozero::ToWkb;

    use super::*;

    #[test]
    fn test_from_wkt() {
        run_from_wkt(&"POINT(-122.35 37.55)"); // geo_buf:17, ewkb:21, points:16
        run_from_wkt(&"MULTIPOINT(-122.35 37.55,0 -90)"); // geo_buf:33, ewkb:51, points:32
        run_from_wkt(&"LINESTRING(-124.2 42,-120.01 41.99)"); // geo_buf:33, ewkb:41, points:32
        run_from_wkt(&"LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)"); // geo_buf:49, ewkb:57, points:48

        // geo_buf:141, ewkb:123, points:96
        run_from_wkt(&"MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))");
        // geo_buf:245, ewkb:237, points:192
        run_from_wkt(
            &"MULTILINESTRING((-124.2 42,-120.01 41.99),(-124.2 42,-120.01 41.99,-122.5 42.01,-122.5 42.01),(-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))",
        );
        // geo_buf:121, ewkb:93, points:80
        run_from_wkt(&"POLYGON((17 17,17 30,30 30,30 17,17 17))");
        // geo_buf:205, ewkb:177, points:160
        run_from_wkt(
            &"POLYGON((100 0,101 0,101 1,100 1,100 0),(100.8 0.8,100.8 0.2,100.2 0.2,100.2 0.8,100.8 0.8))",
        );
        // geo_buf:185, ewkb:163, points:128
        run_from_wkt(&"MULTIPOLYGON(((-10 0,0 10,10 0,-10 0)),((-10 40,10 40,0 20,-10 40)))");
        // geo_buf:205, ewkb:108, points:80
        run_from_wkt(
            &"GEOMETRYCOLLECTION(POINT(99 11),LINESTRING(40 60,50 50,60 40),POINT(99 10))",
        );
        // geo_buf:281, ewkb:164, points:128
        run_from_wkt(
            &"GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),LINESTRING(40 60,50 50,60 40),POINT(99 11))",
        );
        // geo_buf:353, ewkb:194, points:144
        run_from_wkt(
            &"GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),GEOMETRYCOLLECTION(LINESTRING(40 60,50 50,60 40),POINT(99 11)),POINT(50 70))",
        );
    }

    fn run_from_wkt(want: &str) {
        let geom = Geometry::try_from(Wkt(want)).unwrap();
        let ewkb = Wkt(want).to_ewkb(CoordDimensions::xy(), None).unwrap();
        println!(
            "geo_buf:{}, ewkb:{}, points:{}",
            geom.memory_size(),
            ewkb.len(),
            geom.column_x.len() * 16
        );

        let Wkt(got) = geom.try_into().unwrap();
        assert_eq!(want, got)
    }
}
