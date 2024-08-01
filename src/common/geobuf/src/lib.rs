#![feature(iter_map_windows)]

mod geo_generated;

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
}

impl<'fbb> GeometryBuilder<'fbb> {
    pub fn new() -> Self {
        Self {
            fbb: FlatBufferBuilder::new(),
            column_x: Vec::new(),
            column_y: Vec::new(),
            point_offsets: Vec::new(),
            ring_offsets: Vec::new(),
        }
    }

    pub fn point_len(&self) -> usize {
        self.column_x.len()
    }

    pub fn push_point(&mut self, point: Coord) {
        self.column_x.push(point.x);
        self.column_y.push(point.y);
    }

    pub fn push_line(&mut self, line: &[Coord], multi: bool) {
        if multi && self.point_offsets.is_empty() {
            self.point_offsets.push(self.point_len() as u32)
        }
        for p in line.iter() {
            self.push_point(*p);
        }
        if multi {
            self.point_offsets.push(self.point_len() as u32)
        }
    }

    pub fn push_polygon(&mut self, polygon: &Polygon, multi: bool) {
        if multi && self.ring_offsets.is_empty() {
            self.ring_offsets.push(self.point_offsets.len() as u32)
        }
        self.push_line(&polygon.exterior().0, true);
        for line in polygon.interiors().iter() {
            self.push_line(&line.0, true);
        }
        if multi {
            self.ring_offsets.push(self.point_offsets.len() as u32 - 1)
        }
    }

    fn push_inner_object(&mut self, geometry: &geo::Geometry) -> WIPOffset<InnerObject<'fbb>> {
        match geometry {
            geo::Geometry::Point(point) => {
                if self.point_offsets.is_empty() {
                    self.point_offsets.push(self.point_len() as u32)
                }
                self.push_point(point.0);
                self.point_offsets.push(self.point_len() as u32);

                let point_offsets = self.create_point_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::Point.0 as u32,
                    point_offsets,
                    ..Default::default()
                })
            }
            geo::Geometry::MultiPoint(points) => {
                if self.point_offsets.is_empty() {
                    self.point_offsets.push(self.point_len() as u32)
                }
                for p in points.iter() {
                    self.push_point(p.0);
                }
                self.point_offsets.push(self.point_len() as u32);
                let point_offsets = self.create_point_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::MultiPoint.0 as u32,
                    point_offsets,
                    ..Default::default()
                })
            }
            geo::Geometry::LineString(line) => {
                self.push_line(&line.0, true);
                let point_offsets = self.create_point_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::LineString.0 as u32,
                    point_offsets,
                    ..Default::default()
                })
            }
            geo::Geometry::MultiLineString(lines) => {
                if self.ring_offsets.is_empty() {
                    self.ring_offsets.push(self.point_offsets.len() as u32)
                }
                for line in lines.iter() {
                    self.push_line(&line.0, true);
                }
                self.ring_offsets.push(self.point_offsets.len() as u32 - 1);
                let point_offsets = self.create_point_offsets();
                let ring_offsets = self.create_ring_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::MultiLineString.0 as u32,
                    point_offsets,
                    ring_offsets,
                    ..Default::default()
                })
            }
            geo::Geometry::Polygon(polygon) => {
                self.push_polygon(polygon, true);
                let point_offsets = self.create_point_offsets();
                let ring_offsets = self.create_ring_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::Polygon.0 as u32,
                    point_offsets,
                    ring_offsets,
                    ..Default::default()
                })
            }
            geo::Geometry::MultiPolygon(polygons) => {
                for polygon in polygons.0.iter() {
                    self.push_polygon(polygon, true);
                }
                let point_offsets = self.create_point_offsets();
                let ring_offsets = self.create_ring_offsets();
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::MultiPolygon.0 as u32,
                    point_offsets,
                    ring_offsets,
                    ..Default::default()
                })
            }
            geo::Geometry::GeometryCollection(collection) => {
                let geometrys = collection
                    .0
                    .iter()
                    .map(|geom| self.push_inner_object(geom))
                    .collect::<Vec<_>>();

                let collection = Some(self.fbb.create_vector(&geometrys));
                geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                    wkb_type: geo_buf::InnerObjectKind::Collection.0 as u32,
                    collection,
                    ..Default::default()
                })
            }
            _ => unreachable!(),
        }
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

    pub fn build_light(self, kind: geo_buf::ObjectKind) -> Geometry {
        debug_assert!(
            [
                geo_buf::ObjectKind::Point,
                geo_buf::ObjectKind::MultiPoint,
                geo_buf::ObjectKind::LineString
            ]
            .contains(&kind)
        );
        let GeometryBuilder {
            column_x, column_y, ..
        } = self;
        Geometry {
            buf: vec![kind.0],
            column_x,
            column_y,
        }
    }

    pub fn build(
        self,
        kind: geo_buf::ObjectKind,
        object: WIPOffset<geo_buf::Object<'fbb>>,
    ) -> Geometry {
        let GeometryBuilder {
            mut fbb,
            column_x,
            column_y,
            ..
        } = self;
        geo_buf::finish_object_buffer(&mut fbb, object);

        let (mut buf, index) = fbb.collapse();
        let buf = if index > 0 {
            let index = index - 1;
            buf[index] = kind.0;
            buf[index..].to_vec()
        } else {
            [vec![kind.0], buf].concat()
        };

        Geometry {
            buf,
            column_x,
            column_y,
        }
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
    pub fn try_from_geo(geo: geo::Geometry<f64>) -> Result<Self, anyhow::Error> {
        let mut builder = GeometryBuilder::new();
        match geo {
            geo::Geometry::Point(point) => {
                builder.push_point(point.0);
                Ok(builder.build_light(geo_buf::ObjectKind::Point))
            }
            geo::Geometry::MultiPoint(points) => {
                for p in points.iter() {
                    builder.push_point(p.0);
                }
                Ok(builder.build_light(geo_buf::ObjectKind::MultiPoint))
            }
            geo::Geometry::LineString(line) => {
                builder.push_line(&line.0, false);
                Ok(builder.build_light(geo_buf::ObjectKind::LineString))
            }
            geo::Geometry::MultiLineString(lines) => {
                for line in lines.0 {
                    builder.push_line(&line.0, true)
                }
                let point_offsets = builder.create_point_offsets();
                let object = geo_buf::Object::create(&mut builder.fbb, &geo_buf::ObjectArgs {
                    point_offsets,
                    ..Default::default()
                });
                Ok(builder.build(geo_buf::ObjectKind::MultiLineString, object))
            }
            geo::Geometry::Polygon(polygon) => {
                builder.push_polygon(&polygon, false);
                let point_offsets = builder.create_point_offsets();
                let object = geo_buf::Object::create(&mut builder.fbb, &geo_buf::ObjectArgs {
                    point_offsets,
                    ..Default::default()
                });
                Ok(builder.build(geo_buf::ObjectKind::Polygon, object))
            }
            geo::Geometry::MultiPolygon(polygons) => {
                for polygon in polygons.iter() {
                    builder.push_polygon(polygon, true);
                }
                let point_offsets = builder.create_point_offsets();
                let ring_offsets = builder.create_ring_offsets();
                let object = geo_buf::Object::create(&mut builder.fbb, &geo_buf::ObjectArgs {
                    point_offsets,
                    ring_offsets,
                    ..Default::default()
                });
                Ok(builder.build(geo_buf::ObjectKind::MultiPolygon, object))
            }
            geo::Geometry::GeometryCollection(collection) => {
                let geometrys = collection
                    .0
                    .iter()
                    .map(|geom| builder.push_inner_object(geom))
                    .collect::<Vec<_>>();

                let collection = Some(builder.fbb.create_vector(&geometrys));
                let object = geo_buf::Object::create(&mut builder.fbb, &geo_buf::ObjectArgs {
                    collection,
                    ..Default::default()
                });
                Ok(builder.build(geo_buf::ObjectKind::Collection, object))
            }
            _ => unreachable!(),
        }
    }

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

impl<B: AsRef<[u8]>> TryFrom<Wkt<B>> for Geometry {
    type Error = anyhow::Error;

    fn try_from(wkt: Wkt<B>) -> Result<Self, Self::Error> {
        Geometry::try_from_geo(wkt.to_geo()?)
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

        // geo_buf:133, ewkb:123, points:96
        run_from_wkt(&"MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))");
        // geo_buf:237, ewkb:237, points:192
        run_from_wkt(
            &"MULTILINESTRING((-124.2 42,-120.01 41.99),(-124.2 42,-120.01 41.99,-122.5 42.01,-122.5 42.01),(-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))",
        );
        // geo_buf:113, ewkb:93, points:80
        run_from_wkt(&"POLYGON((17 17,17 30,30 30,30 17,17 17))");
        // geo_buf:197, ewkb:177, points:160
        run_from_wkt(
            &"POLYGON((100 0,101 0,101 1,100 1,100 0),(100.8 0.8,100.8 0.2,100.2 0.2,100.2 0.8,100.8 0.8))",
        );
        // geo_buf:185, ewkb:163, points:128
        run_from_wkt(&"MULTIPOLYGON(((-10 0,0 10,10 0,-10 0)),((-10 40,10 40,0 20,-10 40)))");
        // geo_buf:205, ewkb:108, points:80
        run_from_wkt(
            &"GEOMETRYCOLLECTION(POINT(99 11),LINESTRING(40 60,50 50,60 40),POINT(99 10))",
        );
        // geo_buf:285, ewkb:164, points:128
        run_from_wkt(
            &"GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),LINESTRING(40 60,50 50,60 40),POINT(99 11))",
        );
        // geo_buf:357, ewkb:194, points:144
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
