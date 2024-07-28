#![feature(iter_map_windows)]

mod geo_generated;

use anyhow;
use flatbuffers::FlatBufferBuilder;
use flatbuffers::WIPOffset;
use geo::Coord;
use geo::Geometry;
use geo::LineString;
use geo::MultiLineString;
use geo::MultiPoint;
use geo::Point;
use geo::Polygon;
use geo_generated::geo_buf;
use geozero::wkt::Wkt;
use geozero::ToGeo;
use geozero::ToWkt;

pub struct Builder<'a> {
    fbb: flatbuffers::FlatBufferBuilder<'a>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
}

impl<'a> Builder<'a> {
    pub fn new() -> Self {
        Self {
            fbb: FlatBufferBuilder::new(),
            column_x: Vec::new(),
            column_y: Vec::new(),
        }
    }

    pub fn point_len(&self) -> usize {
        self.column_x.len()
    }

    pub fn push_point(&mut self, point: Coord) -> u16 {
        let pos = self.point_len() as u16;
        self.column_x.push(point.x);
        self.column_y.push(point.y);
        pos
    }

    pub fn push_line(&mut self, line: &[Coord]) -> u16 {
        for p in line.iter() {
            self.push_point(*p);
        }
        self.point_len() as u16
    }

    pub fn create_polygon_object(
        &mut self,
        polygon: WIPOffset<geo_buf::Polygon<'a>>,
    ) -> WIPOffset<geo_buf::Object<'a>> {
        let polygons = [polygon];
        let polygons = Some(self.fbb.create_vector(&polygons));
        geo_buf::Object::create(&mut self.fbb, &geo_buf::ObjectArgs {
            polygons,
            ..Default::default()
        })
    }

    pub fn light_geography(self, kind: geo_buf::ObjectKind) -> Geography {
        debug_assert!(
            [
                geo_buf::ObjectKind::Point,
                geo_buf::ObjectKind::MultiPoint,
                geo_buf::ObjectKind::LineString
            ]
            .contains(&kind)
        );
        let Builder {
            column_x, column_y, ..
        } = self;
        Geography {
            buf: vec![kind.0],
            column_x,
            column_y,
        }
    }

    pub fn geography(
        self,
        kind: geo_buf::ObjectKind,
        object: WIPOffset<geo_buf::Object<'a>>,
    ) -> Geography {
        let Builder {
            mut fbb,
            column_x,
            column_y,
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

        Geography {
            buf,
            column_x,
            column_y,
        }
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

pub struct Column {
    buf: Vec<u8>,
    buf_offsets: Vec<u64>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
    point_offsets: Vec<u64>,
}

pub struct ColumnBuilder {}

impl Geography {
    pub fn try_from_wkt(wkt: &str) -> Result<Self, anyhow::Error> {
        match Wkt(wkt).to_geo()? {
            Geometry::Point(point) => {
                let mut builder = Builder::new();
                let _ = builder.push_point(point.0);
                Ok(builder.light_geography(geo_buf::ObjectKind::Point))
            }
            Geometry::MultiPoint(points) => {
                let mut builder = Builder::new();
                for p in points.iter() {
                    let _ = builder.push_point(p.0);
                }
                Ok(builder.light_geography(geo_buf::ObjectKind::MultiPoint))
            }
            Geometry::LineString(line) => {
                let mut builder = Builder::new();
                builder.push_line(&line.0);
                Ok(builder.light_geography(geo_buf::ObjectKind::LineString))
            }
            Geometry::MultiLineString(lines) => {
                let mut builder = Builder::new();
                let offsets = std::iter::once(builder.point_len() as u16)
                    .chain(lines.0.iter().map(|line| builder.push_line(&line.0)))
                    .collect::<Vec<_>>();
                let offsets = Some(builder.fbb.create_vector(&offsets));

                let object = geo_buf::Object::create(&mut builder.fbb, &geo_buf::ObjectArgs {
                    offsets,
                    ..Default::default()
                });
                Ok(builder.geography(geo_buf::ObjectKind::MultiLineString, object))
            }
            Geometry::Polygon(polygon) => {
                let mut builder = Builder::new();
                let offsets = std::iter::once(builder.point_len() as u16)
                    .chain(std::iter::once({
                        builder.push_line(&polygon.exterior().0)
                    }))
                    .chain(
                        polygon
                            .interiors()
                            .iter()
                            .map(|line| builder.push_line(&line.0)),
                    )
                    .collect::<Vec<_>>();
                let offsets = Some(builder.fbb.create_vector(&offsets));
                let object = geo_buf::Object::create(&mut builder.fbb, &geo_buf::ObjectArgs {
                    offsets,
                    ..Default::default()
                });
                Ok(builder.geography(geo_buf::ObjectKind::Polygon, object))
            }
            Geometry::MultiPolygon(_) => todo!(),
            Geometry::GeometryCollection(_) => todo!(),
            _ => unimplemented!(),
        }
    }

    pub fn try_to_wkt(&self) -> Result<String, anyhow::Error> {
        debug_assert!(self.column_x.len() == self.column_y.len());
        match geo_buf::ObjectKind(self.buf[0]) {
            geo_buf::ObjectKind::Point => {
                debug_assert!(self.column_x.len() == 1);
                Ok(Geometry::Point(Point(Coord {
                    x: self.column_x[0],
                    y: self.column_y[0],
                }))
                .to_wkt()?)
            }
            geo_buf::ObjectKind::MultiPoint => {
                let points = self
                    .column_x
                    .iter()
                    .cloned()
                    .zip(self.column_y.iter().cloned())
                    .map(|(x, y)| Point(Coord { x, y }))
                    .collect::<Vec<_>>();
                Ok(Geometry::MultiPoint(MultiPoint(points)).to_wkt()?)
            }
            geo_buf::ObjectKind::LineString => {
                let points = self
                    .column_x
                    .iter()
                    .cloned()
                    .zip(self.column_y.iter().cloned())
                    .map(|(x, y)| Coord { x, y })
                    .collect::<Vec<_>>();
                Ok(Geometry::LineString(LineString(points)).to_wkt()?)
            }
            geo_buf::ObjectKind::MultiLineString => {
                let object = geo_buf::root_as_object(&self.buf[1..])?;
                let lines: Vec<_> = object
                    .offsets()
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
                Ok(Geometry::MultiLineString(MultiLineString(lines)).to_wkt()?)
            }
            geo_buf::ObjectKind::Polygon => {
                let object = geo_buf::root_as_object(&self.buf[1..])?;

                let mut lines = object
                    .offsets()
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
                let exterior = lines.next().ok_or(anyhow::Error::msg("invalid data"))?;
                let interiors = lines.collect();
                Ok(Geometry::Polygon(Polygon::new(exterior, interiors)).to_wkt()?)
            }
            geo_buf::ObjectKind::MultiPolygon => todo!(),
            geo_buf::ObjectKind::Collection => todo!(),
            geo_buf::ObjectKind(geo_buf::ObjectKind::ENUM_MAX..) => unreachable!(),
        }
    }

    pub fn memory_size(&self) -> usize {
        self.buf.len() + self.column_x.len() * 16
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use geozero::CoordDimensions;
    use geozero::ToWkb;

    use super::*;

    const TEST_WKT_POINT: &str = &"POINT(-122.35 37.55)";
    const TEST_WKT_MULTIPOINT: &str = &"MULTIPOINT((-122.35 37.55),(0 -90))";
    const TEST_WKT_LINESTRING1: &str = &"LINESTRING(-124.2 42,-120.01 41.99)";
    const TEST_WKT_LINESTRING2: &str = &"LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)";
    const TEST_WKT_MULTILINESTRING: &str =
        &"MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))";
    const TEST_WKT_POLYGON: &str = &"POLYGON((17 17,17 30,30 30,30 17,17 17))";
    const TEST_WKT_MULTIPOLYGON: &str =
        &"MULTIPOLYGON(((-10 0,0 10,10 0,-10 0)),((-10 40,10 40,0 20,-10 40)))";
    const TEST_WKT_GEOMETRYCOLLECTION: &str = &"GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),LINESTRING(40 60, 50 50, 60 40), POINT(99 11))";

    #[test]
    fn test_geo() {
        let mut builder = Builder::new();
        let object = build_polygon(&mut builder);
        let geog = builder.geography(geo_buf::ObjectKind::Polygon, object);

        let object = geo_buf::root_as_object(&geog.buf[1..]).unwrap();
        println!("{object:?}");
    }

    fn build_polygon<'a>(builder: &mut Builder<'a>) -> WIPOffset<geo_buf::Object<'a>> {
        let points = [
            (102.0, 2.0),
            (103.0, 2.0),
            (103.0, 3.0),
            (102.0, 3.0),
            (102.0, 2.0),
        ];

        let items = points.map(|(x, y)| builder.push_point(Coord { x, y }));

        let points = Some(builder.fbb.create_vector(&items));
        let line =
            geo_buf::LineString::create(&mut builder.fbb, &geo_buf::LineStringArgs { points });

        let lines = [line];
        let rings = Some(builder.fbb.create_vector(&lines));
        let polygon = geo_buf::Polygon::create(&mut builder.fbb, &geo_buf::PolygonArgs { rings });

        builder.create_polygon_object(polygon)
    }

    #[test]
    fn test_point() {
        run_test(&"POINT(-122.35 37.55)"); // geo_buf:17, ewkb:21, points:16
        run_test(&"MULTIPOINT(-122.35 37.55,0 -90)"); // geo_buf:33, ewkb:51, points:32
        run_test(&"LINESTRING(-124.2 42,-120.01 41.99)"); // geo_buf:33, ewkb:41, points:32

        // geo_buf:181, ewkb:123, points:96
        run_test(&"MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))");
        // geo_buf:325, ewkb:237, points:192
        run_test(
            &"MULTILINESTRING((-124.2 42,-120.01 41.99),(-124.2 42,-120.01 41.99,-122.5 42.01,-122.5 42.01),(-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))",
        );
        // geo_buf:109, ewkb:93, points:80
        run_test(&"POLYGON((17 17,17 30,30 30,30 17,17 17))");
        //
        run_test(
            &"POLYGON((100 0,101 0,101 1,100 1,100 0),(100.8 0.8,100.8 0.2,100.2 0.2,100.2 0.8,100.8 0.8))",
        )
    }

    fn run_test(want: &str) {
        let geog = Geography::try_from_wkt(want).unwrap();
        let ewkb = Wkt(want).to_ewkb(CoordDimensions::xy(), None).unwrap();
        println!(
            "geo_buf:{}, ewkb:{}, points:{}",
            geog.memory_size(),
            ewkb.len(),
            geog.column_x.len() * 16
        );

        let got = geog.try_to_wkt().unwrap();
        assert_eq!(want, got)
    }
}

// let lines = object
// .line_strings()
// .ok_or(anyhow::Error::msg("invalid data"))?
// .iter()
// .map(|line| {
//     let line = line
//         .points()
//         .ok_or(anyhow::Error::msg("empty line_string"))?
//         .iter()
//         .map(|pos| Coord {
//             x: self.column_x[pos as usize],
//             y: self.column_y[pos as usize],
//         })
//         .collect();
//     Ok::<_, anyhow::Error>(LineString(line))
// })
// .collect::<Result<_, _>>()?;
