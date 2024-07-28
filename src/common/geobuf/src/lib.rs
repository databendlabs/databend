#![feature(iter_map_windows)]

mod geo_generated;

use anyhow;
use flatbuffers::FlatBufferBuilder;
use flatbuffers::WIPOffset;
use geo;
use geo::Coord;
use geo::LineString;
use geo::MultiLineString;
use geo::MultiPoint;
use geo::Point;
use geo::Polygon;
use geo_generated::geo_buf;
use geozero::wkt::Wkt;
use geozero::ToGeo;
use geozero::ToWkt;

pub struct GeographyBuilder<'a> {
    fbb: flatbuffers::FlatBufferBuilder<'a>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
}

impl<'a> GeographyBuilder<'a> {
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

    pub fn light_build(self, kind: geo_buf::ObjectKind) -> Geography {
        debug_assert!(
            [
                geo_buf::ObjectKind::Point,
                geo_buf::ObjectKind::MultiPoint,
                geo_buf::ObjectKind::LineString
            ]
            .contains(&kind)
        );
        let GeographyBuilder {
            column_x, column_y, ..
        } = self;
        Geography {
            buf: vec![kind.0],
            column_x,
            column_y,
        }
    }

    pub fn build(
        self,
        kind: geo_buf::ObjectKind,
        object: WIPOffset<geo_buf::Object<'a>>,
    ) -> Geography {
        let GeographyBuilder {
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

pub struct GeographyColumn {
    buf: Vec<u8>,
    buf_offsets: Vec<u64>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
    point_offsets: Vec<u64>,
}

pub struct Geometry {
    buf: Vec<u8>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
    column_z: Vec<f64>,
}

pub struct GeometryRef<'a> {
    buf: &'a [u8],
    column_x: &'a [f64],
    column_y: &'a [f64],
    column_z: &'a [f64],
}

pub struct GeometryColumn {
    buf: Vec<u8>,
    buf_offsets: Vec<u64>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
    column_z: Vec<f64>,
    point_offsets: Vec<u64>,
}

impl Geography {
    pub fn try_from_wkt(wkt: &str) -> Result<Self, anyhow::Error> {
        match Wkt(wkt).to_geo()? {
            geo::Geometry::Point(point) => {
                let mut builder = GeographyBuilder::new();
                let _ = builder.push_point(point.0);
                Ok(builder.light_build(geo_buf::ObjectKind::Point))
            }
            geo::Geometry::MultiPoint(points) => {
                let mut builder = GeographyBuilder::new();
                for p in points.iter() {
                    let _ = builder.push_point(p.0);
                }
                Ok(builder.light_build(geo_buf::ObjectKind::MultiPoint))
            }
            geo::Geometry::LineString(line) => {
                let mut builder = GeographyBuilder::new();
                builder.push_line(&line.0);
                Ok(builder.light_build(geo_buf::ObjectKind::LineString))
            }
            geo::Geometry::MultiLineString(lines) => {
                let mut builder = GeographyBuilder::new();
                let point_offsets = std::iter::once(builder.point_len() as u16)
                    .chain(lines.0.iter().map(|line| builder.push_line(&line.0)))
                    .collect::<Vec<_>>();
                let point_offsets = Some(builder.fbb.create_vector(&point_offsets));

                let object = geo_buf::Object::create(&mut builder.fbb, &geo_buf::ObjectArgs {
                    point_offsets,
                    ..Default::default()
                });
                Ok(builder.build(geo_buf::ObjectKind::MultiLineString, object))
            }
            geo::Geometry::Polygon(polygon) => {
                let mut builder = GeographyBuilder::new();
                let point_offsets = std::iter::once(builder.point_len() as u16)
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
                let point_offsets = Some(builder.fbb.create_vector(&point_offsets));
                let object = geo_buf::Object::create(&mut builder.fbb, &geo_buf::ObjectArgs {
                    point_offsets,
                    ..Default::default()
                });
                Ok(builder.build(geo_buf::ObjectKind::Polygon, object))
            }
            geo::Geometry::MultiPolygon(_) => todo!(),
            geo::Geometry::GeometryCollection(_) => todo!(),
            _ => unimplemented!(),
        }
    }

    pub fn try_to_wkt(&self) -> Result<String, anyhow::Error> {
        debug_assert!(self.column_x.len() == self.column_y.len());
        match geo_buf::ObjectKind(self.buf[0]) {
            geo_buf::ObjectKind::Point => {
                debug_assert!(self.column_x.len() == 1);
                Ok(geo::Geometry::Point(Point(Coord {
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
                Ok(geo::Geometry::MultiPoint(MultiPoint(points)).to_wkt()?)
            }
            geo_buf::ObjectKind::LineString => {
                let points = self
                    .column_x
                    .iter()
                    .cloned()
                    .zip(self.column_y.iter().cloned())
                    .map(|(x, y)| Coord { x, y })
                    .collect::<Vec<_>>();
                Ok(geo::Geometry::LineString(LineString(points)).to_wkt()?)
            }
            geo_buf::ObjectKind::MultiLineString => {
                let object = geo_buf::root_as_object(&self.buf[1..])?;
                let lines: Vec<_> = object
                    .point_offsets()
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
                Ok(geo::Geometry::MultiLineString(MultiLineString(lines)).to_wkt()?)
            }
            geo_buf::ObjectKind::Polygon => {
                let object = geo_buf::root_as_object(&self.buf[1..])?;

                let mut lines = object
                    .point_offsets()
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
                Ok(geo::Geometry::Polygon(Polygon::new(exterior, interiors)).to_wkt()?)
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

    #[test]
    fn test_from_wkt() {
        // https://libgeos.org/specifications/wkb/#standard-wkb
        // https://datatracker.ietf.org/doc/html/rfc7946

        // 做了什么？
        // 设计了一种列式Geometry/Geography格式，作为所有空间存储计算功能的基础设施。
        //
        // Geometry，总共有6个列，分别是，点列（x列, y列, point_offset列），其它信息行存，binary列（data+offset）
        // Geography，总共有5个列，点列只有两个维度，其他都一样

        // 可以把 Geometry 设计为 三维点，把 Geography 设计为 二维点

        // 为什么采用列式格式存储 Geometry

        // 列式Geometry的好处
        // 浮点列的压缩编码问题有大量现成的研究成果，可以低成本引入，有利于压缩存储空间，提高io效率。
        //
        // 浮点数列有min max 稀疏索引，能提供近似于R tree的搜索加速效果，且实现维护成本很低。
        //
        // 方便实现过滤下推到存储层。
        //
        // 为什么用flatbuffer？
        // flatbuffer提供延迟反序列化，和部分反序列化的能力。另外代码生成便于调整设计。
        // 但是flatbuffer也有缺点，flatbuffer 和 protobuffer 类似，是一种可扩展，兼容性格式，为了提供可扩展性兼容性在数据中存入了vtable，导致体积膨胀。
        // 后面可以想办法优化，但是优先级会比较低，因为将一个复杂而巨大的结构存在存入同一行是一个设计错误，不应该这样使用。

        // 缺点，函数的实现和维护成本明显变高了，需要投入大量工作量。

        run_from_wkt(&"POINT(-122.35 37.55)"); // geo_buf:17, ewkb:21, points:16
        run_from_wkt(&"MULTIPOINT(-122.35 37.55,0 -90)"); // geo_buf:33, ewkb:51, points:32
        run_from_wkt(&"LINESTRING(-124.2 42,-120.01 41.99)"); // geo_buf:33, ewkb:41, points:32
        run_from_wkt(&"LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)"); // geo_buf:49, ewkb:57, points:48

        // geo_buf:129, ewkb:123, points:96
        run_from_wkt(&"MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))");
        // geo_buf:229, ewkb:237, points:192
        run_from_wkt(
            &"MULTILINESTRING((-124.2 42,-120.01 41.99),(-124.2 42,-120.01 41.99,-122.5 42.01,-122.5 42.01),(-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))",
        );
        // geo_buf:109, ewkb:93, points:80
        run_from_wkt(&"POLYGON((17 17,17 30,30 30,30 17,17 17))");
        // geo_buf:193, ewkb:177, points:160
        run_from_wkt(
            &"POLYGON((100 0,101 0,101 1,100 1,100 0),(100.8 0.8,100.8 0.2,100.2 0.2,100.2 0.8,100.8 0.8))",
        );

        const TEST_WKT_MULTIPOLYGON: &str =
            &"MULTIPOLYGON(((-10 0,0 10,10 0,-10 0)),((-10 40,10 40,0 20,-10 40)))";
        const TEST_WKT_GEOMETRYCOLLECTION: &str = &"GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),LINESTRING(40 60, 50 50, 60 40), POINT(99 11))";
    }

    fn run_from_wkt(want: &str) {
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
